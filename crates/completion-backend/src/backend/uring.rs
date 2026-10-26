use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use super::Op;
use crate::desc::{BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
use crate::error::{Error, Result};
use io_uring::{EnterFlags, IoUring, cqueue, opcode, squeue, types};
use rustix::io::Errno;
use slotmap::{DefaultKey, Key, KeyData, SlotMap};
use socket2::SockAddr;

use super::{Config, Driver, Response, Waker, WakerInner};

const MIN_RING_ENTRIES: u32 = 2;

/// `-1` (`u64::MAX`), interpreted by the kernel as the descriptor's current
/// file position (`IORING_FEAT_RW_CUR_POS`, Linux 5.6+).
const CURRENT_POSITION: u64 = u64::MAX;

/// `user_data` for `IORING_OP_ASYNC_CANCEL` SQEs. SlotMap keys have odd
/// versions, so `0` never collides with an operation key.
const CANCEL_TOKEN: u64 = 0;

/// `user_data` for the internal futex-wait SQE. SlotMap indices stay below
/// `u32::MAX`, so `u64::MAX` never collides with an operation key.
const FUTEX_TOKEN: u64 = u64::MAX;

/// `FUTEX2_SIZE_U32` (`include/uapi/linux/futex.h`).
const FUTEX2_SIZE_U32: u32 = 0x02;

const EMPTY: u32 = 0;
const PARKED: u32 = 1;
const NOTIFIED: u32 = 2;

pub(super) struct Signal {
	state: AtomicU32,
	iopoll: bool,
}

impl Signal {
	pub(super) fn wake(&self) {
		if self.state.swap(NOTIFIED, Ordering::AcqRel) == PARKED && !self.iopoll {
			// `IORING_OP_FUTEX_WAIT` queues a shared futex wait (`Flags::empty()`),
			// so a `FUTEX_PRIVATE_FLAG` wake would not match it.
			let _ =
				rustix::thread::futex::wake(&self.state, rustix::thread::futex::Flags::empty(), 1);
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
	Count,
	Accepted,
	Done,
	Hangup,
}

pub struct Uring<T = ()> {
	ring: IoUring,
	/// Whether the ring reports completions only while polling; such a ring has
	/// no futex wake to ring.
	iopoll: bool,
	/// The word a blocking wait sleeps on and a cross-thread [`Waker`] flips.
	signal: Arc<Signal>,
	/// The operations the ring is running, filed under a key of this map: what
	/// the kernel still reads for each, held until its completion arrives. The
	/// key's FFI form is the request's `user_data`.
	active: SlotMap<DefaultKey, Active<T>>,
	/// Whether the kernel takes `IORING_ENTER_NO_IOWAIT` (Linux 6.15+). A kernel that does not still blocks in `io_uring_enter`, and that sleep counts as iowait.
	no_iowait: bool,
	/// Whether the futex wait is queued and has not completed.
	futex_armed: bool,
	sqpoll: bool,
}

// SAFETY: the ring owns its queue memory, and every buffer a queued request
// names is boxed or a `Vec` on the heap whose address does not change when the
// ring moves: handing the ring to another thread hands it the same addresses.
unsafe impl<T: Send> Send for Uring<T> {}

/// What the kernel still holds for one operation, and what its completion
/// reports, kept until the completion arrives.
struct Active<T> {
	/// The address a connect names. The kernel reads it when the request runs,
	/// so it lives in a box of its own until the completion arrives.
	_addr: Option<Box<SockAddr>>,
	/// What the completion reports.
	kind: Kind,
	/// The caller's data, handed back with the completion.
	data: T,
}

impl<T> Active<T> {
	/// An operation whose completion reports `kind`, with nothing extra for the
	/// kernel to hold beyond the request itself.
	fn new(kind: Kind, data: T) -> Active<T> {
		Active {
			_addr: None,
			kind,
			data,
		}
	}
}

/// What the mechanism reported, as the operation's own result.
fn completion(kind: Kind, result: i32) -> Result<Response> {
	if result < 0 {
		let errno = -result;
		return Err(if errno == Errno::CANCELED.raw_os_error() {
			Error::cancelled()
		} else {
			Error::from(io::Error::from_raw_os_error(errno))
		});
	}
	let moved = result as usize;
	Ok(match kind {
		Kind::Count => Response::Count(moved),
		Kind::Accepted => Response::Accepted(unsafe { OwnedSocket::from_raw_fd(result) }),
		Kind::Done | Kind::Hangup => Response::Done,
	})
}

/// Whether a poll's result mask names the peer gone or the socket failed.
fn hung(result: i32) -> bool {
	let gone = rustix::event::PollFlags::HUP | rustix::event::PollFlags::ERR;
	result >= 0 && (result as u32) & (gone.bits() as u32) != 0
}

/// The most bytes one request names. The kernel caps a single transfer at its
/// own `MAX_RW_COUNT` whatever the request asks for, and a request's length
/// field is a `u32`: a longer buffer moves what fits and reports a short count,
/// which the exact wrappers repeat.
fn capped(len: usize) -> u32 {
	u32::try_from(len).unwrap_or(u32::MAX)
}

impl<T> Uring<T> {
	pub(super) fn new(config: &Config) -> io::Result<Uring<T>> {
		let mut builder = IoUring::builder();
		if config.iopoll {
			builder.setup_iopoll();
		}
		if config.sqpoll {
			builder.setup_sqpoll(0);
		}
		let ring = builder.build(config.entries.max(MIN_RING_ENTRIES))?;
		let iopoll = ring.params().is_setup_iopoll();
		let sqpoll = ring.params().is_setup_sqpoll();
		let no_iowait = ring.params().is_feature_no_iowait();
		Ok(Uring {
			ring,
			iopoll,
			signal: Arc::new(Signal {
				state: AtomicU32::new(EMPTY),
				iopoll,
			}),
			active: SlotMap::new(),
			no_iowait,
			futex_armed: false,
			sqpoll,
		})
	}

	/// Queues a wait on the signal's state word. A wake that flips the word and
	/// calls `FUTEX_WAKE` completes it, so `enter` returns. The word is `PARKED`
	/// here: `poll` set it before the wait.
	fn arm_futex(&mut self) -> io::Result<()> {
		if self.futex_armed {
			return Ok(());
		}
		let word = self.signal.state.as_ptr().cast_const();
		let entry = opcode::FutexWait::new(
			word,
			u64::from(PARKED),
			u64::from(u32::MAX),
			FUTEX2_SIZE_U32,
		)
		.build()
		.user_data(FUTEX_TOKEN);
		self.push(entry)?;
		self.futex_armed = true;
		Ok(())
	}

	fn wait_flags(&self) -> EnterFlags {
		let mut flags = EnterFlags::GETEVENTS | EnterFlags::EXT_ARG;
		if self.no_iowait && !cfg!(feature = "iowait") {
			flags |= EnterFlags::NO_IOWAIT;
		}
		if self.sqpoll {
			flags |= EnterFlags::SQ_WAKEUP;
		}
		flags
	}

	/// The io-uring submit helpers cannot carry `NO_IOWAIT`, so call `io_uring_enter` directly.
	fn enter_wait(&mut self, timeout: Option<Duration>) -> io::Result<()> {
		let flags = self.wait_flags().bits();
		loop {
			// Dropping the submission-queue borrow publishes its tail.
			let queued = self.ring.submission().len() as u32;
			let result = match timeout {
				// SAFETY: `EXT_ARG` is set and the argument outlives the call.
				None => unsafe {
					let args = types::SubmitArgs::new();
					self.ring.submitter().enter(queued, 1, flags, Some(&args))
				},
				Some(timeout) => {
					let timespec = types::Timespec::from(timeout);
					let args = types::SubmitArgs::new().timespec(&timespec);
					// SAFETY: `EXT_ARG` is set, and `args` and the timespec it
					// points to outlive the call.
					unsafe { self.ring.submitter().enter(queued, 1, flags, Some(&args)) }
				}
			};
			match result {
				Ok(_) => return Ok(()),
				Err(error) if error.raw_os_error() == Some(Errno::TIME.raw_os_error()) => {
					return Ok(());
				}
				Err(error) if error.raw_os_error() == Some(Errno::INTR.raw_os_error()) => continue,
				Err(error) => return Err(error),
			}
		}
	}

	/// IOPOLL must actively poll the device; without a timeout, spin until completion.
	fn poll_devices(
		&mut self,
		timeout: Option<Duration>,
		mut complete: impl FnMut(u64, T, Result<Response>),
	) -> io::Result<()> {
		let start = Instant::now();
		loop {
			self.ring.submitter().submit_and_wait(0)?;
			if self.reap(&mut complete)? > 0 {
				return Ok(());
			}
			// An IOPOLL ring has no doorbell to ring, so a cross-thread wake is
			// only in the state word: see it within one spin instead of to the
			// timeout.
			if self.signal.state.load(Ordering::Acquire) == NOTIFIED {
				return Ok(());
			}
			if self.active.is_empty() {
				return Ok(());
			}
			match timeout {
				Some(Duration::ZERO) => return Ok(()),
				Some(timeout) if start.elapsed() >= timeout => return Ok(()),
				_ => {}
			}
		}
	}

	/// Takes every settled completion out of the ring and reports it to
	/// `complete`, returning how many landed here.
	fn reap(&mut self, mut complete: impl FnMut(u64, T, Result<Response>)) -> io::Result<usize> {
		let mut settled = Vec::new();
		let mut landed = 0;
		{
			let ring = &mut self.ring;
			let active = &mut self.active;
			let futex_armed = &mut self.futex_armed;
			for cqe in ring.completion() {
				let token = cqe.user_data();
				let more = cqueue::more(cqe.flags());
				if token == FUTEX_TOKEN {
					// Woken, refused because the word had moved on, or cancelled:
					// each one just ends the wait.
					*futex_armed = false;
					continue;
				}
				if token == CANCEL_TOKEN {
					continue;
				}
				// The token names the slot the operation is filed in. A slot taken
				// again since carries a new generation, so a stale completion — a
				// poll's second report, or one for an operation already settled —
				// names nothing here.
				let key = DefaultKey::from(KeyData::from_ffi(token));
				// Only a hangup poll is multishot, and it reports nothing until
				// the mask names the hangup.
				if more {
					if hung(cqe.result()) {
						// The first report settles the operation; removing it here
						// makes the multishot's later completions, and any cancel,
						// find nothing to say.
						let Some(Active { data, .. }) = active.remove(key) else {
							continue;
						};
						settled.push(token);
						complete(token, data, Ok(Response::Done));
						landed += 1;
					}
					continue;
				}
				let Some(Active { kind, data, .. }) = active.remove(key) else {
					// A poll can report twice — a multishot one whose hangup was
					// already taken as the result — and then has nothing left to
					// say.
					continue;
				};
				let reported = if kind == Kind::Hangup && cqe.result() >= 0 && !hung(cqe.result()) {
					// The kernel ended the multishot without a hangup (a CQ it
					// could not post to): report a temporary failure so the
					// caller can re-issue if it still wants to wait.
					Err(Error::from(io::Error::from(io::ErrorKind::WouldBlock)))
				} else {
					completion(kind, cqe.result())
				};
				complete(token, data, reported);
				landed += 1;
			}
		}
		for token in settled {
			let entry = opcode::AsyncCancel::new(token)
				.build()
				.user_data(CANCEL_TOKEN);
			if let Err(error) = self.push(entry) {
				log::warn!(token, error:%; "a reported hangup poll stays armed");
			}
		}
		Ok(landed)
	}

	/// Queues a request, submitting what is already there when the queue is full.
	/// A queue that is still full after that is the caller's error to report,
	/// rather than a spin here.
	fn push(&mut self, entry: squeue::Entry) -> io::Result<()> {
		if self.try_push(&entry) {
			return Ok(());
		}
		self.ring.submit()?;
		if self.try_push(&entry) {
			return Ok(());
		}
		Err(io::Error::new(
			io::ErrorKind::WouldBlock,
			"the submission queue is full",
		))
	}

	/// Queues a request if the submission queue has room for it; publishing the
	/// queue's tail is the caller's to decide.
	fn try_push(&mut self, entry: &squeue::Entry) -> bool {
		let mut submission = self.ring.submission();
		unsafe { submission.push(entry) }.is_ok()
	}

	/// Files `request` under `active`, queues it, and returns the id its
	/// completion reports under. `duplicate` is the descriptor a request that
	/// closes one closes in the caller's place.
	fn queue(
		&mut self,
		request: squeue::Entry,
		active: Active<T>,
		duplicate: Option<OwnedDescriptor>,
	) -> Result<u64> {
		// The request names the slot the operation is filed in: the ring hands
		// that token back, and its FFI form is the id the completion reports.
		let key = self.active.insert(active);
		let id = key.data().as_ffi();
		if let Err(error) = self.push(request.user_data(id)) {
			// Nothing reached the ring, so the operation never was. The guard in
			// `duplicate` closes what the request would have closed.
			self.active
				.remove(key)
				.expect("the operation was just filed");
			return Err(Error::from(error));
		}
		// The request names the duplicate a flush runs on, so the kernel's own
		// close owns it from the moment it is queued — whether or not the wake
		// below reaches a sleeping SQPOLL thread. From here it is not this call's
		// to close.
		if let Some(duplicate) = duplicate {
			std::mem::forget(duplicate);
		}
		if self.sqpoll {
			// SQPOLL consumes submissions itself but needs a syscall to wake its
			// sleeping thread. A wake that fails leaves the request queued all the
			// same, so the operation stays with the ring — what its entry holds for
			// the kernel may not be freed while the kernel can still read it.
			self.ring.submit()?;
		}
		Ok(id)
	}

	/// Refuses an operation an IOPOLL ring has no form for, before anything is
	/// built for it: such a ring takes only operations the kernel can poll for,
	/// and `what` names the one it cannot.
	fn refuse_unpollable(&self, what: &str) -> Result<()> {
		if !self.iopoll {
			return Ok(());
		}
		Err(Error::from(io::Error::new(
			io::ErrorKind::Unsupported,
			format!("{what} cannot be submitted on an IOPOLL ring; give it a ring of its own"),
		)))
	}
}

impl<T> Uring<T> {
	/// Files the request `op` names and queues it, returning the id its
	/// completion reports under.
	///
	/// An IOPOLL ring takes only operations the kernel can poll for, so the
	/// operations it has no form for are refused here, before anything is built
	/// for them.
	fn prepare(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64> {
		let raw = types::Fd(fd.as_raw_fd());
		match op {
			Op::Read { offset, buf } => {
				let offset = offset.unwrap_or(CURRENT_POSITION);
				let request = opcode::Read::new(raw, buf.ptr, capped(buf.len))
					.offset(offset)
					.build();
				self.queue(request, Active::new(Kind::Count, data), None)
			}
			Op::Write { offset, buf } => {
				let offset = offset.unwrap_or(CURRENT_POSITION);
				let request = opcode::Write::new(raw, buf.ptr.cast_const(), capped(buf.len))
					.offset(offset)
					.build();
				self.queue(request, Active::new(Kind::Count, data), None)
			}
			Op::Fsync => {
				self.refuse_unpollable("fsync")?;
				let request = opcode::Fsync::new(raw).build();
				self.queue(request, Active::new(Kind::Done, data), None)
			}
			Op::Flush => {
				self.refuse_unpollable("flush")?;
				// A flush is a close, and a close releases the descriptor the
				// caller still holds: it runs on a duplicate, so what the kernel
				// closes is this backend's. The reactor's entry goes on naming the
				// caller's descriptor, which it keeps open until it goes.
				let duplicate = fd.try_clone_to_owned()?;
				let request = opcode::Close::new(types::Fd(duplicate.as_raw_fd())).build();
				self.queue(request, Active::new(Kind::Done, data), Some(duplicate))
			}
			Op::Allocate { offset, len } => {
				self.refuse_unpollable("fallocate")?;
				let request = opcode::Fallocate::new(raw, len)
					.offset(offset)
					.mode(0)
					.build();
				self.queue(request, Active::new(Kind::Done, data), None)
			}
			Op::Punch { offset, len } => {
				self.refuse_unpollable("fallocate")?;
				let request = opcode::Fallocate::new(raw, len)
					.offset(offset)
					.mode(libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE)
					.build();
				self.queue(request, Active::new(Kind::Done, data), None)
			}
			Op::Accept => {
				self.refuse_unpollable("accept")?;
				let flags = (rustix::net::SocketFlags::CLOEXEC | rustix::net::SocketFlags::NONBLOCK)
					.bits() as i32;
				let request = opcode::Accept::new(raw, std::ptr::null_mut(), std::ptr::null_mut())
					.flags(flags)
					.build();
				self.queue(request, Active::new(Kind::Accepted, data), None)
			}
			Op::Recv { buf } => {
				let request = opcode::Recv::new(raw, buf.ptr, capped(buf.len)).build();
				self.queue(request, Active::new(Kind::Count, data), None)
			}
			Op::Send { buf } => {
				let request = opcode::Send::new(raw, buf.ptr, capped(buf.len)).build();
				self.queue(request, Active::new(Kind::Count, data), None)
			}
			Op::Connect { addr } => {
				self.refuse_unpollable("connect")?;
				// The kernel reads the address the request points at when it runs,
				// so the address moves into a box whose address stays put until the
				// completion.
				let addr = Box::new(addr);
				let request = opcode::Connect::new(raw, addr.as_ptr().cast(), addr.len()).build();
				self.queue(
					request,
					Active {
						_addr: Some(addr),
						..Active::new(Kind::Done, data)
					},
					None,
				)
			}
			Op::Readable => {
				let request =
					opcode::PollAdd::new(raw, rustix::event::PollFlags::IN.bits() as u32).build();
				self.queue(request, Active::new(Kind::Done, data), None)
			}
			Op::Writable => {
				let request =
					opcode::PollAdd::new(raw, rustix::event::PollFlags::OUT.bits() as u32).build();
				self.queue(request, Active::new(Kind::Done, data), None)
			}
			Op::Hangup => {
				// The kernel ORs IO_POLL_UNMASK (EPOLLERR | EPOLLHUP | EPOLLNVAL |
				// EPOLLRDHUP, io_uring/poll.c) into every poll's events, so even an
				// empty mask fires on a half-close. Multishot keeps the poll armed
				// past those; `reap` waits for a mask with HUP or ERR.
				let request = opcode::PollAdd::new(raw, 0).multi(true).build();
				self.queue(request, Active::new(Kind::Hangup, data), None)
			}
		}
	}
}

impl<T> Driver<T> for Uring<T> {
	fn submit(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64> {
		self.prepare(fd, op, data)
	}

	fn get_mut(&mut self, id: u64) -> Option<&mut T> {
		self.active
			.get_mut(DefaultKey::from(KeyData::from_ffi(id)))
			.map(|active| &mut active.data)
	}

	fn cancel(&mut self, id: u64) {
		// The id is the token the ring holds: a key it never handed out, or one
		// whose slot has been taken again since, names no operation here.
		if !self
			.active
			.contains_key(DefaultKey::from(KeyData::from_ffi(id)))
		{
			return;
		}
		let entry = opcode::AsyncCancel::new(id).build().user_data(CANCEL_TOKEN);
		drop(self.push(entry).and_then(|()| self.ring.submit()));
	}

	fn cancel_all(&mut self) {
		let ids: Vec<u64> = self.active.keys().map(|key| key.data().as_ffi()).collect();
		for id in ids {
			self.cancel(id);
		}
	}

	fn is_idle(&self) -> bool {
		self.active.is_empty()
	}

	fn poll(
		&mut self,
		timeout: Option<Duration>,
		mut complete: impl FnMut(u64, T, Result<Response>),
	) -> Result<()> {
		if timeout == Some(Duration::ZERO) {
			// A poll that does not wait leaves the state word alone: a wake that
			// arrived while the executor ticked must stay for the next blocking
			// poll instead of being slept through.
			if self.iopoll {
				self.poll_devices(Some(Duration::ZERO), complete)?;
			} else {
				self.ring.submit()?;
				self.reap(complete)?;
			}
			return Ok(());
		}
		// A completion already in the ring means the work is done: reap it
		// instead of queueing a wait the kernel would have to answer first.
		// `submit` enters the kernel only when the submission queue holds
		// something, and a blocking `enter_wait` below is a syscall either way.
		if !self.ring.completion().is_empty() {
			self.ring.submit()?;
			if self.reap(&mut complete)? > 0 {
				return Ok(());
			}
		}
		// A wake that arrived before the wait is taken and answered with a
		// non-blocking reap instead of sleeping; one that arrives after the
		// transition rings the futex (or is seen by the IOPOLL spin).
		if self
			.signal
			.state
			.compare_exchange(EMPTY, PARKED, Ordering::AcqRel, Ordering::Acquire)
			.is_err()
		{
			self.signal.state.store(EMPTY, Ordering::Release);
			if self.iopoll {
				self.poll_devices(Some(Duration::ZERO), complete)?;
			} else {
				self.ring.submit()?;
				self.reap(complete)?;
			}
			return Ok(());
		}
		let result = if self.iopoll {
			self.poll_devices(timeout, complete)
		} else {
			self.arm_futex()
				.and_then(|()| self.enter_wait(timeout))
				.and_then(|()| self.reap(complete).map(|_| ()))
		};
		self.signal.state.store(EMPTY, Ordering::Release);
		result?;
		Ok(())
	}

	fn waker(&self) -> Waker {
		Waker(WakerInner::Uring(Arc::clone(&self.signal)))
	}
}

impl<T> Drop for Uring<T> {
	fn drop(&mut self) {
		// A queued futex wait points at the signal's state word, which the
		// reactor frees with its last `Arc`. Cancel it and reap its completion
		// first, so the ring lets go before the word can go.
		if !self.futex_armed {
			return;
		}
		let entry = opcode::AsyncCancel::new(FUTEX_TOKEN)
			.build()
			.user_data(CANCEL_TOKEN);
		if self.push(entry).is_err() {
			return;
		}
		while self.futex_armed {
			if self.ring.submit_and_wait(1).is_err() || self.reap(|_, _, _| {}).is_err() {
				return;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::io;
	use std::sync::atomic::{AtomicU32, Ordering};

	use assert2::assert;
	use rstest::rstest;

	use crate::error::ErrorKind;

	use super::{EMPTY, Kind, NOTIFIED, PARKED, Response, Signal, capped, completion, hung};

	#[test]
	fn completion_maps_cqe_results_to_responses_and_errors() {
		assert!(let Ok(Response::Count(64)) = completion(Kind::Count, 64));
		assert!(let Ok(Response::Done) = completion(Kind::Done, 0));

		assert!(let Err(cancelled) = completion(Kind::Count, -libc::ECANCELED));
		assert!(cancelled.kind() == ErrorKind::Cancelled);

		assert!(let Err(failed) = completion(Kind::Count, -libc::EIO));
		assert!(failed.kind() == ErrorKind::Failed);
		assert!(let Ok(io_error) = io::Error::try_from(failed));
		assert!(io_error.raw_os_error() == Some(libc::EIO));
	}

	#[rstest]
	#[case::cancelled(-libc::ECANCELED, false)]
	#[case::none(0, false)]
	#[case::readable(rustix::event::PollFlags::IN.bits() as i32, false)]
	#[case::hangup(rustix::event::PollFlags::HUP.bits() as i32, true)]
	#[case::error(rustix::event::PollFlags::ERR.bits() as i32, true)]
	fn hung_detects_poll_hup_and_err_bits_only_on_non_negative_results(
		#[case] result: i32,
		#[case] expected: bool,
	) {
		assert!(hung(result) == expected);
	}

	#[rstest]
	#[case(0, 0)]
	#[case(4096, 4096)]
	#[case(usize::MAX, u32::MAX)]
	fn capped_saturates_lengths_above_u32_max(#[case] len: usize, #[case] expected: u32) {
		assert!(capped(len) == expected);
	}

	#[rstest]
	fn signal_wake_transitions_state_to_notified(#[values(false, true)] iopoll: bool) {
		let signal = Signal {
			state: AtomicU32::new(EMPTY),
			iopoll,
		};
		signal.wake();
		assert!(signal.state.load(Ordering::Acquire) == NOTIFIED);

		signal.state.store(PARKED, Ordering::Release);
		signal.wake();
		assert!(signal.state.load(Ordering::Acquire) == NOTIFIED);
	}
}
