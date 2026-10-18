//! Linux `io_uring` backend.
//!
//! The reactor files each operation under an id, and this backend files it
//! again under a key of its own `active` slot map, carrying that key as the
//! request's `user_data`: the completion queue gives it straight back, and a
//! slot taken again since carries a new generation, so a late completion names
//! nothing. What the kernel still reads through — the iovec array a `readv` or
//! `writev` points at, a control message — is what the `active` entry holds,
//! until the completion arrives.

use std::io;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use super::Op;
use crate::desc::{BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
use crate::error::{Error, Result};
use crate::memory::{Resolved, Span};
use io_uring::{EnterFlags, IoUring, cqueue, opcode, squeue, types};
use rustix::io::Errno;
use slotmap::{DefaultKey, Key, KeyData, SlotMap};

use super::{Config, Driver, Response};

const MIN_RING_ENTRIES: u32 = 2;

/// Maximum descriptors per control message.
const MAX_FDS: usize = 8;

/// The token a cancel is reported under: no completion waits for it, so it is
/// the one id no operation can have.
const CANCEL_TOKEN: u64 = 0;

/// The token the futex wait on the signal's state word is reported under. An
/// operation's token packs a slot key's version above its index: the version is
/// odd, so the token is never `0`, and the index stays below `u32::MAX`, so it
/// is never this one either.
const FUTEX_TOKEN: u64 = u64::MAX;

/// `FUTEX2_SIZE_U32`: the state word is a 32-bit futex (`include/uapi/linux/futex.h`).
const FUTEX2_SIZE_U32: u32 = 0x02;

/// No wait is outstanding.
const EMPTY: u32 = 0;
/// A wait is between its compare-exchange and its return.
const PARKED: u32 = 1;
/// A wake arrived with no wait to take it.
const NOTIFIED: u32 = 2;

/// The futex word a blocking wait sleeps on and a cross-thread wake flips.
struct Signal {
	state: AtomicU32,
	/// An IOPOLL ring spins instead of sleeping and reads the word itself, so
	/// waking it has no futex to ring.
	iopoll: bool,
}

impl Wake for Signal {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		if self.state.swap(NOTIFIED, Ordering::AcqRel) == PARKED && !self.iopoll {
			// Shared, not private: the ring queues its `FUTEX_WAIT` on this same
			// address, and a private wake would not match it.
			let _ =
				rustix::thread::futex::wake(&self.state, rustix::thread::futex::Flags::empty(), 1);
		}
	}
}

/// What a completion reports.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
	Count,
	CountWithFds,
	Accepted,
	Done,
	/// A multishot poll waiting for a hangup: a completion it reports without
	/// one is a temporary failure rather than the result.
	Hangup,
}

impl Kind {
	fn of(op: &Op) -> Kind {
		match op {
			Op::RecvWithFds { .. } => Kind::CountWithFds,
			Op::Accept => Kind::Accepted,
			Op::Hangup => Kind::Hangup,
			Op::Fsync
			| Op::Flush
			| Op::Allocate { .. }
			| Op::Punch { .. }
			| Op::Readable
			| Op::Writable => Kind::Done,
			_ => Kind::Count,
		}
	}
}

/// The kernel dereferences the header and its buffers at execution, so all must remain stable until completion.
struct Control {
	msg: Box<libc::msghdr>,
	_iov: Box<libc::iovec>,
	_cmsg: Box<[u8]>,
	/// Descriptors must stay open until the kernel executes the send.
	_fds: Vec<OwnedDescriptor>,
}

// SAFETY: every pointer in `msg` names one of this value's own heap buffers
// (`iov`, `cmsg`), whose addresses do not change when the value moves — which
// is what the ring needs, and what makes sending it to another thread sound.
unsafe impl Send for Control {}

impl Control {
	fn receive(buf: *mut u8, len: usize) -> Control {
		let mut iov = Box::new(libc::iovec {
			iov_base: buf.cast(),
			iov_len: len,
		});
		let space =
			(unsafe { libc::CMSG_SPACE((MAX_FDS * size_of::<libc::c_int>()) as u32) }) as usize;
		let mut cmsg = vec![0u8; space].into_boxed_slice();
		let mut msg: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
		msg.msg_iov = iov.as_mut() as *mut libc::iovec;
		msg.msg_iovlen = 1;
		msg.msg_control = cmsg.as_mut_ptr().cast();
		msg.msg_controllen = cmsg.len() as _;
		Control {
			msg,
			_iov: iov,
			_cmsg: cmsg,
			_fds: Vec::new(),
		}
	}

	fn send(buf: *const u8, len: usize, fds: Vec<OwnedDescriptor>) -> io::Result<Control> {
		if fds.len() > MAX_FDS {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"too many descriptors",
			));
		}
		let mut iov = Box::new(libc::iovec {
			iov_base: buf.cast_mut().cast(),
			iov_len: len,
		});
		let bytes = fds.len() * size_of::<libc::c_int>();
		let space = (unsafe { libc::CMSG_SPACE(bytes as u32) }) as usize;
		let mut cmsg = vec![0u8; space].into_boxed_slice();
		let mut msg: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
		msg.msg_iov = iov.as_mut() as *mut libc::iovec;
		msg.msg_iovlen = 1;
		msg.msg_control = cmsg.as_mut_ptr().cast();
		msg.msg_controllen = (unsafe { libc::CMSG_LEN(bytes as u32) }) as _;
		unsafe {
			let header = cmsg.as_mut_ptr().cast::<libc::cmsghdr>();
			(*header).cmsg_level = libc::SOL_SOCKET;
			(*header).cmsg_type = libc::SCM_RIGHTS;
			(*header).cmsg_len = msg.msg_controllen as _;
			let data = libc::CMSG_DATA(header).cast::<libc::c_int>();
			for (index, fd) in fds.iter().enumerate() {
				*data.add(index) = fd.as_raw_fd();
			}
		}
		Ok(Control {
			msg,
			_iov: iov,
			_cmsg: cmsg,
			_fds: fds,
		})
	}

	fn message(&mut self) -> *mut libc::msghdr {
		&mut *self.msg
	}

	fn take_fds(&mut self) -> Vec<OwnedDescriptor> {
		let mut out = Vec::new();
		let msg: *mut libc::msghdr = &mut *self.msg;
		let mut header = unsafe { libc::CMSG_FIRSTHDR(msg) };
		while !header.is_null() {
			let (level, kind) = unsafe { ((*header).cmsg_level, (*header).cmsg_type) };
			if level == libc::SOL_SOCKET && kind == libc::SCM_RIGHTS {
				let length = (unsafe { (*header).cmsg_len } as usize)
					- (unsafe { libc::CMSG_LEN(0) }) as usize;
				let count = length / size_of::<libc::c_int>();
				let data = unsafe { libc::CMSG_DATA(header) }.cast::<libc::c_int>();
				for index in 0..count {
					let fd = unsafe { *data.add(index) };
					// SAFETY: the kernel put this descriptor in the process,
					// and nothing else in it owns the number.
					out.push(unsafe { OwnedDescriptor::from_raw_fd(fd) });
				}
			}
			header = unsafe { libc::CMSG_NXTHDR(msg, header) };
		}
		out
	}
}

pub(crate) struct Uring {
	ring: IoUring,
	/// Whether the ring reports completions only while polling; such a ring has
	/// no futex wake to ring.
	iopoll: bool,
	/// The word a blocking wait sleeps on and a cross-thread [`Waker`] flips.
	signal: Arc<Signal>,
	/// The operations the ring is running, filed under a key of this map: what
	/// the kernel still reads for each, held until its completion arrives. The
	/// key's FFI form is the request's `user_data`.
	active: SlotMap<DefaultKey, Active>,
	/// The iovec arrays a vectored request points at, kept for the next one once
	/// nothing reads them.
	iovecs: Vec<Vec<libc::iovec>>,
	/// Whether the kernel takes `IORING_ENTER_NO_IOWAIT` (Linux 6.15+). A kernel that does not still blocks in `io_uring_enter`, and that sleep counts as iowait.
	no_iowait: bool,
	/// Whether the futex wait is queued and has not completed.
	futex_armed: bool,
	sqpoll: bool,
}

/// What the kernel still holds for one operation, and what its completion
/// reports, kept until the completion arrives.
struct Active {
	/// The id the reactor gave the operation, which the completion reports.
	id: u64,
	/// The iovec array a `readv` or `writev` request points at: the kernel reads
	/// the addresses from it at execution, so it lives until the completion. A
	/// `Vec`'s elements live on the heap, so the array a queued request points
	/// at keeps its address when the `Vec` moves in here; only a reallocation
	/// would move it, and the array is filled before its request is queued and
	/// left alone until the completion takes it back.
	buffers: Vec<libc::iovec>,
	/// The message area an operation carrying descriptors reads through, and
	/// where a receive's descriptors land.
	control: Option<Control>,
	/// What the completion reports.
	kind: Kind,
}

/// An iovec array to fill for a vectored request: one nothing is reading from,
/// or a new one.
fn lease(iovecs: &mut Vec<Vec<libc::iovec>>) -> Vec<libc::iovec> {
	iovecs.pop().unwrap_or_default()
}

/// Takes an array back once nothing reads it, keeping the allocation for the
/// next vectored request.
fn release(iovecs: &mut Vec<Vec<libc::iovec>>, mut buffers: Vec<libc::iovec>) {
	buffers.clear();
	iovecs.push(buffers);
}

/// What the mechanism reported, as the operation's own result.
fn completion(kind: Kind, result: i32, control: Option<Control>) -> Result<Response> {
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
		Kind::CountWithFds => Response::CountWithFds(
			moved,
			control.map_or_else(Vec::new, |mut area| area.take_fds()),
		),
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

/// The one buffer a transfer names, with its length, when the transfer can go
/// through a plain `read` or `write`: the kernel takes that buffer's address
/// and length from the SQE itself, so no iovec array has to outlive the
/// request. A span of any other shape needs the vectored form.
fn single(span: &Span) -> Result<Option<(Resolved, u32)>> {
	let Some(buf) = span.resolve_single() else {
		return Ok(None);
	};
	let buf = buf?;
	Ok(Some((buf, capped(buf.len))))
}

/// The one buffer a control message names: a `recvmsg` or `sendmsg` carries a
/// single iovec.
fn only(span: &Span, call: &str) -> Result<Resolved> {
	if span.extents().len() != 1 {
		return Err(Error::from(io::Error::new(
			io::ErrorKind::Unsupported,
			format!("a socket {call} needs exactly one buffer"),
		)));
	}
	span.resolve_single()
		.expect("a span of one extent needs no array")
}

impl Uring {
	pub(super) fn new(config: &Config) -> io::Result<Uring> {
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
			iovecs: Vec::new(),
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
		mut complete: impl FnMut(u64, Result<Response>),
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
	fn reap(&mut self, mut complete: impl FnMut(u64, Result<Response>)) -> io::Result<usize> {
		let mut settled = Vec::new();
		let mut landed = 0;
		{
			let ring = &mut self.ring;
			let active = &mut self.active;
			let futex_armed = &mut self.futex_armed;
			let iovecs = &mut self.iovecs;
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
						let Some(in_flight) = active.remove(key) else {
							continue;
						};
						release(iovecs, in_flight.buffers);
						settled.push(token);
						complete(in_flight.id, Ok(Response::Done));
						landed += 1;
					}
					continue;
				}
				let Some(in_flight) = active.remove(key) else {
					// A poll can report twice — a multishot one whose hangup was
					// already taken as the result — and then has nothing left to
					// say.
					continue;
				};
				release(iovecs, in_flight.buffers);
				let reported =
					if in_flight.kind == Kind::Hangup && cqe.result() >= 0 && !hung(cqe.result()) {
						// The kernel ended the multishot without a hangup (a CQ it
						// could not post to): report a temporary failure so the
						// caller can re-issue if it still wants to wait.
						Err(Error::from(io::Error::from(io::ErrorKind::WouldBlock)))
					} else {
						completion(in_flight.kind, cqe.result(), in_flight.control)
					};
				complete(in_flight.id, reported);
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

	/// The ring's own request for an operation, and what has to outlive it: the
	/// iovec array and the control area the kernel reads through, and a
	/// descriptor the kernel closes in the caller's place.
	///
	/// An IOPOLL ring takes only operations the kernel can poll for, so the
	/// operations it has no form for are refused here, before anything is built
	/// for them.
	fn prepare(
		&mut self,
		id: u64,
		fd: BorrowedDescriptor<'_>,
		op: Op,
	) -> Result<(squeue::Entry, Active, Option<OwnedDescriptor>)> {
		if self.iopoll {
			let what = match &op {
				Op::Fsync => Some("fsync"),
				Op::Flush => Some("flush"),
				Op::Allocate { .. } | Op::Punch { .. } => Some("fallocate"),
				Op::Accept => Some("accept"),
				_ => None,
			};
			if let Some(what) = what {
				return Err(Error::from(io::Error::new(
					io::ErrorKind::Unsupported,
					format!(
						"{what} cannot be submitted on an IOPOLL ring; give it a ring of its own"
					),
				)));
			}
		}
		// The descriptors a send carries move into the control area the kernel
		// reads through, and a vectored transfer's array moves into the
		// operation's `active` entry: neither is left for a second owner here.
		let kind = Kind::of(&op);
		let raw_fd = types::Fd(fd.as_raw_fd());
		let mut control = None;
		let mut duplicate: Option<OwnedDescriptor> = None;
		let (request, buffers) = match op {
			Op::Read { offset, span } => match single(&span)? {
				Some((buf, count)) => (
					opcode::Read::new(raw_fd, buf.ptr, count)
						.offset(offset)
						.build(),
					Vec::new(),
				),
				None => self.vectored(&span, |ptr, count| {
					opcode::Readv::new(raw_fd, ptr, count)
						.offset(offset)
						.build()
				})?,
			},
			Op::Write { offset, span } => match single(&span)? {
				Some((buf, count)) => (
					opcode::Write::new(raw_fd, buf.ptr, count)
						.offset(offset)
						.build(),
					Vec::new(),
				),
				None => self.vectored(&span, |ptr, count| {
					opcode::Writev::new(raw_fd, ptr, count)
						.offset(offset)
						.build()
				})?,
			},
			Op::Fsync => (opcode::Fsync::new(raw_fd).build(), Vec::new()),
			Op::Flush => {
				// A flush is a close, and a close releases the descriptor the caller
				// still holds: it runs on a duplicate, so what the kernel closes is
				// this backend's. The reactor's entry goes on naming the caller's
				// descriptor, which it keeps open until it goes.
				let dup = fd.try_clone_to_owned()?;
				let request = opcode::Close::new(types::Fd(dup.as_raw_fd())).build();
				duplicate = Some(dup);
				(request, Vec::new())
			}
			Op::Allocate { offset, len } => (
				opcode::Fallocate::new(raw_fd, len)
					.offset(offset)
					.mode(0)
					.build(),
				Vec::new(),
			),
			Op::Punch { offset, len } => (
				opcode::Fallocate::new(raw_fd, len)
					.offset(offset)
					.mode(libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE)
					.build(),
				Vec::new(),
			),
			Op::Accept => {
				let flags = (rustix::net::SocketFlags::CLOEXEC | rustix::net::SocketFlags::NONBLOCK)
					.bits() as i32;
				(
					opcode::Accept::new(raw_fd, std::ptr::null_mut(), std::ptr::null_mut())
						.flags(flags)
						.build(),
					Vec::new(),
				)
			}
			Op::Recv { span } => {
				let buf = only(&span, "receive")?;
				(
					opcode::Recv::new(raw_fd, buf.ptr, capped(buf.len)).build(),
					Vec::new(),
				)
			}
			Op::RecvWithFds { span } => {
				let buf = only(&span, "receive")?;
				let mut area = Control::receive(buf.ptr, buf.len);
				let message = area.message();
				control = Some(area);
				(
					opcode::RecvMsg::new(raw_fd, message)
						.flags(libc::MSG_CMSG_CLOEXEC as u32)
						.build(),
					Vec::new(),
				)
			}
			Op::Send { span, fds } => {
				let buf = only(&span, "send")?;
				if fds.is_empty() {
					(
						opcode::Send::new(raw_fd, buf.ptr, capped(buf.len)).build(),
						Vec::new(),
					)
				} else {
					let mut area = Control::send(buf.ptr, buf.len, fds)?;
					let message = area.message();
					control = Some(area);
					(opcode::SendMsg::new(raw_fd, message).build(), Vec::new())
				}
			}
			Op::Readable => (
				opcode::PollAdd::new(raw_fd, rustix::event::PollFlags::IN.bits() as u32).build(),
				Vec::new(),
			),
			Op::Writable => (
				opcode::PollAdd::new(raw_fd, rustix::event::PollFlags::OUT.bits() as u32).build(),
				Vec::new(),
			),
			Op::Hangup => {
				// The kernel ORs IO_POLL_UNMASK (EPOLLERR | EPOLLHUP | EPOLLNVAL |
				// EPOLLRDHUP, io_uring/poll.c) into every poll's events, so even an
				// empty mask fires on a half-close. Multishot keeps the poll armed
				// past those; `reap` waits for a mask with HUP or ERR.
				(
					opcode::PollAdd::new(raw_fd, 0).multi(true).build(),
					Vec::new(),
				)
			}
		};
		Ok((
			request,
			Active {
				id,
				buffers,
				control,
				kind,
			},
			duplicate,
		))
	}

	/// Builds a vectored request: leases an iovec array, fills it from `span`,
	/// and names it in the entry. The array moves into the operation's `active`
	/// entry — keeping its heap address — and goes back to the pool when the
	/// completion releases it.
	fn vectored(
		&mut self,
		span: &Span,
		entry: impl FnOnce(*const libc::iovec, u32) -> squeue::Entry,
	) -> Result<(squeue::Entry, Vec<libc::iovec>)> {
		let count = u32::try_from(span.extents().len()).map_err(|_| {
			Error::from(io::Error::new(
				io::ErrorKind::InvalidInput,
				"too many buffers",
			))
		})?;
		let mut buffers = lease(&mut self.iovecs);
		let resolved = span.resolve_with(|buf| {
			buffers.push(libc::iovec {
				iov_base: buf.ptr.cast(),
				iov_len: buf.len,
			});
		});
		match resolved {
			Ok(()) => Ok((entry(buffers.as_ptr(), count), buffers)),
			Err(error) => {
				release(&mut self.iovecs, buffers);
				Err(error)
			}
		}
	}
}

impl Driver for Uring {
	fn submit(&mut self, id: u64, fd: BorrowedDescriptor<'_>, op: Op) -> Result<()> {
		// Prepared before the request names the id: a submission that fails before
		// anything reaches the ring gives the caller the refusal, and the reactor
		// takes its entry back out.
		let (request, active, duplicate) = self.prepare(id, fd, op)?;
		// The request names the slot the operation is filed in, not the
		// reactor's id: the ring hands the token back, and the table maps it to
		// the id its completion reports.
		let key = self.active.insert(active);
		if let Err(error) = self.push(request.user_data(key.data().as_ffi())) {
			// Nothing reached the ring, so the operation never was. The guard in
			// `duplicate` closes what the request would have closed, and the
			// leased iovec array goes back to the pool.
			let active = self
				.active
				.remove(key)
				.expect("the operation was just filed");
			release(&mut self.iovecs, active.buffers);
			return Err(Error::from(error));
		}
		// The request names the duplicate a flush runs on, so the kernel's own
		// close owns it from the moment it is queued — whether or not the wake
		// below reaches a sleeping SQPOLL thread. From here it is not this call's
		// to close.
		std::mem::forget(duplicate);
		if self.sqpoll {
			// SQPOLL consumes submissions itself but needs a syscall to wake its
			// sleeping thread. A wake that fails leaves the request queued all the
			// same, so the operation stays with the ring — its iovecs and control
			// area may not be freed while the kernel can still read them.
			self.ring.submit()?;
		}
		Ok(())
	}

	fn cancel(&mut self, id: u64) {
		// The kernel cancels by the token it holds, which only the table maps
		// back to the reactor's id.
		let token = self
			.active
			.iter()
			.find(|(_, active)| active.id == id)
			.map(|(key, _)| key.data().as_ffi());
		let Some(token) = token else {
			return;
		};
		let entry = opcode::AsyncCancel::new(token)
			.build()
			.user_data(CANCEL_TOKEN);
		let _ = self.push(entry).and_then(|()| self.ring.submit());
	}

	fn poll(
		&mut self,
		timeout: Option<Duration>,
		mut complete: impl FnMut(u64, Result<Response>),
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
		Waker::from(Arc::clone(&self.signal))
	}
}

impl Drop for Uring {
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
			if self.ring.submit_and_wait(1).is_err() || self.reap(|_, _| {}).is_err() {
				return;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// `NO_IOWAIT` rides every blocking wait on a kernel that reports the
	/// feature, unless the `iowait` feature withholds it.
	#[test]
	fn the_no_iowait_flag_follows_the_kernel_and_the_feature() -> io::Result<()> {
		let mut uring = Uring::new(&Config::default())?;
		uring.no_iowait = false;
		assert!(!uring.wait_flags().contains(EnterFlags::NO_IOWAIT));
		uring.no_iowait = true;
		assert_eq!(
			uring.wait_flags().contains(EnterFlags::NO_IOWAIT),
			!cfg!(feature = "iowait"),
		);
		Ok(())
	}

	/// A kernel without `NO_IOWAIT` takes the same `io_uring_enter` wait.
	#[test]
	fn a_blocking_wait_works_without_the_feature() -> io::Result<()> {
		let mut uring = Uring::new(&Config::default())?;
		uring.no_iowait = false;
		let start = Instant::now();
		uring.poll(Some(Duration::from_millis(5)), |_, _| {})?;
		assert!(
			start.elapsed() >= Duration::from_millis(5),
			"the wait did not hold for its deadline"
		);
		Ok(())
	}
}
