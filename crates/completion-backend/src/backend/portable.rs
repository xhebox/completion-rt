//! Readiness polling for sockets and a thread pool for file I/O.
//!
//! Each operation is filed under a key of this backend's one `active` slot map:
//! the waiting ones name the source the poller watches them through, and the
//! ones on the pool name the job running them. The key's FFI form is the `u64`
//! id a submission hands back and a completion reports under. The poller names a
//! source by a key of the `sources` slot map, which a readiness set carries
//! alongside the operations waiting on it.
//!
//! What a descriptor is, and what a platform can do with one, differ, so the
//! `unix` and `windows` modules name one, place each operation, and make the
//! calls. The tables, the poll cycle, the registrations, the socket calls, and
//! the pool over them are the same everywhere, the socket calls through
//! `socket2`.

use std::collections::HashMap;
use std::io;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::slice;
use std::sync::Arc;
use std::task::{Context, Poll as TaskPoll, Wake, Waker};
use std::time::Duration;

use blocking::{Task, unblock};
use polling::{Event as Ready, Events, PollMode, Poller};
use slotmap::{DefaultKey, Key, KeyData, SlotMap};
use smallvec::SmallVec;
use socket2::{SockAddr, SockRef};

use super::Op;
use crate::desc::{BorrowedDescriptor, OwnedSocket};
use crate::error::{Error, Result};
use crate::memory::IoVec;

use super::{Config, Driver, Response};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
use self::unix as sys;
#[cfg(windows)]
use self::windows as sys;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Direction {
	Readable,
	Writable,
}

const EVENTS: usize = 64;

/// The most bytes one call names: the calls take a signed length, and a unix
/// that refuses a length above `ssize_t` would fail the whole transfer. The
/// kernel caps a single call at its own limit anyway, so a longer buffer moves
/// what fits and reports a short count, which the exact wrappers repeat.
const MAX_TRANSFER: usize = i32::MAX as usize;

/// Where a platform runs a byte transfer, which only its descriptor decides.
enum Transfer {
	/// On the pool, as a file call: a regular file has no readiness to wait on.
	Pool,
	/// On the poller, when the descriptor reports ready.
	Watch,
}

/// Where a platform runs a sync or a flush.
enum SyncFlush {
	/// On the pool, as a file call.
	Pool,
	/// Here, on the reactor thread: a descriptor that is no regular file has no
	/// file call the pool could make.
	#[cfg(unix)]
	Inline,
}

/// The file call the pool makes for an operation.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
	Read,
	Write,
	Fsync,
	Flush,
	Allocate { offset: u64, len: u64 },
	Punch { offset: u64, len: u64 },
}

enum Progress {
	Done(Response),
	Blocked,
}

/// What beginning a connect did: a socket the kernel connected here and now
/// has nothing to wait for, and one it is still connecting on waits for the
/// poller.
enum Handshake {
	Connected,
	InFlight,
}

/// A wake that rings the poller, the backend's own primitive: both a finished
/// pool job and a cross-thread [`Waker`] reach the reactor through it. A notify
/// that a zero-timeout wait takes instead loses nothing, since every poll
/// collects the pool after its wait.
struct PollerWake(Poller);

impl std::ops::Deref for PollerWake {
	type Target = Poller;

	fn deref(&self) -> &Poller {
		&self.0
	}
}

impl Wake for PollerWake {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		if let Err(error) = self.0.notify() {
			log::warn!(error:%; "the poller refused a wake");
		}
	}
}

pub struct Portable<T = ()> {
	poller: Arc<PollerWake>,
	/// The key of the source each descriptor the poller knows is registered
	/// under.
	keys: HashMap<sys::Raw, DefaultKey>,
	/// Each source the poller knows, named by the key it reports readiness
	/// under, with the operations waiting on it.
	sources: SlotMap<DefaultKey, Source>,
	/// The operations this backend runs, named by a key the readiness sets
	/// carry and by the pool job itself. A worker cannot be interrupted, so a
	/// job stays here until it lands; the caller's entry may already be gone.
	active: SlotMap<DefaultKey, Active<T>>,
	/// Completions that landed outside `poll`'s wait — a submission answered here
	/// and now, a cancel, or a job the pool finished — kept until the next `poll`
	/// hands them over.
	done: Vec<(u64, T, Result<Response>)>,
	events: Events,
	scratch: Vec<Ready>,
}

/// One descriptor the poller reports readiness for, and the operations waiting
/// on it in each direction, named by a key of the `active` slot map.
struct Source {
	fd: sys::Raw,
	readers: SmallVec<[DefaultKey; 2]>,
	writers: SmallVec<[DefaultKey; 2]>,
}

impl Source {
	/// Files an operation waiting for `direction`; the same key is filed once.
	fn add(&mut self, direction: Direction, key: DefaultKey) {
		let keys = self.waiting_in(direction);
		if !keys.contains(&key) {
			keys.push(key);
		}
	}

	/// Takes an operation waiting for `direction` back out.
	fn remove(&mut self, direction: Direction, key: DefaultKey) {
		self.waiting_in(direction).retain(|waiting| *waiting != key);
	}

	/// Whether no operation waits on the source any more.
	fn idle(&self) -> bool {
		self.readers.is_empty() && self.writers.is_empty()
	}

	/// The operations waiting in `direction`.
	fn waiting_in(&mut self, direction: Direction) -> &mut SmallVec<[DefaultKey; 2]> {
		match direction {
			Direction::Readable => &mut self.readers,
			Direction::Writable => &mut self.writers,
		}
	}

	/// The poller interest for the source, which the poller names by `key`.
	fn interest(&self, key: DefaultKey) -> Ready {
		match (self.readers.is_empty(), self.writers.is_empty()) {
			(false, false) => Ready::all(event_key(key)),
			(false, true) => Ready::readable(event_key(key)),
			(true, false) => Ready::writable(event_key(key)),
			(true, true) => Ready::none(event_key(key)),
		}
	}
}

/// One operation this backend runs, and where it runs.
enum Active<T> {
	/// Waiting for the poller to report its descriptor ready.
	Waiting {
		fd: sys::Raw,
		op: Op,
		/// The range of the operation's span, resolved when it was submitted: every
		/// call this backend makes outside the pool transfers through one buffer.
		/// `None` for an operation that transfers nothing.
		buffer: Option<IoVec>,
		/// The caller's data, handed back with the completion.
		data: T,
	},
	/// A job the pool is running. The key it is filed under is what its
	/// completion reports.
	Pool {
		task: Task<io::Result<Response>>,
		/// The caller's data, handed back with the completion.
		data: T,
	},
	/// An operation answered here and now, whose slot goes with its entry.
	Done,
}

/// The poller key a slot map key is reported under. `polling` names an event
/// by a `usize`; on the targets this backend serves that holds the key's FFI
/// form whole, so a key handed out again cannot alias a stale event.
fn event_key(key: DefaultKey) -> usize {
	key.data().as_ffi() as usize
}

/// The slot map key an event's poller key names.
fn source(key: usize) -> DefaultKey {
	DefaultKey::from(KeyData::from_ffi(key as u64))
}

impl<T> Portable<T> {
	pub(super) fn new(config: &Config) -> io::Result<Portable<T>> {
		if config.iopoll || config.sqpoll {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"this backend has no IOPOLL or SQPOLL mode",
			));
		}
		Ok(Portable {
			poller: Arc::new(PollerWake(Poller::new()?)),
			keys: HashMap::new(),
			sources: SlotMap::new(),
			active: SlotMap::new(),
			done: Vec::new(),
			events: Events::with_capacity(NonZeroUsize::new(EVENTS).expect("a nonzero capacity")),
			scratch: Vec::with_capacity(EVENTS),
		})
	}

	/// Registers a source the poller has not seen, level-triggered.
	fn add(&self, fd: sys::Raw, interest: Ready) -> io::Result<()> {
		let source = sys::borrow(fd)?;
		// SAFETY: the reactor keeps the descriptor open and `delete`s the source
		// from the poller before it goes.
		unsafe {
			self.poller
				.add_with_mode(&source, interest, PollMode::Level)
		}
	}

	/// Rearms a source the poller already holds.
	fn modify(&self, fd: sys::Raw, interest: Ready) -> io::Result<()> {
		let source = sys::borrow(fd)?;
		self.poller
			.modify_with_mode(source, interest, PollMode::Level)
	}

	/// Deregisters a source, which the reactor still holds open.
	fn delete(&self, fd: sys::Raw) -> io::Result<()> {
		let source = sys::borrow(fd)?;
		self.poller.delete(source)
	}

	/// Rearms the poller for a source it already holds, whose key is `key`:
	/// level triggering reports a still-ready source again after rearming.
	fn arm(&mut self, fd: sys::Raw, key: DefaultKey) -> io::Result<()> {
		let interest = self.sources[key].interest(key);
		self.modify(fd, interest)
	}

	fn watch(&mut self, fd: sys::Raw, direction: Direction, key: DefaultKey) -> io::Result<()> {
		if let Some(&source_key) = self.keys.get(&fd) {
			self.sources[source_key].add(direction, key);
			match self.arm(fd, source_key) {
				Ok(()) => Ok(()),
				Err(error) => {
					// Nothing is registered for this operation, and the poller must not
					// report the descriptor as waiting for it: the key goes back out.
					let _ = self.unwatch(fd, direction, key);
					Err(error)
				}
			}
		} else {
			// A readiness backend calls the operation when the poller says it
			// is ready, and a descriptor the caller left in blocking mode would
			// stall the reactor in that call. A socket's MSG_DONTWAIT does not
			// cover it: XNU's sosendcheck only honours SS_NBIO. `socket2` sets
			// the flag with `fcntl(F_SETFL, O_NONBLOCK)` on unix — which a pipe
			// takes as well as a socket — and with `ioctlsocket(FIONBIO)` on a
			// socket on Windows.
			let source = sys::borrow(fd)?;
			SockRef::from(&source).set_nonblocking(true)?;
			let mut entry = Source {
				fd,
				readers: SmallVec::new(),
				writers: SmallVec::new(),
			};
			entry.add(direction, key);
			let source_key = self.sources.insert(entry);
			let interest = self.sources[source_key].interest(source_key);
			if let Err(error) = self.add(fd, interest) {
				// The poller never took the source, so nothing may name it.
				self.sources.remove(source_key);
				return Err(error);
			}
			self.keys.insert(fd, source_key);
			Ok(())
		}
	}

	fn unwatch(&mut self, fd: sys::Raw, direction: Direction, key: DefaultKey) -> io::Result<()> {
		let Some(&source_key) = self.keys.get(&fd) else {
			return Ok(());
		};
		self.sources[source_key].remove(direction, key);
		self.rearm(fd)
	}

	fn rearm(&mut self, fd: sys::Raw) -> io::Result<()> {
		let Some(&key) = self.keys.get(&fd) else {
			return Ok(());
		};
		if !self.sources[key].idle() {
			return self.arm(fd, key);
		}
		self.keys.remove(&fd);
		self.sources.remove(key);
		self.delete(fd).inspect_err(
			|error| log::warn!(fd:? = fd, error:% = error; "the poller refused a deregistration"),
		)
	}

	fn deliver(&mut self, event: &Ready) -> io::Result<()> {
		let Some(registered) = self.sources.get(source(event.key)) else {
			return Ok(());
		};
		// Taken first: answering a key takes it out of the source it came from.
		let mut keys: SmallVec<[DefaultKey; 4]> = SmallVec::new();
		if event.readable {
			keys.extend(registered.readers.iter().copied());
		}
		if event.writable {
			keys.extend(registered.writers.iter().copied());
		}
		for key in keys {
			self.run_ready(key)?;
		}
		Ok(())
	}

	/// Calls the operation the poller reported ready. One still blocked stays
	/// registered, and a level-triggered poller reports it again.
	fn run_ready(&mut self, key: DefaultKey) -> io::Result<()> {
		let (fd, direction, outcome) = {
			let Some(Active::Waiting { fd, op, buffer, .. }) = self.active.get_mut(key) else {
				// Gone, or the report the first pass has already answered.
				return Ok(());
			};
			let fd = *fd;
			let direction = direction(op);
			let outcome = run_watched(op, fd, *buffer);
			(fd, direction, outcome)
		};
		let landed = match outcome {
			// The call would block after all: the registration stays, and the
			// poller reports the descriptor again.
			Ok(Progress::Blocked) => return Ok(()),
			Ok(Progress::Done(response)) => Ok(response),
			Err(error) => Err(Error::from(error)),
		};
		self.unwatch(fd, direction, key)?;
		let Some(Active::Waiting { data, .. }) = self.active.remove(key) else {
			unreachable!("the operation waiting on the poller was just run");
		};
		self.done.push((key.data().as_ffi(), data, landed));
		Ok(())
	}

	/// Files an operation waiting for the poller to report its descriptor ready,
	/// and registers it.
	fn wait(&mut self, fd: sys::Raw, op: Op, buffer: Option<IoVec>, data: T) -> Result<u64> {
		let direction = direction(&op);
		let key = self.active.insert(Active::Waiting {
			fd,
			op,
			buffer,
			data,
		});
		if let Err(error) = self.watch(fd, direction, key) {
			// The poller never took the operation, so it never was: the key goes
			// back out with it.
			self.active
				.remove(key)
				.expect("the operation was just filed");
			return Err(Error::from(error));
		}
		Ok(key.data().as_ffi())
	}

	/// Answers an operation here and now, before the next poll hands it over.
	fn answer(&mut self, response: Result<Response>, data: T) -> Result<u64> {
		// An operation answered here and now still needs an id of its own, and
		// the slot it takes is free again straight away: the generation moves
		// on, so no later submission is filed under this id.
		let key = self.active.insert(Active::Done);
		self.active.remove(key);
		let id = key.data().as_ffi();
		self.done.push((id, data, response));
		Ok(id)
	}

	fn submit_pool(
		&mut self,
		fd: sys::Raw,
		fdoff: u64,
		kind: Kind,
		buffer: Option<IoVec>,
		data: T,
	) -> Result<u64> {
		if matches!(kind, Kind::Read | Kind::Write) && buffer.is_none() {
			return self.answer(Ok(Response::Count(0)), data);
		}
		let task = unblock(move || sys::run_file(fd, fdoff, kind, buffer));
		let key = self.active.insert(Active::Pool { task, data });
		Ok(key.data().as_ffi())
	}

	fn collect_pool(&mut self) {
		let mut landed = Vec::new();
		let waker = self.waker();
		for (key, active) in &mut self.active {
			let Active::Pool { task, .. } = active else {
				continue;
			};
			let mut context = Context::from_waker(&waker);
			if let TaskPoll::Ready(result) = std::pin::Pin::new(task).poll(&mut context) {
				landed.push((key, result));
			}
		}
		for (key, result) in landed {
			let Some(Active::Pool { data, .. }) = self.active.remove(key) else {
				unreachable!("the job the pool just finished was filed as one");
			};
			self.done
				.push((key.data().as_ffi(), data, result.map_err(Error::from)));
		}
	}
}

impl<T> Drop for Portable<T> {
	fn drop(&mut self) {
		// The poller requires deregistration before descriptors are closed.
		for (_, source) in std::mem::take(&mut self.sources) {
			let _ = self.delete(source.fd);
		}
		// Workers cannot be interrupted; wait until no job can access the
		// caller's resources.
		while self
			.active
			.values()
			.any(|active| matches!(active, Active::Pool { .. }))
		{
			self.collect_pool();
			std::thread::sleep(Duration::from_millis(1));
		}
	}
}

impl<T> Driver<T> for Portable<T> {
	fn submit(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64> {
		self.submit_op(fd, op, data)
	}

	fn cancel(&mut self, id: u64) {
		// The id is the key the operation is filed under. A worker already
		// inside a syscall must finish before its buffers can be released, so
		// only a waiting operation is taken back out; an id that names nothing
		// has nothing to stop.
		let key = DefaultKey::from(KeyData::from_ffi(id));
		let Some(Active::Waiting { .. }) = self.active.get(key) else {
			return;
		};
		let Some(Active::Waiting { fd, op, data, .. }) = self.active.remove(key) else {
			unreachable!("the operation waiting on the poller has no job");
		};
		let _ = self.unwatch(fd, direction(&op), key);
		self.done.push((id, data, Err(Error::cancelled())));
	}

	fn cancel_all(&mut self) {
		let ids: Vec<u64> = self.active.keys().map(|key| key.data().as_ffi()).collect();
		for id in ids {
			// A job the pool is running cannot be stopped; only the operations
			// waiting on the poller are taken back out.
			self.cancel(id);
		}
	}

	fn is_idle(&self) -> bool {
		self.active.is_empty() && self.done.is_empty()
	}

	fn poll(
		&mut self,
		timeout: Option<Duration>,
		mut complete: impl FnMut(u64, T, Result<Response>),
	) -> Result<()> {
		// Collect finished jobs before sleeping so their completions do not wait
		// for the timeout.
		self.collect_pool();
		// A result can land outside this call — a submission with nothing to
		// wait for, a cancel answered here and now, or a job the pool finished
		// before this poll — and the reactor has not settled it: sleeping now
		// would wait out a timeout on news already in hand.
		if !self.done.is_empty() {
			for (id, data, response) in self.done.drain(..) {
				complete(id, data, response);
			}
			return Ok(());
		}
		self.events.clear();
		self.poller.wait(&mut self.events, timeout)?;
		let mut batch = std::mem::take(&mut self.scratch);
		batch.clear();
		batch.extend(self.events.iter());
		let result = batch.iter().try_for_each(|event| self.deliver(event));
		self.scratch = batch;
		result?;
		self.collect_pool();
		for (id, data, response) in self.done.drain(..) {
			complete(id, data, response);
		}
		Ok(())
	}

	fn waker(&self) -> Waker {
		Waker::from(Arc::clone(&self.poller))
	}
}

impl<T> Portable<T> {
	/// Files the operation `op` names where its descriptor and the platform say
	/// it runs, and returns the id its completion reports under.
	fn submit_op(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64> {
		let raw = sys::raw(fd);
		match op {
			Op::Read { offset, span } => {
				// Resolved before the operation is queued, whenever it lands: an
				// span out of bounds refuses the submission.
				let buffer = Some(span.resolve()?);
				match sys::transfer(raw) {
					Transfer::Pool => {
						self.submit_pool(raw, offset.unwrap_or(0), Kind::Read, buffer, data)
					}
					Transfer::Watch => self.wait(raw, Op::Read { offset, span }, buffer, data),
				}
			}
			Op::Write { offset, span } => {
				let buffer = Some(span.resolve()?);
				match sys::transfer(raw) {
					Transfer::Pool => {
						self.submit_pool(raw, offset.unwrap_or(0), Kind::Write, buffer, data)
					}
					Transfer::Watch => self.wait(raw, Op::Write { offset, span }, buffer, data),
				}
			}
			Op::Fsync => match sys::sync_flush(raw)? {
				SyncFlush::Pool => self.submit_pool(raw, 0, Kind::Fsync, None, data),
				// A descriptor that is no regular file cannot be synced by the
				// pool: the call is made here, on the reactor thread.
				#[cfg(unix)]
				SyncFlush::Inline => self.answer(
					sys::run_file(raw, 0, Kind::Fsync, None).map_err(Error::from),
					data,
				),
			},
			Op::Flush => match sys::sync_flush(raw)? {
				SyncFlush::Pool => self.submit_pool(raw, 0, Kind::Flush, None, data),
				#[cfg(unix)]
				SyncFlush::Inline => self.answer(
					sys::run_file(raw, 0, Kind::Flush, None).map_err(Error::from),
					data,
				),
			},
			Op::Allocate { offset, len } => {
				sys::reservable(raw)?;
				self.submit_pool(raw, 0, Kind::Allocate { offset, len }, None, data)
			}
			Op::Punch { offset, len } => {
				sys::reservable(raw)?;
				self.submit_pool(raw, 0, Kind::Punch { offset, len }, None, data)
			}
			Op::Recv { span } => {
				sys::pollable(raw)?;
				let buffer = Some(span.resolve()?);
				self.wait(raw, Op::Recv { span }, buffer, data)
			}
			#[cfg(unix)]
			Op::RecvWithFds { span } => {
				let buffer = Some(span.resolve()?);
				self.wait(raw, Op::RecvWithFds { span }, buffer, data)
			}
			// A receive that carries descriptors has no form on a platform whose
			// socket call has none.
			#[cfg(windows)]
			Op::RecvWithFds { .. } => Err(unsupported("this backend cannot receive descriptors")),
			Op::Send { span, fds } => {
				// A send that carries no descriptor is a socket call, and one
				// that does needs a control message.
				if fds.is_empty() {
					sys::pollable(raw)?;
				} else {
					sys::control_message(raw)?;
				}
				let buffer = Some(span.resolve()?);
				self.wait(raw, Op::Send { span, fds }, buffer, data)
			}
			Op::Accept => {
				sys::pollable(raw)?;
				self.wait(raw, Op::Accept, None, data)
			}
			Op::Connect { addr } => {
				sys::pollable(raw)?;
				// A connect is begun before the poller can report it writable,
				// and the call that begins it answers here: a socket it
				// connected at once needs no wait, and one still connecting — or
				// failed — is this operation's completion.
				match initiate(raw, &addr) {
					Ok(Handshake::InFlight) => self.wait(raw, Op::Connect { addr }, None, data),
					Ok(Handshake::Connected) => self.answer(Ok(Response::Done), data),
					Err(error) => self.answer(Err(Error::from(error)), data),
				}
			}
			Op::Readable => {
				sys::pollable(raw)?;
				self.wait(raw, Op::Readable, None, data)
			}
			Op::Writable => {
				sys::pollable(raw)?;
				self.wait(raw, Op::Writable, None, data)
			}
			Op::Hangup => {
				// `polling`'s interrupt interest is a no-op on kqueue, and its
				// events carry no `EV_EOF`, so a peer gone cannot be told from
				// one that half-closed.
				Err(unsupported("this backend cannot wait for a hangup"))
			}
		}
	}
}

/// The direction the poller has to report ready for a waiting operation.
fn direction(op: &Op) -> Direction {
	match op {
		Op::Read { .. } | Op::Recv { .. } | Op::RecvWithFds { .. } | Op::Accept | Op::Readable => {
			Direction::Readable
		}
		Op::Write { .. } | Op::Send { .. } | Op::Writable | Op::Connect { .. } => {
			Direction::Writable
		}
		_ => unreachable!("only a socket call and a readiness wait reach the poller"),
	}
}

/// Makes the call the poller's report permits, through `buffer` — the one buffer
/// the operation transfers through, resolved when it was submitted.
fn run_watched(op: &mut Op, fd: sys::Raw, buffer: Option<IoVec>) -> io::Result<Progress> {
	match op {
		Op::Read { .. } => sys::read(fd, buffer),
		Op::Write { .. } => sys::write(fd, buffer),
		Op::Recv { .. } => recv(fd, buffer),
		Op::Send { fds, .. } if fds.is_empty() => send(fd, buffer),
		#[cfg(unix)]
		Op::Send { fds, .. } => sys::send_with_fds(fd, buffer, fds),
		#[cfg(unix)]
		Op::RecvWithFds { .. } => sys::recv_with_fds(fd, buffer),
		Op::Accept => accept(fd),
		Op::Connect { .. } => connect(fd),
		// A wait for readiness is answered by the poller itself.
		Op::Readable | Op::Writable => Ok(Progress::Done(Response::Done)),
		_ => unreachable!("only a socket call and a readiness wait reach the poller"),
	}
}

/// The error an operation this platform cannot run reports.
fn unsupported(reason: &'static str) -> Error {
	Error::from(io::Error::new(io::ErrorKind::Unsupported, reason))
}

/// The one buffer a socket call transfers through, or the error for a call that
/// was given none.
fn one_buffer(buffer: Option<IoVec>) -> io::Result<(*mut u8, usize)> {
	let Some(buf) = buffer else {
		return Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a socket transfer needs exactly one buffer",
		));
	};
	Ok((buf.ptr, buf.len.min(MAX_TRANSFER)))
}

/// A socket call's outcome, with `None` for one that would block.
fn blockable<T>(result: io::Result<T>) -> io::Result<Option<T>> {
	match result {
		Ok(value) => Ok(Some(value)),
		Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
		Err(error) => Err(error),
	}
}

/// Settles a call that moves bytes: one that would block stays registered, and
/// the next readiness report answers it.
fn counted(result: io::Result<usize>) -> io::Result<Progress> {
	Ok(match blockable(result)? {
		Some(moved) => Progress::Done(Response::Count(moved)),
		None => Progress::Blocked,
	})
}

/// Receives into the one buffer `buffer` names, on the socket the reactor
/// holds open.
fn recv(fd: sys::Raw, buffer: Option<IoVec>) -> io::Result<Progress> {
	let (ptr, len) = one_buffer(buffer)?;
	// SAFETY: the reactor holds the memory this buffer names until the operation
	// lands, and the call is the only mover of the range while the slice stands.
	let bytes = unsafe { slice::from_raw_parts_mut(ptr.cast::<MaybeUninit<u8>>(), len) };
	let source = sys::borrow(fd)?;
	counted(SockRef::from(&source).recv(bytes))
}

/// Sends the one buffer `buffer` names, on the socket the reactor holds open.
fn send(fd: sys::Raw, buffer: Option<IoVec>) -> io::Result<Progress> {
	let (ptr, len) = one_buffer(buffer)?;
	// SAFETY: the reactor holds the memory this buffer names until the operation
	// lands, and the call only reads it.
	let bytes = unsafe { slice::from_raw_parts(ptr, len) };
	let source = sys::borrow(fd)?;
	counted(SockRef::from(&source).send(bytes))
}

/// Accepts the connection the poller reported ready. The response owns the
/// socket, which `socket2` accepts with the flags a new socket takes (no
/// inheritance) and this backend then runs non-blocking.
fn accept(fd: sys::Raw) -> io::Result<Progress> {
	let source = sys::borrow(fd)?;
	let Some((accepted, _)) = blockable(SockRef::from(&source).accept())? else {
		return Ok(Progress::Blocked);
	};
	let accepted: OwnedSocket = accepted.into();
	SockRef::from(&accepted).set_nonblocking(true)?;
	Ok(Progress::Done(Response::Accepted(accepted)))
}

/// Begins the connection `addr` names on the socket the reactor holds open. The
/// handshake has to start before the poller can report it done, so the call is
/// made here.
fn initiate(fd: sys::Raw, addr: &SockAddr) -> io::Result<Handshake> {
	let source = sys::borrow(fd)?;
	let socket = SockRef::from(&source);
	// The call must not stall the reactor thread, whatever mode the caller left
	// the descriptor in.
	socket.set_nonblocking(true)?;
	match socket.connect(addr) {
		Ok(()) => Ok(Handshake::Connected),
		// The kernel took the handshake and reports its outcome as readiness.
		Err(error) if sys::in_progress(&error) => Ok(Handshake::InFlight),
		Err(error) => Err(error),
	}
}

/// Answers the connection the poller reported writable: a writable socket
/// carries the handshake's outcome as its pending error. One that is writable
/// with no peer has not finished connecting — a report that came too early —
/// and stays registered for the next one.
fn connect(fd: sys::Raw) -> io::Result<Progress> {
	let source = sys::borrow(fd)?;
	let socket = SockRef::from(&source);
	if let Some(error) = socket.take_error()? {
		return Err(error);
	}
	match socket.peer_addr() {
		Ok(_) => Ok(Progress::Done(Response::Done)),
		Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(Progress::Blocked),
		Err(error) => Err(error),
	}
}
