//! Readiness polling for sockets and a thread pool for file I/O.
//!
//! The reactor files each operation under an id; this backend files it again
//! under a key of its own tables — one for the operations waiting on the
//! poller, one for the jobs on the pool — and each entry names the reactor's
//! id. The poller names a source by a key of the `sources` slot map, which a
//! readiness set carries alongside the operations waiting on it.
//!
//! What a descriptor is, and what a platform can do with one, differ, so the
//! `unix` and `windows` modules name one, route its operations, and make the
//! file calls. The tables, the poll cycle, the registrations, the socket calls,
//! and the pool over them are the same everywhere, the socket calls through
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
use socket2::SockRef;

use super::Op;
use crate::desc::{BorrowedDescriptor, OwnedSocket};
use crate::error::{Error, Result};
use crate::memory::Resolved;

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

/// Where an operation runs, which a platform decides from its descriptor and
/// the operation itself.
enum Route {
	/// On the pool, as the file call `Kind` names.
	Pool(Kind),
	/// On the poller, waiting for the descriptor to be ready.
	Watch,
	/// Here, on the reactor thread, before the operation is answered.
	#[cfg(unix)]
	Inline,
	/// Nowhere, with the reason the submission reports.
	Unsupported(&'static str),
}

enum Progress {
	Done(Response),
	Blocked,
}

/// A wake that rings the poller, the backend's own primitive: both a finished
/// pool job and a cross-thread [`Waker`] reach the reactor through it. A notify
/// that a zero-timeout wait takes instead loses nothing, since every poll
/// collects the pool after its wait.
struct PollerWake(Arc<Poller>);

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

pub(crate) struct Portable {
	poller: Arc<Poller>,
	/// The key of the source each descriptor the poller knows is registered
	/// under.
	keys: HashMap<sys::Raw, DefaultKey>,
	/// Each source the poller knows, named by the key it reports readiness
	/// under, with the operations waiting on it.
	sources: SlotMap<DefaultKey, Source>,
	/// The operations waiting on the poller, named by a key the readiness sets
	/// carry: the reactor's id, the descriptor, the operation to run when the
	/// poller reports that descriptor ready, and the buffer it transfers through.
	/// The buffer is resolved once, when the operation is submitted.
	waiting: SlotMap<DefaultKey, Waiting>,
	/// The jobs running on the pool, named by a key of this map; each job names
	/// the id the pool reports it by. A worker cannot be interrupted, so a job
	/// stays here until it lands; the reactor's entry may already be gone.
	pool: SlotMap<DefaultKey, Job>,
	/// Completions that landed outside `poll`'s wait — a submission answered here
	/// and now, a cancel, or a job the pool finished — kept until the next `poll`
	/// hands them over.
	done: Vec<(u64, Result<Response>)>,
	waker: Waker,
	events: Events,
	scratch: Vec<Ready>,
}

/// One descriptor the poller reports readiness for, and the operations waiting
/// on it in each direction, named by a key of the `waiting` slot map.
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

/// One operation waiting for the poller to report its descriptor ready.
struct Waiting {
	id: u64,
	fd: sys::Raw,
	op: Op,
	/// The first extent of the operation's span, resolved when it was submitted:
	/// every call this backend makes outside the pool transfers through one
	/// buffer.
	buffer: Option<Resolved>,
}

/// One job the pool is running.
struct Job {
	id: u64,
	task: Task<(u64, io::Result<Response>)>,
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

impl Portable {
	pub(super) fn new(config: &Config) -> io::Result<Portable> {
		if config.iopoll || config.sqpoll {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"this backend has no IOPOLL or SQPOLL mode",
			));
		}
		let poller = Arc::new(Poller::new()?);
		Ok(Portable {
			poller: Arc::clone(&poller),
			keys: HashMap::new(),
			sources: SlotMap::new(),
			waiting: SlotMap::new(),
			pool: SlotMap::new(),
			done: Vec::new(),
			waker: Waker::from(Arc::new(PollerWake(poller))),
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
		let (id, fd, direction, outcome) = {
			let Some(waiting) = self.waiting.get_mut(key) else {
				// Gone, or a second report the first pass has already answered.
				return Ok(());
			};
			let fd = waiting.fd;
			let Some(direction) = direction(&waiting.op) else {
				return Ok(());
			};
			(
				waiting.id,
				fd,
				direction,
				run_watched(fd, &mut waiting.op, waiting.buffer),
			)
		};
		let landed = match outcome {
			// The call would block after all: the registration stays, and the
			// poller reports the descriptor again.
			Ok(Progress::Blocked) => return Ok(()),
			Ok(Progress::Done(response)) => Ok(response),
			Err(error) => Err(Error::from(error)),
		};
		self.unwatch(fd, direction, key)?;
		self.waiting
			.remove(key)
			.expect("the operation was just run");
		self.done.push((id, landed));
		Ok(())
	}

	fn submit_pool(
		&mut self,
		id: u64,
		fd: sys::Raw,
		fdoff: u64,
		kind: Kind,
		buffer: Option<Resolved>,
	) -> io::Result<()> {
		if matches!(kind, Kind::Read | Kind::Write) && buffer.is_none() {
			self.done.push((id, Ok(Response::Count(0))));
			return Ok(());
		}
		let task = unblock(move || (id, sys::run_file(fd, fdoff, kind, buffer)));
		self.pool.insert(Job { id, task });
		Ok(())
	}

	fn collect_pool(&mut self) {
		if self.pool.is_empty() {
			return;
		}
		let mut landed = Vec::new();
		let waker = &self.waker;
		for (key, job) in &mut self.pool {
			let mut context = Context::from_waker(waker);
			if let TaskPoll::Ready((id, result)) =
				std::pin::Pin::new(&mut job.task).poll(&mut context)
			{
				landed.push((key, id, result));
			}
		}
		for (key, id, result) in landed {
			self.pool.remove(key);
			self.done.push((id, result.map_err(Error::from)));
		}
	}
}

impl Drop for Portable {
	fn drop(&mut self) {
		// The poller requires deregistration before descriptors are closed.
		for (_, source) in std::mem::take(&mut self.sources) {
			let _ = self.delete(source.fd);
		}
		// Workers cannot be interrupted; wait until no job can access the caller's resources.
		while !self.pool.is_empty() {
			self.collect_pool();
			std::thread::sleep(Duration::from_millis(1));
		}
	}
}

impl Driver for Portable {
	fn submit(&mut self, id: u64, fd: BorrowedDescriptor<'_>, op: Op) -> Result<()> {
		let fd = sys::raw(fd);
		match sys::route(fd, &op) {
			Route::Pool(kind) => {
				let fdoff = match &op {
					Op::Read { offset, .. } | Op::Write { offset, .. } => *offset,
					_ => 0,
				};
				let buffer = first_extent(&op)?;
				self.submit_pool(id, fd, fdoff, kind, buffer)
					.map_err(Error::from)
			}
			#[cfg(unix)]
			Route::Inline => {
				let result = sys::run_inline(fd, &op).map_err(Error::from);
				self.done.push((id, result));
				Ok(())
			}
			// A socket call and a wait for readiness differ only in what the
			// operation says to do once the poller reports the descriptor.
			Route::Watch => {
				let Some(direction) = direction(&op) else {
					return Err(unsupported(
						"this operation is not implemented on this backend",
					));
				};
				// Resolved before the operation is queued: an extent out of bounds
				// refuses the submission, and a socket call transfers through the one
				// buffer its span has to be.
				let buffer = match &op {
					Op::Recv { span } | Op::RecvWithFds { span } | Op::Send { span, .. } => {
						if span.extents().len() != 1 {
							return Err(unsupported("a socket transfer needs exactly one buffer"));
						}
						span.resolve_first()?
					}
					op => first_extent(op)?,
				};
				let key = self.waiting.insert(Waiting { id, fd, op, buffer });
				if let Err(error) = self.watch(fd, direction, key) {
					// The poller never took the operation, so it never was: the key
					// goes back out with it.
					self.waiting
						.remove(key)
						.expect("the operation was just filed");
					return Err(Error::from(error));
				}
				Ok(())
			}
			Route::Unsupported(reason) => Err(unsupported(reason)),
		}
	}

	fn cancel(&mut self, id: u64) {
		// A worker already inside a syscall must finish before its buffers can be released.
		if self.pool.values().any(|job| job.id == id) {
			return;
		}
		let Some(key) = self
			.waiting
			.iter()
			.find(|(_, waiting)| waiting.id == id)
			.map(|(key, _)| key)
		else {
			return;
		};
		let waiting = self
			.waiting
			.remove(key)
			.expect("the operation was just found");
		if let Some(direction) = direction(&waiting.op) {
			let _ = self.unwatch(waiting.fd, direction, key);
		}
		self.done.push((id, Err(Error::cancelled())));
	}

	fn poll(
		&mut self,
		timeout: Option<Duration>,
		mut complete: impl FnMut(u64, Result<Response>),
	) -> Result<()> {
		// Collect finished jobs before sleeping so their completions do not wait
		// for the timeout.
		self.collect_pool();
		// A result can land outside this call — a submission with nothing to
		// wait for, a cancel answered here and now, or a job the pool finished
		// before this poll — and the reactor has not settled it: sleeping now
		// would wait out a timeout on news already in hand.
		if !self.done.is_empty() {
			for (id, response) in self.done.drain(..) {
				complete(id, response);
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
		for (id, response) in self.done.drain(..) {
			complete(id, response);
		}
		Ok(())
	}

	fn waker(&self) -> Waker {
		self.waker.clone()
	}
}

/// The error an operation this platform cannot run reports.
fn unsupported(reason: &'static str) -> Error {
	Error::from(io::Error::new(io::ErrorKind::Unsupported, reason))
}

/// The direction an operation waits on for the poller to report it ready;
/// `None` for one that never waits.
fn direction(op: &Op) -> Option<Direction> {
	match op {
		Op::Read { .. } | Op::Recv { .. } | Op::RecvWithFds { .. } | Op::Accept | Op::Readable => {
			Some(Direction::Readable)
		}
		Op::Write { .. } | Op::Send { .. } | Op::Writable => Some(Direction::Writable),
		_ => None,
	}
}

/// The first extent of the span `op` transfers through, with every extent
/// resolved so one out of bounds refuses the submission; `None` for an
/// operation that transfers no bytes.
fn first_extent(op: &Op) -> Result<Option<Resolved>> {
	match op.span() {
		Some(span) => span.resolve_first(),
		None => Ok(None),
	}
}

/// The first of `buffer` as a pointer and length, for a call that takes one.
fn first_buffer(buffer: Option<Resolved>) -> Option<(*mut u8, usize)> {
	buffer.map(|buf| (buf.ptr, buf.len.min(MAX_TRANSFER)))
}

/// The one buffer a socket call transfers through, which `submit` checked the
/// operation's span is.
fn one_buffer(buffer: Option<Resolved>) -> io::Result<(*mut u8, usize)> {
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

/// Calls the operation the poller reported ready that is one call on every
/// platform; the rest are the platform's own.
fn run_watched(fd: sys::Raw, op: &mut Op, buffer: Option<Resolved>) -> io::Result<Progress> {
	match op {
		Op::Recv { .. } => recv(fd, buffer),
		Op::Send { fds, .. } if fds.is_empty() => send(fd, buffer),
		Op::Accept => accept(fd),
		// A wait for readiness is answered by the poller itself.
		Op::Readable | Op::Writable => Ok(Progress::Done(Response::Done)),
		// A unix descriptor takes `read`/`write` where a socket takes
		// `recv`/`send`, and a send that carries descriptors is a `sendmsg`.
		Op::Read { .. } | Op::Write { .. } | Op::Send { .. } | Op::RecvWithFds { .. } => {
			sys::run_op(fd, op, buffer)
		}
		_ => unreachable!("only a socket call and a readiness wait reach the poller"),
	}
}

/// Receives into the one buffer `buffer` names, on the socket the reactor
/// holds open.
fn recv(fd: sys::Raw, buffer: Option<Resolved>) -> io::Result<Progress> {
	let (ptr, len) = one_buffer(buffer)?;
	// SAFETY: the reactor holds the memory this buffer names until the operation
	// lands, and the call is the only mover of the range while the slice stands.
	let bytes = unsafe { slice::from_raw_parts_mut(ptr.cast::<MaybeUninit<u8>>(), len) };
	let source = sys::borrow(fd)?;
	counted(SockRef::from(&source).recv(bytes))
}

/// Sends the one buffer `buffer` names, on the socket the reactor holds open.
fn send(fd: sys::Raw, buffer: Option<Resolved>) -> io::Result<Progress> {
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

#[cfg(all(test, unix))]
mod tests {
	use super::*;

	use crate::{AsDescriptor, Config};

	/// Closed-descriptor deregistration differs by poller: epoll may reject it, while kqueue may have removed it already. Either outcome must discard the registration.
	#[test]
	fn a_deregistration_of_a_closed_descriptor_leaves_no_key_behind() {
		let mut backend = Portable::new(&Config::default()).unwrap();
		let (reader, _writer) = std::io::pipe().unwrap();
		let fd = sys::raw(reader.as_descriptor());
		let key = backend.waiting.insert(Waiting {
			id: 7,
			fd,
			op: Op::Readable,
			buffer: None,
		});
		backend.watch(fd, Direction::Readable, key).unwrap();
		let registered = backend.keys[&fd];
		drop(reader);
		let _ = backend.unwatch(fd, Direction::Readable, key);
		assert!(!backend.keys.contains_key(&fd));
		assert!(!backend.sources.contains_key(registered));
		assert!(backend.sources.is_empty());
	}

	/// Simulate a poller that lost the source before deregistration; stale readiness must not retain or dispatch the old key.
	#[test]
	fn a_refused_deregistration_leaves_no_key_behind() {
		let mut backend = Portable::new(&Config::default()).unwrap();
		let (reader, _writer) = std::io::pipe().unwrap();
		let fd = sys::raw(reader.as_descriptor());
		let key = backend.waiting.insert(Waiting {
			id: 7,
			fd,
			op: Op::Readable,
			buffer: None,
		});
		backend.watch(fd, Direction::Readable, key).unwrap();
		let registered = backend.keys[&fd];
		backend.delete(fd).unwrap();
		assert!(backend.unwatch(fd, Direction::Readable, key).is_err());
		assert!(!backend.keys.contains_key(&fd));
		assert!(!backend.sources.contains_key(registered));
		assert!(backend.sources.is_empty());
	}
}
