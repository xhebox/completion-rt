//! Readiness polling for sockets and a thread pool for regular files.
//!
//! The reactor files each operation under an id; this backend files it again
//! under a key of its own tables — one for the operations waiting on the
//! poller, one for the jobs on the pool — and each entry names the reactor's
//! id. The poller names a source by a key of the `sources` slot map, which a
//! readiness set carries alongside the operations waiting on it.

use std::collections::HashMap;
use std::io;
use std::mem::{MaybeUninit, size_of};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, RawFd};
use std::sync::Arc;
use std::task::{Context, Poll as TaskPoll, Wake, Waker};
use std::time::Duration;

use blocking::{Task, unblock};
use polling::{Event as Ready, Events, Poller};
use slotmap::{DefaultKey, Key, KeyData, SlotMap};
use smallvec::SmallVec;

use super::Op;
use crate::desc::{BorrowedDescriptor, OwnedDescriptor};
use crate::error::Error;
use crate::memory::{IoVec, Span};

use super::{Config, Response};

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

/// The most descriptors one control message carries.
const MAX_FDS: usize = 8;

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
	keys: HashMap<RawFd, DefaultKey>,
	/// Each source the poller knows, named by the key it reports readiness
	/// under, with the operations waiting on it.
	sources: SlotMap<DefaultKey, Source>,
	/// The operations waiting on the poller, named by a key the readiness sets
	/// carry: the reactor's id, the descriptor, the operation to run when the
	/// poller reports that descriptor ready, and the iovec array it reads
	/// through. The array is resolved once, when the operation is submitted.
	waiting: SlotMap<DefaultKey, Waiting>,
	/// The jobs running on the pool, named by a key of this map; each job names
	/// the id the pool reports it by. A worker cannot be interrupted, so a job
	/// stays here until it lands; the reactor's entry may already be gone.
	pool: SlotMap<DefaultKey, Job>,
	/// The iovec arrays no waiting operation reads any more, each with its
	/// allocation kept for the next one.
	iovecs: Vec<Vec<IoVec>>,
	/// Completions that landed outside `poll`'s wait — a submission answered here
	/// and now, a cancel, or a job the pool finished — kept until the next `poll`
	/// hands them over.
	done: Vec<(u64, Result<Response, Error>)>,
	waker: Waker,
	events: Events,
	scratch: Vec<Ready>,
}

/// One descriptor the poller reports readiness for, and the operations waiting
/// on it in each direction, named by a key of the `waiting` slot map.
struct Source {
	fd: RawFd,
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
	fd: RawFd,
	op: Op,
	buffers: Vec<IoVec>,
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
			iovecs: Vec::new(),
			done: Vec::new(),
			waker: Waker::from(Arc::new(PollerWake(poller))),
			events: Events::with_capacity(NonZeroUsize::new(EVENTS).expect("a nonzero capacity")),
			scratch: Vec::with_capacity(EVENTS),
		})
	}

	/// Rearms the poller for a source it already holds, whose key is `key`:
	/// level triggering reports a still-ready source again after rearming.
	fn arm(&mut self, fd: RawFd, key: DefaultKey) -> io::Result<()> {
		let interest = self.sources[key].interest(key);
		// SAFETY: the descriptor stays open while the reactor holds it.
		let source = unsafe { BorrowedFd::borrow_raw(fd) };
		self.poller
			.modify_with_mode(&source, interest, polling::PollMode::Level)
	}

	fn watch(&mut self, fd: RawFd, direction: Direction, key: DefaultKey) -> io::Result<()> {
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
			// is ready, and a socket the caller left in blocking mode would
			// stall the reactor in that call. MSG_DONTWAIT does not cover it:
			// XNU's sosendcheck only honours SS_NBIO.
			// SAFETY: the descriptor stays open while the reactor holds it.
			rustix::io::ioctl_fionbio(unsafe { BorrowedFd::borrow_raw(fd) }, true)?;
			let mut entry = Source {
				fd,
				readers: SmallVec::new(),
				writers: SmallVec::new(),
			};
			entry.add(direction, key);
			let source_key = self.sources.insert(entry);
			let interest = self.sources[source_key].interest(source_key);
			// SAFETY: the descriptor stays open while the reactor holds it, and
			// every source is `delete`d from the poller before the reactor goes.
			let source = unsafe { BorrowedFd::borrow_raw(fd) };
			if let Err(error) = unsafe {
				self.poller
					.add_with_mode(&source, interest, polling::PollMode::Level)
			} {
				// The poller never took the source, so nothing may name it.
				self.sources.remove(source_key);
				return Err(error);
			}
			self.keys.insert(fd, source_key);
			Ok(())
		}
	}

	fn unwatch(&mut self, fd: RawFd, direction: Direction, key: DefaultKey) -> io::Result<()> {
		let Some(&source_key) = self.keys.get(&fd) else {
			return Ok(());
		};
		self.sources[source_key].remove(direction, key);
		self.rearm(fd)
	}

	fn rearm(&mut self, fd: RawFd) -> io::Result<()> {
		let Some(&key) = self.keys.get(&fd) else {
			return Ok(());
		};
		if !self.sources[key].idle() {
			return self.arm(fd, key);
		}
		self.keys.remove(&fd);
		self.sources.remove(key);
		self.poller
			.delete(unsafe { BorrowedFd::borrow_raw(fd) })
			.inspect_err(
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
				run_op(fd, &mut waiting.op, &mut waiting.buffers),
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
		let waiting = self
			.waiting
			.remove(key)
			.expect("the operation was just run");
		release(&mut self.iovecs, waiting.buffers);
		self.done.push((id, landed));
		Ok(())
	}

	fn submit_pool(
		&mut self,
		id: u64,
		fd: RawFd,
		fdoff: u64,
		kind: Kind,
		buffers: Vec<IoVec>,
	) -> io::Result<()> {
		if matches!(kind, Kind::Read | Kind::Write) && buffers.is_empty() {
			self.done.push((id, Ok(Response::Count(0))));
			return Ok(());
		}
		let task = unblock(move || (id, run_file(fd, fdoff, kind, buffers)));
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
			let _ = self
				.poller
				.delete(unsafe { BorrowedFd::borrow_raw(source.fd) });
		}
		// Workers cannot be interrupted; wait until no job can access the caller's resources.
		while !self.pool.is_empty() {
			self.collect_pool();
			std::thread::sleep(Duration::from_millis(1));
		}
	}
}

impl Portable {
	pub(super) fn submit(
		&mut self,
		id: u64,
		fd: BorrowedDescriptor<'_>,
		op: Op,
	) -> Result<(), Error> {
		let fd = fd.as_raw_fd();
		match op {
			Op::Read { offset, span } if takes_pool(fd) => self
				.submit_pool(id, fd, offset, Kind::Read, resolve(&span)?)
				.map_err(Error::from),
			Op::Write { offset, span } if takes_pool(fd) => self
				.submit_pool(id, fd, offset, Kind::Write, resolve(&span)?)
				.map_err(Error::from),
			Op::Fsync if takes_pool(fd) => self
				.submit_pool(id, fd, 0, Kind::Fsync, Vec::new())
				.map_err(Error::from),
			Op::Flush if takes_pool(fd) => self
				.submit_pool(id, fd, 0, Kind::Flush, Vec::new())
				.map_err(Error::from),
			Op::Allocate { offset, len } => self
				.submit_pool(id, fd, 0, Kind::Allocate { offset, len }, Vec::new())
				.map_err(Error::from),
			Op::Punch { offset, len } => self
				.submit_pool(id, fd, 0, Kind::Punch { offset, len }, Vec::new())
				.map_err(Error::from),
			// A descriptor that is no regular file cannot be synced or flushed
			// by the pool: the call is made here.
			Op::Fsync => {
				let result = rustix::fs::fsync(unsafe { BorrowedFd::borrow_raw(fd) })
					.map(|()| Response::Done)
					.map_err(io::Error::from);
				self.done.push((id, result.map_err(Error::from)));
				Ok(())
			}
			Op::Flush => {
				let result = flush_handle(unsafe { BorrowedFd::borrow_raw(fd) })
					.map(|()| Response::Done)
					.map_err(Error::from);
				self.done.push((id, result));
				Ok(())
			}
			// `polling`'s interrupt interest is a no-op on kqueue, and its
			// events carry no `EV_EOF`, so a peer gone cannot be told from one
			// that half-closed.
			Op::Hangup => Err(Error::from(io::Error::new(
				io::ErrorKind::Unsupported,
				"this backend cannot wait for a hangup",
			))),
			// A socket call and a wait for readiness differ only in what the
			// operation says to do once the poller reports the descriptor.
			other => {
				let Some(direction) = direction(&other) else {
					return Err(Error::from(io::Error::new(
						io::ErrorKind::Unsupported,
						"this operation is not implemented on this backend",
					)));
				};
				// Resolved before the operation is queued, so an extent out of
				// bounds refuses the submission.
				let buffers = match other.span() {
					Some(span) => resolve_into(&mut self.iovecs, span)?,
					None => Vec::new(),
				};
				let key = self.waiting.insert(Waiting {
					id,
					fd,
					op: other,
					buffers,
				});
				if let Err(error) = self.watch(fd, direction, key) {
					// The poller never took the operation, so it never was: the key
					// goes back out with it.
					let waiting = self
						.waiting
						.remove(key)
						.expect("the operation was just filed");
					release(&mut self.iovecs, waiting.buffers);
					return Err(Error::from(error));
				}
				Ok(())
			}
		}
	}

	pub(super) fn cancel(&mut self, id: u64) {
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
		release(&mut self.iovecs, waiting.buffers);
		self.done.push((id, Err(Error::cancelled())));
	}

	pub(super) fn poll(
		&mut self,
		timeout: Option<Duration>,
		mut complete: impl FnMut(u64, Result<Response, Error>),
	) -> Result<(), Error> {
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

	pub(super) fn waker(&self) -> Waker {
		self.waker.clone()
	}
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

/// The iovec array a span resolves to before its operation is queued, so one of
/// its extents out of bounds refuses the submission.
fn resolve(span: &Span) -> io::Result<Vec<IoVec>> {
	let mut buffers = Vec::new();
	span.resolve_into(&mut buffers)?;
	Ok(buffers)
}

/// An iovec array to fill: one no waiting operation reads any more, or a new
/// one.
fn lease(iovecs: &mut Vec<Vec<IoVec>>) -> Vec<IoVec> {
	iovecs.pop().unwrap_or_default()
}

/// Takes an array back once nothing reads it, keeping the allocation for the
/// next operation.
fn release(iovecs: &mut Vec<Vec<IoVec>>, mut buffers: Vec<IoVec>) {
	buffers.clear();
	iovecs.push(buffers);
}

/// Resolves `span` into an array from the pool, so an extent out of bounds
/// refuses the submission before the operation is queued.
fn resolve_into(iovecs: &mut Vec<Vec<IoVec>>, span: &Span) -> io::Result<Vec<IoVec>> {
	let mut buffers = lease(iovecs);
	match span.resolve_into(&mut buffers) {
		Ok(()) => Ok(buffers),
		Err(error) => {
			release(iovecs, buffers);
			Err(error)
		}
	}
}

fn takes_pool(fd: RawFd) -> bool {
	let Ok(stat) = rustix::fs::fstat(unsafe { BorrowedFd::borrow_raw(fd) }) else {
		return false;
	};
	rustix::fs::FileType::from_raw_mode(stat.st_mode as _) == rustix::fs::FileType::RegularFile
}

fn run_file(fd: RawFd, fdoff: u64, kind: Kind, buffers: Vec<IoVec>) -> io::Result<Response> {
	let fd = unsafe { BorrowedFd::borrow_raw(fd) };
	match kind {
		Kind::Fsync => rustix::fs::fsync(fd)
			.map(|()| Response::Done)
			.map_err(io::Error::from),
		Kind::Flush => flush_handle(fd).map(|()| Response::Done),
		Kind::Allocate { offset, len } => allocate(fd, offset, len).map(|()| Response::Done),
		Kind::Punch { offset, len } => punch(fd, offset, len).map(|()| Response::Done),
		Kind::Read | Kind::Write => {
			let Some(span) = buffers.first() else {
				return Ok(Response::Count(0));
			};
			// SAFETY: the reactor holds the memory backing this iovec until the job lands; the syscall copies through its pointer without forming a Rust reference.
			let offset = fdoff as libc::off_t;
			let len = span.len().min(MAX_TRANSFER);
			count(unsafe {
				match kind {
					Kind::Read => libc::pread(fd.as_raw_fd(), span.as_ptr().cast(), len, offset),
					Kind::Write => libc::pwrite(fd.as_raw_fd(), span.as_ptr().cast(), len, offset),
					_ => unreachable!("handled above"),
				}
			})
			.map(Response::Count)
		}
	}
}

/// Grows the file to the range's end, so the reserved space is held against
/// it; a file already ending past the range keeps its length.
fn allocate(fd: BorrowedFd<'_>, offset: u64, len: u64) -> io::Result<()> {
	let end = offset
		.checked_add(len)
		.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
	let before = rustix::fs::fstat(fd)?.st_size.max(0) as u64;
	if end > before {
		reserve(fd, offset, len, end)?;
		rustix::fs::ftruncate(fd, end)?;
	}
	Ok(())
}

#[cfg(target_vendor = "apple")]
fn reserve(fd: BorrowedFd<'_>, _offset: u64, _len: u64, end: u64) -> io::Result<()> {
	// F_PREALLOCATE, asked for a contiguous run first and a scattered one after,
	// as rustix's own fallocate does here.
	let mut store = libc::fstore_t {
		fst_flags: libc::F_ALLOCATECONTIG,
		fst_posmode: libc::F_PEOFPOSMODE,
		fst_offset: 0,
		fst_length: end as libc::off_t,
		fst_bytesalloc: 0,
	};
	if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_PREALLOCATE, &store) } != -1 {
		return Ok(());
	}
	let error = io::Error::last_os_error();
	if error.raw_os_error() != Some(libc::ENOTSUP) {
		return Err(error);
	}
	store.fst_flags = libc::F_ALLOCATEALL;
	if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_PREALLOCATE, &store) } == -1 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

#[cfg(not(target_vendor = "apple"))]
fn reserve(fd: BorrowedFd<'_>, offset: u64, len: u64, _end: u64) -> io::Result<()> {
	let status =
		unsafe { libc::posix_fallocate(fd.as_raw_fd(), offset as libc::off_t, len as libc::off_t) };
	if status != 0 {
		return Err(io::Error::from_raw_os_error(status));
	}
	Ok(())
}

fn punch(fd: BorrowedFd<'_>, offset: u64, len: u64) -> io::Result<()> {
	#[cfg(target_vendor = "apple")]
	{
		// `rustix::fs::fallocate` has no hole-punching mode here; F_PUNCHHOLE is
		// the fcntl that does it, and it keeps the length. It takes only a range
		// that is a multiple of the file system block size and answers EINVAL
		// otherwise, so the aligned middle goes to the kernel and the two
		// partial blocks at the ends are written over: a hole the kernel will
		// not take is still the caller's bytes.
		let end = offset
			.checked_add(len)
			.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
		let block = block_size(fd)?;
		let first = offset.next_multiple_of(block).min(end);
		let last = (end - end % block).max(first);
		if first > offset {
			zero(fd, offset, first - offset)?;
		}
		if end > last {
			zero(fd, last, end - last)?;
		}
		if last > first {
			let request = libc::fpunchhole_t {
				fp_flags: 0,
				reserved: 0,
				fp_offset: first as libc::off_t,
				fp_length: (last - first) as libc::off_t,
			};
			if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_PUNCHHOLE, &request) } == -1 {
				return Err(io::Error::last_os_error());
			}
		}
		Ok(())
	}
	#[cfg(not(target_vendor = "apple"))]
	{
		let _ = (offset, len);
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"punch is not implemented on this platform",
		))
	}
}

/// Check `close(dup(fd))` for deferred write-back errors; Rust descriptor drops discard them. Do not retry EINTR: POSIX leaves descriptor state unspecified, and Linux may already have reused the number.
fn flush_handle(fd: BorrowedFd<'_>) -> io::Result<()> {
	let dup = unsafe { libc::dup(fd.as_raw_fd()) };
	if dup < 0 {
		return Err(io::Error::last_os_error());
	}
	if unsafe { libc::close(dup) } < 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

/// The granularity `F_PUNCHHOLE` accepts: the file system's own block size, at
/// least the page size a hole is checked against.
#[cfg(target_vendor = "apple")]
fn block_size(fd: BorrowedFd<'_>) -> io::Result<u64> {
	let stat = rustix::fs::fstat(fd)?;
	let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
	let page = if page > 0 { page as u64 } else { 4096 };
	Ok((stat.st_blksize.max(0) as u64).max(page))
}

/// Writes zeros over the range, for the ends a hole cannot take.
#[cfg(target_vendor = "apple")]
fn zero(fd: BorrowedFd<'_>, offset: u64, len: u64) -> io::Result<()> {
	const CHUNK: [u8; 4096] = [0; 4096];
	let mut done = 0u64;
	while done < len {
		let take = CHUNK.len().min((len - done) as usize);
		// SAFETY: the buffer outlives the call, which only reads from it.
		let moved = unsafe {
			libc::pwrite(
				fd.as_raw_fd(),
				CHUNK.as_ptr().cast(),
				take,
				(offset + done) as libc::off_t,
			)
		};
		let moved = count(moved)?;
		if moved == 0 {
			return Err(io::Error::from(io::ErrorKind::WriteZero));
		}
		done += moved as u64;
	}
	Ok(())
}

fn count(result: libc::ssize_t) -> io::Result<usize> {
	if result < 0 {
		Err(io::Error::last_os_error())
	} else {
		Ok(result as usize)
	}
}

enum Call {
	Bytes(usize),
	Blocked,
}

fn call(result: libc::ssize_t) -> io::Result<Call> {
	match count(result) {
		Ok(bytes) => Ok(Call::Bytes(bytes)),
		Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(Call::Blocked),
		Err(error) => Err(error),
	}
}

// The socket calls pass MSG_DONTWAIT: the caller's socket may be in blocking
// mode, and a call that blocked would stall the reactor thread behind it.
fn run_op(fd: RawFd, op: &mut Op, buffers: &mut Vec<IoVec>) -> io::Result<Progress> {
	// SAFETY: the descriptor stays open for the operation's flight.
	let fd = unsafe { BorrowedFd::borrow_raw(fd) };
	match op {
		Op::Read { .. } => {
			let Some((ptr, len)) = first_buffer(buffers) else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// SAFETY: the reactor holds the memory backing this pointer until the operation lands.
			match call(unsafe { libc::read(fd.as_raw_fd(), ptr.cast(), len) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Op::Write { .. } => {
			let Some((ptr, len)) = first_buffer(buffers) else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// SAFETY: the reactor holds the memory backing this pointer until the operation lands.
			match call(unsafe { libc::write(fd.as_raw_fd(), ptr.cast(), len) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Op::Accept => match rustix::net::accept(fd) {
			Ok(socket) => {
				// Set flags after accept because `accept4` is not available on every Unix.
				rustix::io::fcntl_setfd(&socket, rustix::io::FdFlags::CLOEXEC)?;
				rustix::io::ioctl_fionbio(&socket, true)?;
				Ok(Progress::Done(Response::Accepted(socket)))
			}
			Err(rustix::io::Errno::AGAIN) => Ok(Progress::Blocked),
			Err(error) => Err(error.into()),
		},
		Op::Recv { .. } => {
			let (ptr, len) = one_buffer(buffers)?;
			match call(unsafe { libc::recv(fd.as_raw_fd(), ptr.cast(), len, libc::MSG_DONTWAIT) })?
			{
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Op::RecvWithFds { .. } => {
			let (ptr, len) = one_buffer(buffers)?;
			// The received descriptors are this call's own: nothing waits on
			// them after the response carries them out.
			let mut fds: Vec<OwnedDescriptor> = Vec::new();
			let mut iov = libc::iovec {
				iov_base: ptr.cast(),
				iov_len: len,
			};
			let mut space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
			let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
			msg.msg_iov = &mut iov;
			msg.msg_iovlen = 1;
			msg.msg_control = space.as_mut_ptr().cast();
			msg.msg_controllen = space.len() as _;
			match call(unsafe { libc::recvmsg(fd.as_raw_fd(), &mut msg, libc::MSG_DONTWAIT) })? {
				Call::Bytes(count) => {
					take_fds(&msg, &mut fds);
					Ok(Progress::Done(Response::CountWithFds(count, fds)))
				}
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Op::Send { fds, .. } => send_socket(fd, buffers, fds),
		// A wait for readiness is answered by the poller itself.
		Op::Readable | Op::Writable => return Ok(Progress::Done(Response::Done)),
		// Everything else runs on the pool.
		_ => unreachable!("only a socket call and a readiness wait reach the poller"),
	}
}

/// Sends `buffers` on `fd`, carrying `fds` when there are any. The descriptors
/// are this operation's own: the peer holds its own copies once the call takes
/// the bytes, so they go with the call.
fn send_socket(
	fd: BorrowedFd<'_>,
	buffers: &[IoVec],
	fds: &mut Vec<OwnedDescriptor>,
) -> io::Result<Progress> {
	if fds.len() > MAX_FDS {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"too many descriptors",
		));
	}
	let (ptr, len) = one_buffer(buffers)?;
	let carried: Vec<libc::c_int> = fds.iter().map(|fd| fd.as_raw_fd()).collect();
	let result = if carried.is_empty() {
		call(unsafe { libc::send(fd.as_raw_fd(), ptr.cast(), len, libc::MSG_DONTWAIT) })
	} else {
		let mut iov = libc::iovec {
			iov_base: ptr.cast(),
			iov_len: len,
		};
		let mut space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
		let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
		msg.msg_iov = &mut iov;
		msg.msg_iovlen = 1;
		msg.msg_control = space.as_mut_ptr().cast();
		msg.msg_controllen =
			unsafe { libc::CMSG_LEN((carried.len() * size_of::<libc::c_int>()) as u32) } as _;
		unsafe {
			let header = space.as_mut_ptr().cast::<libc::cmsghdr>();
			(*header).cmsg_level = libc::SOL_SOCKET;
			(*header).cmsg_type = libc::SCM_RIGHTS;
			(*header).cmsg_len =
				libc::CMSG_LEN((carried.len() * size_of::<libc::c_int>()) as u32) as _;
			let data = libc::CMSG_DATA(header).cast::<libc::c_int>();
			for (index, raw) in carried.iter().enumerate() {
				*data.add(index) = *raw;
			}
		}
		call(unsafe { libc::sendmsg(fd.as_raw_fd(), &msg, libc::MSG_DONTWAIT) })
	};
	match result? {
		Call::Bytes(count) => {
			fds.clear();
			Ok(Progress::Done(Response::Count(count)))
		}
		Call::Blocked => Ok(Progress::Blocked),
	}
}

fn one_buffer(buffers: &[IoVec]) -> io::Result<(*mut u8, usize)> {
	let [buf] = buffers else {
		return Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a socket transfer needs exactly one buffer",
		));
	};
	Ok((buf.as_ptr(), buf.len().min(MAX_TRANSFER)))
}

fn first_buffer(buffers: &[IoVec]) -> Option<(*mut u8, usize)> {
	buffers
		.first()
		.map(|buf| (buf.as_ptr(), buf.len().min(MAX_TRANSFER)))
}

fn take_fds(msg: &libc::msghdr, out: &mut Vec<OwnedDescriptor>) {
	let mut header = unsafe { libc::CMSG_FIRSTHDR(msg) };
	while !header.is_null() {
		let (level, kind) = unsafe { ((*header).cmsg_level, (*header).cmsg_type) };
		if level == libc::SOL_SOCKET && kind == libc::SCM_RIGHTS {
			let count = ((unsafe { (*header).cmsg_len } as usize)
				- (unsafe { libc::CMSG_LEN(0) }) as usize)
				/ size_of::<libc::c_int>();
			let data = unsafe { libc::CMSG_DATA(header) }.cast::<libc::c_int>();
			for index in 0..count {
				let raw = unsafe { *data.add(index) };
				// SAFETY: the kernel put this descriptor in the process, and
				// nothing else in it owns the number.
				let descriptor = unsafe { OwnedDescriptor::from_raw_fd(raw) };
				let _ = rustix::io::fcntl_setfd(&descriptor, rustix::io::FdFlags::CLOEXEC);
				out.push(descriptor);
			}
		}
		header = unsafe { libc::CMSG_NXTHDR(msg, header) };
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use crate::{AsDescriptor, Config};

	/// Closed-descriptor deregistration differs by poller: epoll may reject it, while kqueue may have removed it already. Either outcome must discard the registration.
	#[test]
	fn a_deregistration_of_a_closed_descriptor_leaves_no_key_behind() {
		let mut backend = Portable::new(&Config::default()).unwrap();
		let (reader, _writer) = std::io::pipe().unwrap();
		let fd = reader.as_descriptor().as_raw_fd();
		let key = backend.waiting.insert(Waiting {
			id: 7,
			fd,
			op: Op::Readable,
			buffers: Vec::new(),
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
		let fd = reader.as_descriptor().as_raw_fd();
		let key = backend.waiting.insert(Waiting {
			id: 7,
			fd,
			op: Op::Readable,
			buffers: Vec::new(),
		});
		backend.watch(fd, Direction::Readable, key).unwrap();
		let registered = backend.keys[&fd];
		backend
			.poller
			.delete(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
			.unwrap();
		assert!(backend.unwatch(fd, Direction::Readable, key).is_err());
		assert!(!backend.keys.contains_key(&fd));
		assert!(!backend.sources.contains_key(registered));
		assert!(backend.sources.is_empty());
	}
}
