//! Readiness polling for sockets and a thread pool for regular files.
//!
//! The reactor files each operation under an id, and this backend carries that
//! id as a poller key and as a pool job's name: a wait the poller answers and a
//! job the pool finishes both report the same id back.

use std::collections::{HashMap, HashSet};
use std::io;
use std::mem::{MaybeUninit, size_of};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd};
use std::sync::Arc;
use std::task::{Context, Poll as TaskPoll, Wake, Waker};
use std::time::Duration;

use blocking::{Task, unblock};
use polling::{Event as Ready, Events, Poller};

use super::Op;
use crate::OwnedDescriptor;
use crate::desc::RawHandle;
use crate::error::Error;
use crate::memory::IoVecs;

use super::{Config, Mechanism, Response};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Direction {
	Readable,
	Writable,
}

const EVENTS: usize = 64;

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
	readers: HashMap<RawHandle, HashSet<u64>>,
	writers: HashMap<RawHandle, HashSet<u64>>,
	keys: HashMap<RawHandle, usize>,
	sources: HashMap<usize, RawHandle>,
	next_key: usize,
	/// The operations waiting on the poller, with the descriptor and the operation
	/// to run when the poller reports that descriptor ready, named by the id the
	/// poller carries as a key.
	waiting: HashMap<u64, (RawHandle, Op<IoVecs>)>,
	/// The jobs running on the pool, named by the id the pool reports a job by.
	/// A worker cannot be interrupted, so a job stays here until it lands; the
	/// reactor's entry may already be gone.
	pool: HashMap<u64, Task<(u64, io::Result<Response>)>>,
	/// Completions that landed outside `poll`'s wait — a submission answered here
	/// and now, a cancel, or a job the pool finished — kept until the next `poll`
	/// hands them over.
	done: Vec<(u64, Result<Response, Error>)>,
	waker: Waker,
	events: Events,
	scratch: Vec<Ready>,
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
			readers: HashMap::new(),
			writers: HashMap::new(),
			keys: HashMap::new(),
			sources: HashMap::new(),
			next_key: 0,
			waiting: HashMap::new(),
			pool: HashMap::new(),
			done: Vec::new(),
			waker: Waker::from(Arc::new(PollerWake(poller))),
			events: Events::with_capacity(NonZeroUsize::new(EVENTS).expect("a nonzero capacity")),
			scratch: Vec::with_capacity(EVENTS),
		})
	}

	fn arm(&mut self, fd: RawHandle) -> io::Result<()> {
		if let Some(&key) = self.keys.get(&fd) {
			// SAFETY: the descriptor stays open while the reactor holds it.
			let source = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
			// Level triggering reports a still-ready source again after rearming.
			self.poller
				.modify_with_mode(&source, self.interest(fd, key), polling::PollMode::Level)
		} else {
			// A readiness backend calls the operation when the poller says it
			// is ready, and a socket the caller left in blocking mode would
			// stall the reactor in that call. MSG_DONTWAIT does not cover it:
			// XNU's sosendcheck only honours SS_NBIO.
			// SAFETY: the descriptor stays open while the reactor holds it.
			rustix::io::ioctl_fionbio(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) }, true)?;
			let key = self.next_key;
			self.next_key += 1;
			// SAFETY: the descriptor stays open while the reactor holds it, and
			// every source is `delete`d from the poller before the reactor goes.
			let source = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
			unsafe {
				self.poller
					.add_with_mode(&source, self.interest(fd, key), polling::PollMode::Level)
			}?;
			self.keys.insert(fd, key);
			self.sources.insert(key, fd);
			Ok(())
		}
	}

	/// The poller interest for `fd`, whose sources the poller names by `key`.
	fn interest(&self, fd: RawHandle, key: usize) -> Ready {
		match (
			self.readers.contains_key(&fd),
			self.writers.contains_key(&fd),
		) {
			(true, true) => Ready::all(key),
			(true, false) => Ready::readable(key),
			(false, true) => Ready::writable(key),
			(false, false) => Ready::none(key),
		}
	}

	fn watch(&mut self, fd: RawHandle, direction: Direction, token: u64) -> io::Result<()> {
		match direction {
			Direction::Readable => {
				self.readers.entry(fd).or_default().insert(token);
			}
			Direction::Writable => {
				self.writers.entry(fd).or_default().insert(token);
			}
		}
		match self.arm(fd) {
			Ok(()) => Ok(()),
			Err(error) => {
				// Nothing is registered for this operation, and the poller must not
				// report the descriptor as waiting for it: the token goes back out.
				let _ = self.unwatch(fd, direction, token);
				Err(error)
			}
		}
	}

	fn unwatch(&mut self, fd: RawHandle, direction: Direction, token: u64) -> io::Result<()> {
		match direction {
			Direction::Readable => {
				if let Some(tokens) = self.readers.get_mut(&fd) {
					tokens.remove(&token);
					if tokens.is_empty() {
						self.readers.remove(&fd);
					}
				}
			}
			Direction::Writable => {
				if let Some(tokens) = self.writers.get_mut(&fd) {
					tokens.remove(&token);
					if tokens.is_empty() {
						self.writers.remove(&fd);
					}
				}
			}
		}
		self.rearm(fd)
	}

	fn rearm(&mut self, fd: RawHandle) -> io::Result<()> {
		let waited = self.readers.contains_key(&fd) || self.writers.contains_key(&fd);
		if waited {
			self.arm(fd)
		} else if let Some(key) = self.keys.remove(&fd) {
			self.sources.remove(&key);
			self.poller
				.delete(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
				.inspect_err(
					|error| log::warn!(fd:? = fd, error:% = error; "the poller refused a deregistration"),
				)
		} else {
			Ok(())
		}
	}

	fn deliver(&mut self, event: &Ready) -> io::Result<()> {
		let Some(fd) = self.sources.get(&event.key).copied() else {
			return Ok(());
		};
		let mut ids: Vec<u64> = Vec::new();
		if event.readable
			&& let Some(waiters) = self.readers.get(&fd)
		{
			ids.extend(waiters.iter().copied());
		}
		if event.writable
			&& let Some(waiters) = self.writers.get(&fd)
		{
			ids.extend(waiters.iter().copied());
		}
		for id in ids {
			self.run_ready(id)?;
		}
		Ok(())
	}

	/// Calls the operation the poller reported ready. One still blocked stays
	/// registered, and a level-triggered poller reports it again.
	fn run_ready(&mut self, id: u64) -> io::Result<()> {
		let (fd, direction, outcome) = {
			let Some((fd, op)) = self.waiting.get_mut(&id) else {
				// Gone, or a second report the first pass has already answered.
				return Ok(());
			};
			let fd = *fd;
			let Some(direction) = direction(op) else {
				return Ok(());
			};
			(fd, direction, run_op(fd, op))
		};
		match outcome {
			// The call would block after all: the registration stays, and the
			// poller reports the descriptor again.
			Ok(Progress::Blocked) => Ok(()),
			Ok(Progress::Done(response)) => {
				self.unwatch(fd, direction, id)?;
				self.waiting.remove(&id);
				self.done.push((id, Ok(response)));
				Ok(())
			}
			Err(error) => {
				self.unwatch(fd, direction, id)?;
				self.waiting.remove(&id);
				self.done.push((id, Err(Error::from(error))));
				Ok(())
			}
		}
	}

	fn submit_pool(
		&mut self,
		id: u64,
		fd: RawHandle,
		fdoff: u64,
		kind: Kind,
		buffers: IoVecs,
	) -> io::Result<()> {
		if matches!(kind, Kind::Read | Kind::Write) && buffers.is_empty() {
			self.done.push((id, Ok(Response::Count(0))));
			return Ok(());
		}
		let task = unblock(move || (id, run_file(fd, fdoff, kind, buffers)));
		self.pool.insert(id, task);
		Ok(())
	}

	fn collect_pool(&mut self) {
		if self.pool.is_empty() {
			return;
		}
		let mut landed = Vec::new();
		let waker = &self.waker;
		for task in self.pool.values_mut() {
			let mut context = Context::from_waker(waker);
			if let TaskPoll::Ready((id, result)) = std::pin::Pin::new(task).poll(&mut context) {
				landed.push((id, result));
			}
		}
		for (id, result) in landed {
			self.pool.remove(&id);
			self.done.push((id, result.map_err(Error::from)));
		}
	}
}

impl Drop for Portable {
	fn drop(&mut self) {
		// The poller requires deregistration before descriptors are closed.
		for (_, fd) in std::mem::take(&mut self.sources) {
			let _ = self
				.poller
				.delete(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) });
		}
		// Workers cannot be interrupted; wait until no job can access the caller's resources.
		while !self.pool.is_empty() {
			self.collect_pool();
			std::thread::sleep(Duration::from_millis(1));
		}
	}
}

impl Mechanism for Portable {
	fn submit(&mut self, id: u64, fd: RawHandle, op: Op<IoVecs>) -> Result<(), Error> {
		match op {
			Op::Read { offset, span } if takes_pool(fd) => self
				.submit_pool(id, fd, offset, Kind::Read, span)
				.map_err(Error::from),
			Op::Write { offset, span } if takes_pool(fd) => self
				.submit_pool(id, fd, offset, Kind::Write, span)
				.map_err(Error::from),
			Op::Fsync if takes_pool(fd) => self
				.submit_pool(id, fd, 0, Kind::Fsync, IoVecs::new())
				.map_err(Error::from),
			Op::Flush if takes_pool(fd) => self
				.submit_pool(id, fd, 0, Kind::Flush, IoVecs::new())
				.map_err(Error::from),
			Op::Allocate { offset, len } => self
				.submit_pool(id, fd, 0, Kind::Allocate { offset, len }, IoVecs::new())
				.map_err(Error::from),
			Op::Punch { offset, len } => self
				.submit_pool(id, fd, 0, Kind::Punch { offset, len }, IoVecs::new())
				.map_err(Error::from),
			// A descriptor that is no regular file cannot be synced or flushed
			// by the pool: the call is made here.
			Op::Fsync => {
				let result = rustix::fs::fsync(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
					.map(|()| Response::Done)
					.map_err(io::Error::from);
				self.done.push((id, result.map_err(Error::from)));
				Ok(())
			}
			Op::Flush => {
				let result = flush_handle(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
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
				self.waiting.insert(id, (fd, other));
				if let Err(error) = self.watch(fd, direction, id) {
					// The poller never took the operation, so it never was: the token
					// goes back out with it.
					self.waiting.remove(&id);
					return Err(Error::from(error));
				}
				Ok(())
			}
		}
	}

	fn cancel(&mut self, id: u64) {
		if self.pool.contains_key(&id) {
			// A worker already inside a syscall must finish before its buffers can be released.
			return;
		}
		let Some((fd, op)) = self.waiting.remove(&id) else {
			return;
		};
		if let Some(direction) = direction(&op) {
			let _ = self.unwatch(fd, direction, id);
		}
		self.done.push((id, Err(Error::cancelled())));
	}

	fn poll(
		&mut self,
		timeout: Option<Duration>,
		completed: &mut Vec<(u64, Result<Response, Error>)>,
	) -> Result<(), Error> {
		// Collect finished jobs before sleeping so their completions do not wait
		// for the timeout.
		self.collect_pool();
		// A result can land outside this call — a submission with nothing to
		// wait for, a cancel answered here and now, or a job the pool finished
		// before this poll — and the reactor has not settled it: sleeping now
		// would wait out a timeout on news already in hand.
		if !self.done.is_empty() {
			completed.append(&mut self.done);
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
		completed.append(&mut self.done);
		Ok(())
	}

	fn waker(&self) -> Waker {
		self.waker.clone()
	}
}

/// The direction an operation waits on for the poller to report it ready;
/// `None` for one that never waits.
fn direction(op: &Op<IoVecs>) -> Option<Direction> {
	match op {
		Op::Read { .. } | Op::Recv { .. } | Op::RecvWithFds { .. } | Op::Accept | Op::Readable => {
			Some(Direction::Readable)
		}
		Op::Write { .. } | Op::Send { .. } | Op::Writable => Some(Direction::Writable),
		_ => None,
	}
}

fn takes_pool(fd: RawHandle) -> bool {
	let Ok(stat) = rustix::fs::fstat(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) }) else {
		return false;
	};
	rustix::fs::FileType::from_raw_mode(stat.st_mode as _) == rustix::fs::FileType::RegularFile
}

fn run_file(fd: RawHandle, fdoff: u64, kind: Kind, buffers: IoVecs) -> io::Result<Response> {
	let fd = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
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
			count(unsafe {
				match kind {
					Kind::Read => {
						libc::pread(fd.as_raw_fd(), span.as_ptr().cast(), span.len(), offset)
					}
					Kind::Write => {
						libc::pwrite(fd.as_raw_fd(), span.as_ptr().cast(), span.len(), offset)
					}
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
fn run_op(fd: RawHandle, op: &mut Op<IoVecs>) -> io::Result<Progress> {
	// SAFETY: the descriptor stays open for the operation's flight.
	let fd = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
	match op {
		Op::Read { span: buffers, .. } => {
			let Some((ptr, len)) = first_buffer(buffers) else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// SAFETY: the reactor holds the memory backing this pointer until the operation lands.
			match call(unsafe { libc::read(fd.as_raw_fd(), ptr.cast(), len) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Op::Write { span: buffers, .. } => {
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
		Op::Recv { span: buffers, .. } => {
			let (ptr, len) = one_buffer(buffers)?;
			match call(unsafe { libc::recv(fd.as_raw_fd(), ptr.cast(), len, libc::MSG_DONTWAIT) })?
			{
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Op::RecvWithFds { span: buffers, .. } => {
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
		Op::Send { span: buffers, fds } => send_socket(fd, buffers, fds),
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
	buffers: &mut IoVecs,
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

fn one_buffer(buffers: &IoVecs) -> io::Result<(*mut u8, usize)> {
	let [buf] = buffers.as_slice() else {
		return Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a socket transfer needs exactly one buffer",
		));
	};
	Ok((buf.as_ptr(), buf.len()))
}

fn first_buffer(buffers: &IoVecs) -> Option<(*mut u8, usize)> {
	buffers.first().map(|buf| (buf.as_ptr(), buf.len()))
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
		let token = 7;
		let fd = RawHandle::from_descriptor(reader.as_descriptor());
		backend.watch(fd, Direction::Readable, token).unwrap();
		let key = backend.keys[&fd];
		drop(reader);
		let _ = backend.unwatch(fd, Direction::Readable, token);
		assert!(!backend.keys.contains_key(&fd));
		assert!(!backend.sources.contains_key(&key));
		assert!(
			backend
				.readers
				.get(&fd)
				.is_none_or(|tokens| !tokens.contains(&token))
		);
	}

	/// Simulate a poller that lost the source before deregistration; stale readiness must not retain or dispatch the old key.
	#[test]
	fn a_refused_deregistration_leaves_no_key_behind() {
		let mut backend = Portable::new(&Config::default()).unwrap();
		let (reader, _writer) = std::io::pipe().unwrap();
		let token = 7;
		let fd = RawHandle::from_descriptor(reader.as_descriptor());
		backend.watch(fd, Direction::Readable, token).unwrap();
		let key = backend.keys[&fd];
		backend
			.poller
			.delete(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
			.unwrap();
		assert!(backend.unwatch(fd, Direction::Readable, token).is_err());
		assert!(!backend.keys.contains_key(&fd));
		assert!(!backend.sources.contains_key(&key));
		assert!(
			backend
				.readers
				.get(&fd)
				.is_none_or(|tokens| !tokens.contains(&token))
		);
	}
}
