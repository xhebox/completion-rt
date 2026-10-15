//! Readiness polling for sockets and a thread pool for regular files.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::mem::{MaybeUninit, size_of};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, IntoRawFd};
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::task::{Context, Poll as TaskPoll, Wake, Waker};
use std::time::Duration;

use blocking::{Task, unblock};
use polling::{Event as Ready, Events, Poller};

use super::park::{Doorbell, Park};
use crate::OwnedDescriptor;
use crate::desc::{RawHandle, from_raw_socket};
use crate::error::Error;
use crate::memory::IoVecs;

use super::{Backend, Response};

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

struct Job {
	task: Task<(u64, io::Result<Response>)>,
}

enum Work {
	Read { buffers: IoVecs },
	Write { buffers: IoVecs },
	Recv { buffers: IoVecs },
	RecvWithFds { buffers: IoVecs },
	Send { buffers: IoVecs },
	Accept,
}

struct Op {
	fd: RawHandle,
	direction: Direction,
	work: Work,
	fds: Vec<OwnedDescriptor>,
}

struct Poll {
	fd: RawHandle,
	direction: Direction,
}

/// A pool job's wake rings the same doorbell the reactor's `Park` holds, the
/// poller. It does not go through `Park::unpark`: a finished job only needs the
/// reactor to reap it, not the executor to tick. A notify that a zero-timeout
/// wait takes instead loses nothing, since every poll collects the pool after
/// its wait.
struct PoolWake(Arc<Poller>);

impl Wake for PoolWake {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		let _ = self.0.notify();
	}
}

impl Doorbell for Poller {
	fn ring(&self, _word: &AtomicU32) {
		if let Err(error) = self.notify() {
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
	armed: HashSet<usize>,
	next_key: usize,
	ops: HashMap<u64, Op>,
	polls: HashMap<u64, Poll>,
	pool: HashMap<u64, Job>,
	waker: Waker,
	events: Events,
	scratch: Vec<Ready>,
	done: VecDeque<(u64, Result<Response, Error>)>,
}

impl Portable {
	pub(super) fn new(config: &super::Config) -> io::Result<Portable> {
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
			armed: HashSet::new(),
			next_key: 0,
			ops: HashMap::new(),
			polls: HashMap::new(),
			pool: HashMap::new(),
			waker: Waker::from(Arc::new(PoolWake(poller))),
			events: Events::with_capacity(NonZeroUsize::new(EVENTS).expect("a nonzero capacity")),
			scratch: Vec::with_capacity(EVENTS),
			done: VecDeque::new(),
		})
	}

	fn land(&mut self, out: Vec<(u64, io::Result<Response>)>) {
		self.done.extend(
			out.into_iter()
				.map(|(id, result)| (id, result.map_err(Error::from))),
		);
	}

	fn arm(&mut self, fd: RawHandle) -> io::Result<()> {
		let key = match self.keys.get(&fd) {
			Some(key) => *key,
			None => {
				// A readiness backend calls the operation when the poller says it
				// is ready, and a socket the caller left in blocking mode would
				// stall the reactor in that call. MSG_DONTWAIT does not cover it:
				// XNU's sosendcheck only honours SS_NBIO.
				// SAFETY: the descriptor stays open while the reactor holds it.
				rustix::io::ioctl_fionbio(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) }, true)?;
				let key = self.next_key;
				self.next_key += 1;
				self.keys.insert(fd, key);
				self.sources.insert(key, fd);
				key
			}
		};
		let interest = match (
			self.readers.contains_key(&fd),
			self.writers.contains_key(&fd),
		) {
			(true, true) => Ready::all(key),
			(true, false) => Ready::readable(key),
			(false, true) => Ready::writable(key),
			(false, false) => Ready::none(key),
		};
		// SAFETY: the descriptor stays open while the reactor holds it, and
		// every source is `delete`d from the poller before the reactor goes.
		let source = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
		// Level triggering reports a still-ready source again after rearming.
		if self.armed.contains(&key) {
			self.poller
				.modify_with_mode(&source, interest, polling::PollMode::Level)
		} else {
			unsafe {
				self.poller
					.add_with_mode(&source, interest, polling::PollMode::Level)
			}?;
			self.armed.insert(key);
			Ok(())
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
		self.arm(fd)
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
			self.armed.remove(&key);
			self.poller
				.delete(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
				.inspect_err(
					|error| log::warn!(fd:? = fd, error:% = error; "the poller refused a deregistration"),
				)
		} else {
			Ok(())
		}
	}

	fn deliver(
		&mut self,
		event: &Ready,
		out: &mut Vec<(u64, io::Result<Response>)>,
	) -> io::Result<()> {
		let Some(fd) = self.sources.get(&event.key).copied() else {
			return Ok(());
		};
		let mut tokens: Vec<u64> = Vec::new();
		if event.readable
			&& let Some(waiters) = self.readers.get(&fd)
		{
			tokens.extend(waiters.iter().copied());
		}
		if event.writable
			&& let Some(waiters) = self.writers.get(&fd)
		{
			tokens.extend(waiters.iter().copied());
		}
		for token in tokens {
			self.ready(token, out)?;
		}
		Ok(())
	}

	fn ready(&mut self, token: u64, out: &mut Vec<(u64, io::Result<Response>)>) -> io::Result<()> {
		let Some(op) = self.ops.get_mut(&token) else {
			let Some(poll) = self.polls.get(&token) else {
				return Ok(());
			};
			let (fd, direction) = (poll.fd, poll.direction);
			self.polls.remove(&token);
			self.unwatch(fd, direction, token)?;
			out.push((token, Ok(Response::Done)));
			return Ok(());
		};
		let (fd, direction) = (op.fd, op.direction);
		match run_op(op) {
			Ok(Progress::Blocked) => Ok(()),
			Ok(Progress::Done(response)) => {
				self.ops.remove(&token);
				self.unwatch(fd, direction, token)?;
				out.push((token, Ok(response)));
				Ok(())
			}
			Err(error) => {
				self.ops.remove(&token);
				self.unwatch(fd, direction, token)?;
				out.push((token, Err(error)));
				Ok(())
			}
		}
	}

	fn submit_pool(
		&mut self,
		token: u64,
		fd: RawHandle,
		fdoff: u64,
		kind: Kind,
		buffers: IoVecs,
	) -> io::Result<()> {
		if matches!(kind, Kind::Read | Kind::Write) && buffers.is_empty() {
			self.done.push_back((token, Ok(Response::Count(0))));
			return Ok(());
		}
		let task = unblock(move || (token, run_file(fd, fdoff, kind, buffers)));
		self.pool.insert(token, Job { task });
		Ok(())
	}

	fn submit_op(
		&mut self,
		token: u64,
		fd: RawHandle,
		direction: Direction,
		work: Work,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<()> {
		self.ops.insert(
			token,
			Op {
				fd,
				direction,
				work,
				fds,
			},
		);
		match self.watch(fd, direction, token) {
			Ok(()) => Ok(()),
			Err(error) => {
				self.ops.remove(&token);
				Err(error)
			}
		}
	}

	fn collect_pool(&mut self, out: &mut Vec<(u64, io::Result<Response>)>) {
		if self.pool.is_empty() {
			return;
		}
		let mut landed = Vec::new();
		let waker = &self.waker;
		for job in self.pool.values_mut() {
			let mut context = Context::from_waker(waker);
			if let TaskPoll::Ready(landed_job) =
				std::pin::Pin::new(&mut job.task).poll(&mut context)
			{
				landed.push(landed_job);
			}
		}
		for (token, result) in landed {
			self.pool.remove(&token);
			out.push((token, result));
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
		let mut out = Vec::new();
		while !self.pool.is_empty() {
			self.collect_pool(&mut out);
			out.clear();
			std::thread::sleep(Duration::from_millis(1));
		}
	}
}

impl Backend for Portable {
	fn read(&mut self, id: u64, fd: RawHandle, fdoff: u64, buffers: IoVecs) -> io::Result<()> {
		if takes_pool(fd) {
			self.submit_pool(id, fd, fdoff, Kind::Read, buffers)
		} else {
			self.submit_op(
				id,
				fd,
				Direction::Readable,
				Work::Read { buffers },
				Vec::new(),
			)
		}
	}

	fn write(&mut self, id: u64, fd: RawHandle, fdoff: u64, buffers: IoVecs) -> io::Result<()> {
		if takes_pool(fd) {
			self.submit_pool(id, fd, fdoff, Kind::Write, buffers)
		} else {
			self.submit_op(
				id,
				fd,
				Direction::Writable,
				Work::Write { buffers },
				Vec::new(),
			)
		}
	}

	fn fsync(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		if takes_pool(fd) {
			return self.submit_pool(id, fd, 0, Kind::Fsync, IoVecs::new());
		}
		let result = rustix::fs::fsync(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
			.map(|()| Response::Done)
			.map_err(io::Error::from);
		self.done.push_back((id, result.map_err(Error::from)));
		Ok(())
	}

	fn allocate(&mut self, id: u64, fd: RawHandle, offset: u64, len: u64) -> io::Result<()> {
		debug_assert!(takes_pool(fd), "only a file is allocated");
		self.submit_pool(id, fd, 0, Kind::Allocate { offset, len }, IoVecs::new())
	}

	fn punch(&mut self, id: u64, fd: RawHandle, offset: u64, len: u64) -> io::Result<()> {
		debug_assert!(takes_pool(fd), "only a file is punched");
		self.submit_pool(id, fd, 0, Kind::Punch { offset, len }, IoVecs::new())
	}

	fn flush(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		if takes_pool(fd) {
			return self.submit_pool(id, fd, 0, Kind::Flush, IoVecs::new());
		}
		let result = flush_handle(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
			.map(|()| Response::Done);
		self.done.push_back((id, result.map_err(Error::from)));
		Ok(())
	}

	fn recv(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()> {
		self.submit_op(
			id,
			fd,
			Direction::Readable,
			Work::Recv { buffers },
			Vec::new(),
		)
	}

	fn recv_with_fds(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()> {
		self.submit_op(
			id,
			fd,
			Direction::Readable,
			Work::RecvWithFds { buffers },
			Vec::new(),
		)
	}

	fn send(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()> {
		self.submit_op(
			id,
			fd,
			Direction::Writable,
			Work::Send { buffers },
			Vec::new(),
		)
	}

	fn send_with_fds(
		&mut self,
		id: u64,
		fd: RawHandle,
		buffers: IoVecs,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<()> {
		self.submit_op(id, fd, Direction::Writable, Work::Send { buffers }, fds)
	}

	fn accept(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		self.submit_op(id, fd, Direction::Readable, Work::Accept, Vec::new())
	}

	fn readable(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		self.polls.insert(
			id,
			Poll {
				fd,
				direction: Direction::Readable,
			},
		);
		match self.watch(fd, Direction::Readable, id) {
			Ok(()) => Ok(()),
			Err(error) => {
				self.polls.remove(&id);
				Err(error)
			}
		}
	}

	fn writable(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		self.polls.insert(
			id,
			Poll {
				fd,
				direction: Direction::Writable,
			},
		);
		match self.watch(fd, Direction::Writable, id) {
			Ok(()) => Ok(()),
			Err(error) => {
				self.polls.remove(&id);
				Err(error)
			}
		}
	}

	/// `polling`'s interrupt interest is a no-op on kqueue, and its events carry
	/// no `EV_EOF`, so a peer gone cannot be told from one that half-closed.
	fn hangup(&mut self, _id: u64, _fd: RawHandle) -> io::Result<()> {
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"this backend cannot wait for a hangup",
		))
	}

	fn cancel(&mut self, id: u64) {
		if self.pool.contains_key(&id) {
			// A worker already inside a syscall must finish before its buffers can be released.
			return;
		}
		if let Some(op) = self.ops.remove(&id) {
			let _ = self.unwatch(op.fd, op.direction, id);
		} else if let Some(poll) = self.polls.remove(&id) {
			let _ = self.unwatch(poll.fd, poll.direction, id);
		} else {
			return;
		}
		self.done.push_back((id, Err(Error::cancelled())));
	}

	fn poll(&mut self, timeout: Option<Duration>, _park: &Park) -> io::Result<u64> {
		if let Some((id, _)) = self.done.front() {
			return Ok(*id);
		}
		let mut out = Vec::new();
		// Collect finished jobs before sleeping so their completions do not wait for the timeout.
		self.collect_pool(&mut out);
		if !out.is_empty() {
			self.land(out);
			return Ok(self.done.front().expect("just pushed").0);
		}
		self.events.clear();
		self.poller.wait(&mut self.events, timeout)?;
		let mut batch = std::mem::take(&mut self.scratch);
		batch.clear();
		batch.extend(self.events.iter());
		let result = batch
			.iter()
			.try_for_each(|event| self.deliver(event, &mut out));
		self.scratch = batch;
		result?;
		self.collect_pool(&mut out);
		self.land(out);
		match self.done.front() {
			Some((id, _)) => Ok(*id),
			None => Err(io::Error::from(io::ErrorKind::WouldBlock)),
		}
	}

	fn take(&mut self, id: u64) -> Result<Response, Error> {
		match self.done.pop_front() {
			Some((found, response)) if found == id => response,
			Some((found, response)) => {
				self.done.push_front((found, response));
				Err(Error::from(io::Error::other(format!(
					"take({id}) found a completion for {found}"
				))))
			}
			None => Err(Error::from(io::Error::new(
				io::ErrorKind::NotFound,
				"no completion to take",
			))),
		}
	}

	fn doorbell(&self) -> Arc<dyn Doorbell> {
		self.poller.clone()
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
fn run_op(op: &mut Op) -> io::Result<Progress> {
	let Op { fd, work, fds, .. } = op;
	// SAFETY: the descriptor stays open for the operation's flight.
	let fd = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
	match work {
		Work::Read { buffers } => {
			let Some((ptr, len)) = first_buffer(buffers) else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// SAFETY: the reactor holds the memory backing this pointer until the operation lands.
			match call(unsafe { libc::read(fd.as_raw_fd(), ptr.cast(), len) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Work::Write { buffers } => {
			let Some((ptr, len)) = first_buffer(buffers) else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// SAFETY: the reactor holds the memory backing this pointer until the operation lands.
			match call(unsafe { libc::write(fd.as_raw_fd(), ptr.cast(), len) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Work::Accept => match rustix::net::accept(fd) {
			Ok(socket) => {
				// Set flags after accept because `accept4` is not available on every Unix.
				rustix::io::fcntl_setfd(&socket, rustix::io::FdFlags::CLOEXEC)?;
				rustix::io::ioctl_fionbio(&socket, true)?;
				let handle = RawHandle::from_raw(socket.into_raw_fd() as usize);
				// SAFETY: the kernel created this descriptor for the accept,
				// and the response is its only owner.
				Ok(Progress::Done(Response::Accepted(unsafe {
					from_raw_socket(handle)
				})))
			}
			Err(rustix::io::Errno::AGAIN) => Ok(Progress::Blocked),
			Err(error) => Err(error.into()),
		},
		Work::Recv { buffers } => {
			let (ptr, len) = one_buffer(buffers)?;
			match call(unsafe { libc::recv(fd.as_raw_fd(), ptr.cast(), len, libc::MSG_DONTWAIT) })?
			{
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Work::RecvWithFds { buffers } => {
			let (ptr, len) = one_buffer(buffers)?;
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
					take_fds(&msg, fds);
					Ok(Progress::Done(Response::CountWithFds(
						count,
						std::mem::take(fds),
					)))
				}
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Work::Send { buffers } => {
			let (ptr, len) = one_buffer(buffers)?;
			let carried: Vec<libc::c_int> = fds.iter().map(|fd| fd.as_raw_fd()).collect();
			let result = if carried.is_empty() {
				call(unsafe { libc::send(fd.as_raw_fd(), ptr.cast(), len, libc::MSG_DONTWAIT) })
			} else {
				let mut iov = libc::iovec {
					iov_base: ptr.cast(),
					iov_len: len,
				};
				let mut space =
					[MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
				let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
				msg.msg_iov = &mut iov;
				msg.msg_iovlen = 1;
				msg.msg_control = space.as_mut_ptr().cast();
				msg.msg_controllen =
					unsafe { libc::CMSG_LEN((carried.len() * size_of::<libc::c_int>()) as u32) }
						as _;
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
		assert!(!backend.armed.contains(&key));
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
		assert!(!backend.armed.contains(&key));
		assert!(
			backend
				.readers
				.get(&fd)
				.is_none_or(|tokens| !tokens.contains(&token))
		);
	}
}
