//! The portable backend: `polling` for readiness, a thread pool for files.
//!
//! A regular file has no readiness to wait for: every poller reports it ready
//! whether or not the read will block, and the answer is settled inside the
//! syscall. The other way out is the platform's asynchronous file I/O — on
//! macOS that is POSIX AIO, which holds one process to sixteen outstanding
//! requests out of ninety for the whole machine, gives a slot back only when
//! the request's status is *retrieved* rather than when it completes, and
//! answers the seventeenth with `EAGAIN`. This backend moves the bytes on a
//! worker thread instead: what cannot be waited for runs on `blocking`'s pool
//! and comes back through the reactor's own wake-up.
//!
//! What is left is readiness for the descriptors that have it — sockets, pipes,
//! and the wake-up itself — and that is `polling`'s
//! (`epoll`/`kqueue`/event ports/`poll`), so one implementation covers every
//! unix.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::mem::{MaybeUninit, size_of};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, IntoRawFd};
use std::sync::Arc;
use std::task::{Context, Poll as TaskPoll, Wake, Waker};
use std::time::Duration;

use blocking::{Task, unblock};
use polling::{Event as Ready, Events, Poller};

use crate::core::Response;
use crate::desc::{RawHandle, from_raw_socket};
use crate::event::Event as Wakeup;
use crate::memory::Extents;
use crate::{Memory, OwnedDescriptor};

use super::Backend;

/// Which way a descriptor is waited on.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Direction {
	Readable,
	Writable,
}

/// How many events one wait takes out of the poller.
const EVENTS: usize = 64;

/// The most descriptors one control message carries.
const MAX_FDS: usize = 8;

/// What a file operation does.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
	Read,
	Write,
	Fsync,
	Flush,
}

/// What a readiness-driven operation did.
enum Progress {
	/// Finished, with what it produced: the bytes it moved, a descriptor it
	/// accepted, or a receive that also brought descriptors.
	Done(Response),
	/// The descriptor would block.
	Blocked,
}

/// One file operation on the pool.
struct Job {
	/// Its result, and its completion: polling the task is what drives the job
	/// to land, so it is kept until it does.
	task: Task<(u64, io::Result<usize>)>,
}

/// What a readiness-driven operation is. The variant is the operation's own;
/// no shared type stands in for all of them.
enum Work {
	Read {
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Write {
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Recv {
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	RecvWithFds {
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Send {
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Accept,
}

/// An operation that runs once its descriptor is ready.
struct Op {
	fd: RawHandle,
	direction: Direction,
	work: Work,
	/// The descriptors the operation carries: the ones a send owes the peer,
	/// or the ones a receive has collected.
	fds: Vec<OwnedDescriptor>,
}

/// A readiness waiter.
struct Poll {
	fd: RawHandle,
	direction: Direction,
}

/// Raises the reactor's wake-up when a pool job lands. A job finishes on a
/// worker's clock, so the driver has to be woken out of its wait.
struct PoolWake(Wakeup);

impl Wake for PoolWake {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		let _ = self.0.notify();
	}
}

pub struct Portable {
	poller: Poller,
	/// The reactor's wake-up: a source like any other, with no token of its own.
	event: Wakeup,
	/// Readiness waiters, per descriptor and direction.
	readers: HashMap<RawHandle, HashSet<u64>>,
	writers: HashMap<RawHandle, HashSet<u64>>,
	/// The key the poller reports a descriptor under, and the way back.
	keys: HashMap<RawHandle, usize>,
	sources: HashMap<usize, RawHandle>,
	/// Keys the poller holds a registration for.
	armed: HashSet<usize>,
	next_key: usize,
	/// Operations that run once their descriptor is ready.
	ops: HashMap<u64, Op>,
	/// Readiness waiters, with what they were armed for.
	polls: HashMap<u64, Poll>,
	/// File operations the pool is running.
	pool: HashMap<u64, Job>,
	waker: Waker,
	/// Where a wait puts what it took out of the poller; kept across calls.
	events: Events,
	scratch: Vec<Ready>,
	/// Completions that needed no wait.
	done: VecDeque<(u64, io::Result<Response>)>,
}

impl Portable {
	pub fn new(config: &super::Config) -> io::Result<(Portable, Wakeup)> {
		if config.iopoll || config.sqpoll {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"this backend has no IOPOLL or SQPOLL mode",
			));
		}
		if config.wakeup == super::PollMode::Fd {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"a poller has no descriptor a foreign poller can wait on; use PollMode::Poll",
			));
		}
		let poller = Poller::new()?;
		let event = Wakeup::new()?;
		let mut backend = Portable {
			poller,
			event: event.clone(),
			readers: HashMap::new(),
			writers: HashMap::new(),
			keys: HashMap::new(),
			sources: HashMap::new(),
			armed: HashSet::new(),
			next_key: 0,
			ops: HashMap::new(),
			polls: HashMap::new(),
			pool: HashMap::new(),
			waker: Waker::from(Arc::new(PoolWake(event.clone()))),
			events: Events::with_capacity(NonZeroUsize::new(EVENTS).expect("a nonzero capacity")),
			scratch: Vec::with_capacity(EVENTS),
			done: VecDeque::new(),
		};
		// The wake-up is armed for the reactor's life: a submission from
		// another thread raises it, and a parked poll has to see that.
		let fd = event.handle();
		backend.readers.entry(fd).or_default();
		backend.arm(fd)?;
		Ok((backend, event))
	}

	/// Arms the poller for `fd`'s current interest: the directions some token
	/// waits on.
	fn arm(&mut self, fd: RawHandle) -> io::Result<()> {
		let key = match self.keys.get(&fd) {
			Some(key) => *key,
			None => {
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
		// Level-triggered: a source that stays ready is reported again, which
		// is what a re-armed poll needs.
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

	/// Claims `fd` for `token`, and arms it.
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

	/// Drops `token`'s claim on `fd`, and the registration once nobody waits.
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

	/// Re-registers `fd`'s interest, or drops it once nothing waits on it; the
	/// wake-up keeps its own. A refused deregistration is reported and the key
	/// goes anyway: `deliver` drops an event whose key it does not know.
	fn rearm(&mut self, fd: RawHandle) -> io::Result<()> {
		let waited = self.readers.contains_key(&fd) || self.writers.contains_key(&fd);
		if waited || fd == self.event.handle() {
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

	/// Drains the wake-up. Only a wait that the wake-up ended may clear it: a
	/// raise another thread did while this poll ran is news nobody has looked
	/// at yet.
	fn drain_wakeup(&self) -> io::Result<()> {
		let fd = unsafe { BorrowedFd::borrow_raw(self.event.handle().as_raw_fd()) };
		let mut buf = [0u8; 64];
		loop {
			match rustix::io::read(fd, &mut buf) {
				Ok(_) => continue,
				Err(rustix::io::Errno::AGAIN) => return Ok(()),
				Err(rustix::io::Errno::INTR) => continue,
				Err(error) => return Err(error.into()),
			}
		}
	}

	/// One thing the poller reported: readiness on a descriptor.
	fn deliver(
		&mut self,
		event: &Ready,
		out: &mut Vec<(u64, io::Result<Response>)>,
		waited: bool,
	) -> io::Result<()> {
		let Some(fd) = self.sources.get(&event.key).copied() else {
			// The registration was dropped while the event was in flight.
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
		// The reactor's own wake-up: its counter is drained last, and only by a
		// poll that was allowed to wait.
		if fd == self.event.handle() && waited {
			self.drain_wakeup()?;
		}
		Ok(())
	}

	/// A descriptor `token` waits on is ready: run the operation it carries, or
	/// report readiness.
	fn ready(&mut self, token: u64, out: &mut Vec<(u64, io::Result<Response>)>) -> io::Result<()> {
		let Some(op) = self.ops.get_mut(&token) else {
			let Some(poll) = self.polls.get(&token) else {
				// Cancelled while the event was in flight.
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

	/// Hands a file operation to the pool. Everything it touches goes with it:
	/// the memory keeps the spans alive, and the descriptor is a value.
	fn submit_pool(
		&mut self,
		token: u64,
		fd: RawHandle,
		fdoff: u64,
		kind: Kind,
		memory: Option<Arc<dyn Memory>>,
		extents: Extents,
	) -> io::Result<()> {
		if !matches!(kind, Kind::Fsync | Kind::Flush) && extents.is_empty() {
			self.done.push_back((token, Ok(Response::Count(0))));
			return Ok(());
		}
		let task = unblock(move || (token, run_file(fd, fdoff, kind, memory, extents)));
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

	/// Takes the pool jobs that landed.
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
			out.push((token, result.map(Response::Count)));
		}
	}
}

impl Drop for Portable {
	fn drop(&mut self) {
		// A registration outliving its descriptor is what the poller's contract
		// forbids, so every source goes before the reactor does.
		for (_, fd) in std::mem::take(&mut self.sources) {
			let _ = self
				.poller
				.delete(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) });
		}
		// A worker cannot be interrupted: the jobs in flight are waited out, so
		// none of them is left touching a descriptor the caller has closed.
		let mut out = Vec::new();
		while !self.pool.is_empty() {
			self.collect_pool(&mut out);
			out.clear();
			std::thread::sleep(Duration::from_millis(1));
		}
	}
}

impl Backend for Portable {
	fn read(
		&mut self,
		id: u64,
		fd: RawHandle,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		// A regular file has no readiness to wait for, so it runs on the pool;
		// anything else has readiness instead.
		if takes_pool(fd) {
			self.submit_pool(id, fd, fdoff, Kind::Read, Some(memory), extents)
		} else {
			self.submit_op(
				id,
				fd,
				Direction::Readable,
				Work::Read { memory, extents },
				Vec::new(),
			)
		}
	}

	fn write(
		&mut self,
		id: u64,
		fd: RawHandle,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		if takes_pool(fd) {
			self.submit_pool(id, fd, fdoff, Kind::Write, Some(memory), extents)
		} else {
			self.submit_op(
				id,
				fd,
				Direction::Writable,
				Work::Write { memory, extents },
				Vec::new(),
			)
		}
	}

	fn fsync(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		if takes_pool(fd) {
			return self.submit_pool(id, fd, 0, Kind::Fsync, None, Extents::new());
		}
		// Nothing to wait for on a descriptor the kernel does not sync: the
		// call answers now.
		let result = rustix::fs::fsync(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
			.map(|()| Response::Done)
			.map_err(io::Error::from);
		self.done.push_back((id, result));
		Ok(())
	}

	fn flush(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		if takes_pool(fd) {
			return self.submit_pool(id, fd, 0, Kind::Flush, None, Extents::new());
		}
		// Nothing to wait for on a descriptor the kernel does not sync: the
		// dup-and-close answers now.
		let result = flush_handle(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
			.map(|()| Response::Done);
		self.done.push_back((id, result));
		Ok(())
	}

	fn recv(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		self.submit_op(
			id,
			fd,
			Direction::Readable,
			Work::Recv { memory, extents },
			Vec::new(),
		)
	}

	fn recv_with_fds(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		self.submit_op(
			id,
			fd,
			Direction::Readable,
			Work::RecvWithFds { memory, extents },
			Vec::new(),
		)
	}

	fn send(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		self.submit_op(
			id,
			fd,
			Direction::Writable,
			Work::Send { memory, extents },
			Vec::new(),
		)
	}

	fn send_with_fds(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<()> {
		self.submit_op(
			id,
			fd,
			Direction::Writable,
			Work::Send { memory, extents },
			fds,
		)
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

	fn cancel(&mut self, id: u64) {
		if self.pool.contains_key(&id) {
			// The worker is inside a syscall and cannot be called back. The job
			// runs to its end and its completion is still reported: a caller
			// waiting for the buffers to be released waits for exactly that.
			return;
		}
		if let Some(op) = self.ops.remove(&id) {
			// A deregistration the poller refuses is reported by `rearm`; there is
			// nowhere else to report it, and the completion below is what a caller
			// waiting on the cancel is after.
			let _ = self.unwatch(op.fd, op.direction, id);
		} else if let Some(poll) = self.polls.remove(&id) {
			let _ = self.unwatch(poll.fd, poll.direction, id);
		} else {
			// Nothing held it: the completion already landed.
			return;
		}
		// A cancelled operation still completes, and it is the completion that
		// releases the reactor's entry: nobody waits for the result any more,
		// but the caller's `cancel` does wait for this.
		self.done
			.push_back((id, Err(rustix::io::Errno::CANCELED.into())));
	}

	fn poll(&mut self, timeout: Option<Duration>) -> io::Result<u64> {
		if let Some((id, _)) = self.done.front() {
			return Ok(*id);
		}
		// A zero timeout is a non-blocking look, not a pass: readiness is the
		// poller's news, and only a wait takes it out. The wake-up is the
		// exception — clearing it is what a wait earns.
		let waited = timeout != Some(Duration::ZERO);
		let mut out = Vec::new();
		// A pool job lands on a worker's clock, so it can be finished before
		// the wait even starts. News in hand is handed back: a completion a
		// caller waits for while the reactor sleeps on it is a stall the
		// timeout, not the work, ends.
		self.collect_pool(&mut out);
		if !out.is_empty() {
			self.done.extend(out);
			return Ok(self.done.front().expect("just pushed").0);
		}
		self.events.clear();
		self.poller.wait(&mut self.events, timeout)?;
		// The events borrow the poller's buffer; the batch is copied out so the
		// dispatch below can take the backend by `&mut`.
		let mut batch = std::mem::take(&mut self.scratch);
		batch.clear();
		batch.extend(self.events.iter());
		let result = batch
			.iter()
			.try_for_each(|event| self.deliver(event, &mut out, waited));
		self.scratch = batch;
		result?;
		// A job that landed while the events were being dispatched.
		self.collect_pool(&mut out);
		self.done.extend(out);
		match self.done.front() {
			Some((id, _)) => Ok(*id),
			None => Err(io::Error::from(io::ErrorKind::WouldBlock)),
		}
	}

	fn take(&mut self, id: u64) -> io::Result<Response> {
		match self.done.pop_front() {
			Some((found, response)) if found == id => response,
			Some((found, response)) => {
				self.done.push_front((found, response));
				Err(io::Error::other(format!(
					"take({id}) found a completion for {found}"
				)))
			}
			None => Err(io::Error::new(
				io::ErrorKind::NotFound,
				"no completion to take",
			)),
		}
	}

	fn poll_fd(&self) -> io::Result<Wakeup> {
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a poller is not a descriptor a foreign poller can wait on",
		))
	}
}

/// Whether `error` is a cancellation. A readiness operation is stopped by
/// dropping it, which reports `-ECANCELED`; a pool job cannot be stopped and
/// runs to its end.
pub(super) fn cancelled(error: &io::Error) -> bool {
	error.raw_os_error() == Some(rustix::io::Errno::CANCELED.raw_os_error())
}

/// Whether this descriptor runs on the pool: a regular file has no readiness.
fn takes_pool(fd: RawHandle) -> bool {
	let Ok(stat) = rustix::fs::fstat(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) }) else {
		return false;
	};
	rustix::fs::FileType::from_raw_mode(stat.st_mode as _) == rustix::fs::FileType::RegularFile
}

/// Runs one file operation on a worker thread: one `pread`/`pwrite` over the
/// first buffer the extents name. The spans are resolved here, on the thread
/// that moves the bytes, so nothing borrowed crosses the pool.
fn run_file(
	fd: RawHandle,
	fdoff: u64,
	kind: Kind,
	memory: Option<Arc<dyn crate::Memory>>,
	extents: Extents,
) -> io::Result<usize> {
	let fd = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
	match kind {
		Kind::Fsync => rustix::fs::fsync(fd).map(|()| 0).map_err(io::Error::from),
		Kind::Flush => flush_handle(fd).map(|()| 0),
		Kind::Read | Kind::Write => {
			let memory = memory.expect("a transfer carries its memory");
			let spans = crate::memory::spans(&memory, &extents)?;
			let Some(span) = spans.first() else {
				return Ok(0);
			};
			// The span names a buffer the memory the submission holds owns,
			// and it stays alive in this job until it returns. The kernel
			// copies through the pointer: no reference to the buffer is
			// formed here.
			let offset = fdoff as libc::off_t;
			count(unsafe {
				match kind {
					Kind::Read => {
						libc::pread(fd.as_raw_fd(), span.as_ptr().cast(), span.len(), offset)
					}
					Kind::Write => {
						libc::pwrite(fd.as_raw_fd(), span.as_ptr().cast(), span.len(), offset)
					}
					Kind::Fsync | Kind::Flush => unreachable!("handled above"),
				}
			})
		}
	}
}

/// Reports a file's deferred write-back errors: `close(dup(fd))`, so the
/// descriptor the caller named stays open.
///
/// The close is the whole point — it is what reports a write-back error the
/// filesystem had deferred — so it goes through `libc::close` and the result is
/// checked: `rustix::io::close` and a dropped `OwnedFd` both discard it. EINTR
/// is not retried: POSIX leaves the descriptor's state unspecified, and here
/// Linux has already closed it, so a retry could close a reused number.
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

/// The bytes a raw syscall moved, `errno` being the error it left.
fn count(result: libc::ssize_t) -> io::Result<usize> {
	if result < 0 {
		Err(io::Error::last_os_error())
	} else {
		Ok(result as usize)
	}
}

/// One readiness-driven syscall's answer.
enum Call {
	/// The bytes it moved.
	Bytes(usize),
	/// The peer is not ready yet, which is what the wait is for.
	Blocked,
}

/// As [`count`], except that a not-ready answer is expected rather than an
/// error.
fn call(result: libc::ssize_t) -> io::Result<Call> {
	match count(result) {
		Ok(bytes) => Ok(Call::Bytes(bytes)),
		Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(Call::Blocked),
		Err(error) => Err(error),
	}
}

/// One call of a readiness-driven operation; `Done` carries what the call
/// produced.
fn run_op(op: &mut Op) -> io::Result<Progress> {
	let Op { fd, work, fds, .. } = op;
	// SAFETY: the descriptor stays open for the operation's flight.
	let fd = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
	match work {
		Work::Read { memory, extents } => {
			let Some((ptr, len)) = first_buffer(memory, extents)? else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// The pointer names a buffer the memory the operation carries
			// owns, and that memory stays alive for as long as its flight
			// does. The kernel copies through the pointer: no reference to the
			// buffer is formed here.
			match call(unsafe { libc::read(fd.as_raw_fd(), ptr.cast(), len) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Work::Write { memory, extents } => {
			let Some((ptr, len)) = first_buffer(memory, extents)? else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// As on the read side: the kernel copies through the pointer.
			match call(unsafe { libc::write(fd.as_raw_fd(), ptr.cast(), len) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Work::Accept => match rustix::net::accept(fd) {
			Ok(socket) => {
				// `SOCK_CLOEXEC`/`SOCK_NONBLOCK` are `accept4` flags, and not
				// every unix has that call, so they go on the descriptor
				// afterwards. A failure here drops the accepted socket on the
				// way out.
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
		// A socket transfer takes one buffer.
		Work::Recv { memory, extents } => {
			let (ptr, len) = one_buffer(memory, extents)?;
			match call(unsafe { libc::recv(fd.as_raw_fd(), ptr.cast(), len, 0) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Work::RecvWithFds { memory, extents } => {
			let (ptr, len) = one_buffer(memory, extents)?;
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
			match call(unsafe { libc::recvmsg(fd.as_raw_fd(), &mut msg, 0) })? {
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
		Work::Send { memory, extents } => {
			let (ptr, len) = one_buffer(memory, extents)?;
			let carried: Vec<libc::c_int> = fds.iter().map(|fd| fd.as_raw_fd()).collect();
			let result = if carried.is_empty() {
				call(unsafe { libc::send(fd.as_raw_fd(), ptr.cast(), len, 0) })
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
				call(unsafe { libc::sendmsg(fd.as_raw_fd(), &msg, 0) })
			};
			match result? {
				Call::Bytes(count) => {
					// The descriptors left with the bytes of this one call.
					fds.clear();
					Ok(Progress::Done(Response::Count(count)))
				}
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
	}
}

/// The one buffer a socket transfer moves: a `recv`/`send` names exactly one.
///
/// The pointer names the memory the operation holds: it comes from a span, and
/// the `Arc` the operation carries is what keeps that memory alive while the
/// kernel copies through it.
fn one_buffer(memory: &Arc<dyn Memory>, extents: &Extents) -> io::Result<(*mut u8, usize)> {
	let buffers = crate::memory::spans(memory, extents)?;
	let [buf] = buffers.as_slice() else {
		return Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a socket transfer needs exactly one buffer",
		));
	};
	Ok((buf.as_ptr(), buf.len()))
}

/// The first buffer the extents resolve to, and its length; `None` when they
/// name no buffer. One call moves one buffer, and the rest is the caller's to
/// loop over.
fn first_buffer(
	memory: &Arc<dyn Memory>,
	extents: &Extents,
) -> io::Result<Option<(*mut u8, usize)>> {
	let buffers = crate::memory::spans(memory, extents)?;
	Ok(buffers.first().map(|buf| (buf.as_ptr(), buf.len())))
}

/// The descriptors the kernel wrote into `msg`'s control buffer, taken into
/// `out`. `SCM_RIGHTS` is the only message this backend asks for.
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
				// The descriptors arrive without crossing an exec.
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

	/// A deregistration of a descriptor the process no longer has leaves the
	/// reactor with nothing to act on: the token is out of `readers`, and the
	/// key out of `keys`, `sources` and `armed` — an event for a key `deliver`
	/// does not know is dropped.
	///
	/// Whether the poller calls that a refusal is the host's: `epoll_ctl`
	/// refuses a number the process has lost, while `kevent` drops a
	/// descriptor's knotes when it is closed and answers `ENOENT` for the same
	/// call, which the poller reports as a deregistration that already
	/// happened. Both leave the reactor with nothing behind, which is what is
	/// asserted here; the refusal itself is the test below.
	#[test]
	fn a_deregistration_of_a_closed_descriptor_leaves_no_key_behind() {
		let (mut backend, _event) = Portable::new(&Config::default()).unwrap();
		let (reader, _writer) = std::io::pipe().unwrap();
		let token = 7;
		let fd = RawHandle::from_descriptor(reader.as_descriptor());
		backend.watch(fd, Direction::Readable, token).unwrap();
		let key = backend.keys[&fd];
		// A number the process no longer has is what `epoll_ctl` and `kevent`
		// are asked to drop.
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

	/// A deregistration the poller refuses is reported and the key goes anyway:
	/// an event the kernel had already reported for that key is one `deliver`
	/// drops, because `sources` no longer knows it.
	///
	/// The poller is made to have lost the source while the reactor's maps
	/// still name it, which is what a host that answers the close with a
	/// refusal, rather than with a knote that is already gone, looks like from
	/// here.
	#[test]
	fn a_refused_deregistration_leaves_no_key_behind() {
		let (mut backend, _event) = Portable::new(&Config::default()).unwrap();
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
