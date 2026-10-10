//! Readiness polling for sockets and a thread pool for regular files.

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
use crate::park::Park;
use crate::{Memory, OwnedDescriptor};

use super::Backend;

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
}

enum Progress {
	Done(Response),
	Blocked,
}

struct Job {
	task: Task<(u64, io::Result<usize>)>,
}

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

/// A pool job's wake rings the same doorbell the reactor's `Park` holds — the
/// `Wakeup` `Portable::new` returns, which the reactor hands to its `Park`.
/// It does not go through `Park::unpark`: a finished job only needs the reactor
/// to reap it, not the executor to tick.
struct PoolWake(Wakeup);

impl Wake for PoolWake {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		let _ = self.0.notify();
	}
}

pub(crate) struct Portable {
	poller: Poller,
	event: Wakeup,
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
	done: VecDeque<(u64, io::Result<Response>)>,
}

impl Portable {
	pub(super) fn new(config: &super::Config, embedded: bool) -> io::Result<(Portable, Wakeup)> {
		if config.iopoll || config.sqpoll {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"this backend has no IOPOLL or SQPOLL mode",
			));
		}
		if embedded {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"a poller has no descriptor a foreign poller can wait on",
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
		let fd = event.handle();
		backend.readers.entry(fd).or_default();
		backend.arm(fd)?;
		Ok((backend, event))
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

	/// Only a wait may drain the wake-up; otherwise a concurrent notification could be lost.
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

	fn deliver(
		&mut self,
		event: &Ready,
		out: &mut Vec<(u64, io::Result<Response>)>,
		waited: bool,
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
		if fd == self.event.handle() && waited {
			self.drain_wakeup()?;
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
	fn read(
		&mut self,
		id: u64,
		fd: RawHandle,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
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
		self.done
			.push_back((id, Err(rustix::io::Errno::CANCELED.into())));
	}

	fn poll(&mut self, timeout: Option<Duration>, _park: &Park) -> io::Result<u64> {
		if let Some((id, _)) = self.done.front() {
			return Ok(*id);
		}
		let waited = timeout != Some(Duration::ZERO);
		let mut out = Vec::new();
		// Collect finished jobs before sleeping so their completions do not wait for the timeout.
		self.collect_pool(&mut out);
		if !out.is_empty() {
			self.done.extend(out);
			return Ok(self.done.front().expect("just pushed").0);
		}
		self.events.clear();
		self.poller.wait(&mut self.events, timeout)?;
		let mut batch = std::mem::take(&mut self.scratch);
		batch.clear();
		batch.extend(self.events.iter());
		let result = batch
			.iter()
			.try_for_each(|event| self.deliver(event, &mut out, waited));
		self.scratch = batch;
		result?;
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

pub(super) fn cancelled(error: &io::Error) -> bool {
	error.raw_os_error() == Some(rustix::io::Errno::CANCELED.raw_os_error())
}

fn takes_pool(fd: RawHandle) -> bool {
	let Ok(stat) = rustix::fs::fstat(unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) }) else {
		return false;
	};
	rustix::fs::FileType::from_raw_mode(stat.st_mode as _) == rustix::fs::FileType::RegularFile
}

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
			// SAFETY: the job retains the memory backing this span; the syscall copies through its pointer without forming a Rust reference.
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

fn run_op(op: &mut Op) -> io::Result<Progress> {
	let Op { fd, work, fds, .. } = op;
	// SAFETY: the descriptor stays open for the operation's flight.
	let fd = unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) };
	match work {
		Work::Read { memory, extents } => {
			let Some((ptr, len)) = first_buffer(memory, extents)? else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// SAFETY: the operation retains the memory backing this pointer until the syscall returns.
			match call(unsafe { libc::read(fd.as_raw_fd(), ptr.cast(), len) })? {
				Call::Bytes(count) => Ok(Progress::Done(Response::Count(count))),
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
		Work::Write { memory, extents } => {
			let Some((ptr, len)) = first_buffer(memory, extents)? else {
				return Ok(Progress::Done(Response::Count(0)));
			};
			// SAFETY: the operation retains the memory backing this pointer until the syscall returns.
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
					fds.clear();
					Ok(Progress::Done(Response::Count(count)))
				}
				Call::Blocked => Ok(Progress::Blocked),
			}
		}
	}
}

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

fn first_buffer(
	memory: &Arc<dyn Memory>,
	extents: &Extents,
) -> io::Result<Option<(*mut u8, usize)>> {
	let buffers = crate::memory::spans(memory, extents)?;
	Ok(buffers.first().map(|buf| (buf.as_ptr(), buf.len())))
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
		let (mut backend, _event) = Portable::new(&Config::default(), false).unwrap();
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
		let (mut backend, _event) = Portable::new(&Config::default(), false).unwrap();
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
