//! Core completion engine.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io;
use std::ops::Deref;
use std::panic::Location;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, TryLockError, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use crate::desc::{BorrowedDescriptor, RawHandle};
use crate::{AsDescriptor, Extent, Extents, Memory, OwnedDescriptor, OwnedSocket};

use crate::backend::{self, Backend, BackendEnum, Config, create};
use crate::channel;
use crate::event::Event;

/// What a completed operation produced.
///
/// The variant is the operation's own: a transfer reports what it moved, an
/// accept reports the socket the kernel created, a receive that asked for
/// control data reports the descriptors that came with the bytes.
#[derive(Debug)]
pub enum Response {
	/// The bytes a transfer moved.
	Count(usize),
	/// A receive that also brought descriptors.
	CountWithFds(usize, Vec<OwnedDescriptor>),
	/// The socket an accept produced; a response that is never taken closes it.
	Accepted(OwnedSocket),
	/// A sync or a wait: the completion itself is all of it.
	Done,
}

impl Response {
	/// The count a transfer's response carries. The descriptors a
	/// [`Response::CountWithFds`] brought go with it, and are closed here.
	pub fn count(self) -> io::Result<usize> {
		match self {
			Response::Count(count) | Response::CountWithFds(count, _) => Ok(count),
			Response::Accepted(_) => {
				Err(io::Error::other("an accept reported a socket, not a count"))
			}
			Response::Done => Err(io::Error::other("a transfer reported no count")),
		}
	}

	/// The count a response carries, counting a sync, a wait or an accept as
	/// nothing moved.
	pub fn count_or_zero(self) -> usize {
		match self {
			Response::Count(count) | Response::CountWithFds(count, _) => count,
			Response::Accepted(_) | Response::Done => 0,
		}
	}
}

/// A submission waiting for the reactor: the operation's arguments, kept whole
/// until a backend takes it.
///
/// The variant names the operation and the backend method it goes to; no
/// single type is shared by every operation beyond this queue entry.
enum Request {
	Read {
		fd: RawHandle,
		/// The file position the transfer starts at.
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Write {
		fd: RawHandle,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Fsync {
		fd: RawHandle,
	},
	Flush {
		fd: RawHandle,
	},
	Recv {
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	/// A receive that also takes the descriptors a `recvmsg` control buffer
	/// carries.
	RecvWithFds {
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Send {
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	/// A send that carries `fds` with its first bytes.
	SendWithFds {
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
		fds: Vec<OwnedDescriptor>,
	},
	/// Accepts a connection on a listening socket. The response is
	/// [`Response::Accepted`]: the kernel reports one descriptor, and the
	/// backend takes it into an owner before the response is stored.
	Accept {
		fd: RawHandle,
	},
	/// Waits for `fd` to become readable.
	///
	/// A completion resolves once, so a caller that wants readiness again
	/// submits another one — this is not a standing registration.
	Readable {
		fd: RawHandle,
	},
	/// Waits for `fd` to become writable; sibling of [`Request::Readable`].
	Writable {
		fd: RawHandle,
	},
}

impl Request {
	/// Whether the kernel reads or writes the memory this request names. A
	/// transfer named by `extents` does, whatever descriptors it carries: the
	/// descriptors go into the backend's control buffer, not the caller's
	/// memory.
	fn names_memory(&self) -> bool {
		match self {
			Request::Read { .. }
			| Request::Write { .. }
			| Request::Recv { .. }
			| Request::RecvWithFds { .. }
			| Request::Send { .. } => true,
			// Only a send carrying descriptors and no bytes touches nothing of
			// the caller's: the payload is what names memory.
			Request::SendWithFds { extents, .. } => extents.len_bytes() != 0,
			Request::Fsync { .. }
			| Request::Flush { .. }
			| Request::Accept { .. }
			| Request::Readable { .. }
			| Request::Writable { .. } => false,
		}
	}

	/// The operation's name, for the drop fallback's report.
	fn kind(&self) -> &'static str {
		match self {
			Request::Read { .. } => "read",
			Request::Write { .. } => "write",
			Request::Fsync { .. } => "fsync",
			Request::Flush { .. } => "flush",
			Request::Recv { .. } => "recv",
			Request::RecvWithFds { .. } => "recv_with_fds",
			Request::Send { .. } => "send",
			Request::SendWithFds { .. } => "send_with_fds",
			Request::Accept { .. } => "accept",
			Request::Readable { .. } => "readable",
			Request::Writable { .. } => "writable",
		}
	}
}

/// Where an operation is in its life.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stat {
	/// The request still waits for the reactor to hand it to a backend.
	Pending,
	/// A backend has it.
	InFlight,
	/// The result is in `response` and waits for its future.
	Done,
	/// The future went without the result.
	Abandoned,
}

/// One operation, from submission to response.
struct Entry {
	request: Option<Request>,
	response: Option<io::Result<Response>>,
	/// The task waiting for `response`.
	waker: Option<Waker>,
	stat: Stat,
	/// Ask a backend to stop the operation: set by [`Completion::until`] when
	/// its cancel fires, and by a dropped completion that has to wait for the
	/// kernel to let go.
	cancel: bool,
	/// The kernel reads or writes the caller's memory through this operation,
	/// so going without awaiting it is not just a lost result.
	names_memory: bool,
	/// The operation's name and the submitter's location, for the fallback's
	/// report of a dropped in-flight operation.
	kind: &'static str,
	origin: &'static Location<'static>,
	/// A thread waits on [`Shared::settled`] for this entry: [`Core::settle`]
	/// has to raise it once the entry lands.
	waiter: bool,
	/// A timer's deadline. It is what the reactor waits to and what settles
	/// the entry; a request has none, because a backend does its waiting.
	deadline: Option<Instant>,
	/// The descriptor the operation names, held until the entry goes: the
	/// backend works on the number, which must stay live until then.
	_descriptor: Option<Descriptor>,
}

/// What an in-flight operation holds so the resource it names stays open: the
/// clone's share of the caller's [`Handle`], with the value erased by unsizing
/// so an entry stays concrete whatever the handle names.
struct Descriptor {
	/// The hold on the resource, kept for what dropping it does.
	_value: Arc<dyn Send + Sync>,
	/// Dropped after `_value`, so the resource is let go before the wake; see
	/// [`Release`].
	_release: Release,
}

impl Descriptor {
	fn new<S: Send + Sync + 'static>(handle: &Handle<S>) -> Descriptor {
		// Bound first: an `Arc::clone` under the erased type would have to infer
		// its own parameter as the trait object.
		let value = Arc::clone(&handle.value);
		Self {
			_value: value,
			_release: handle.release.clone(),
		}
	}
}

/// The reactor's own state: the backend and what one pass over it needs.
///
/// Behind [`Shared::core`]. Locking order is this lock first, then
/// [`Shared::events`], everywhere.
struct Core {
	backend: BackendEnum,
}

impl Core {
	fn new(backend: BackendEnum) -> Core {
		Self { backend }
	}

	/// Waits for one completion, then takes everything else that came with it.
	///
	/// The wait is capped at the nearest timer: a timer is settled by the
	/// reactor's own clock, so a wait that outlasted one would sleep through it.
	fn wait(&mut self, shared: &Shared, timeout: Option<Duration>) -> io::Result<()> {
		let within = shared.events.lock().unwrap().within(timeout);
		match self.backend.poll(within) {
			Ok(id) => self.settle(shared, id),
			// Nothing completed within the deadline.
			Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
			Err(error) => return Err(error),
		}
		self.settle_timers(shared);
		self.reap(shared)
	}

	/// Settles every timer whose deadline has passed.
	///
	/// A timer has no backend behind it, so nothing else would land it: the
	/// reactor checks its own table after every wait, and after a poll that did
	/// not wait.
	fn settle_timers(&mut self, shared: &Shared) {
		let now = Instant::now();
		let mut wakers = Vec::new();
		{
			let mut events = shared.events.lock().unwrap();
			// Deadlines are sorted, so the first entry past `now` ends the
			// search.
			while let Some((&(deadline, id), _)) = events.deadlines.first_key_value() {
				if deadline > now {
					break;
				}
				events.deadlines.remove(&(deadline, id));
				let Some(entry) = events.entries.get_mut(&id) else {
					continue;
				};
				// A cancelled timer left the table already; only a waiting one
				// is here.
				if entry.stat != Stat::Pending {
					continue;
				}
				entry.deadline = None;
				entry.response = Some(Ok(Response::Done));
				entry.stat = Stat::Done;
				if let Some(waker) = entry.waker.take() {
					wakers.push(waker);
				}
			}
		}
		for waker in wakers {
			waker.wake();
		}
	}

	/// Takes every completion the backend already holds, without entering the
	/// kernel for more.
	fn reap(&mut self, shared: &Shared) -> io::Result<()> {
		loop {
			match self.backend.poll(Some(Duration::ZERO)) {
				Ok(id) => self.settle(shared, id),
				Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
				Err(error) => return Err(error),
			}
		}
	}

	/// Hands one finished operation its response.
	fn settle(&mut self, shared: &Shared, id: u64) {
		let response = self.backend.take(id);
		let (waiter, waker) = {
			let mut events = shared.events.lock().unwrap();
			let Some(entry) = events.get_mut(id) else {
				// The future went before the backend reported.
				return;
			};
			if entry.stat == Stat::Abandoned {
				events.remove(id);
				return;
			}
			entry.response = Some(response);
			entry.stat = Stat::Done;
			(entry.waiter, entry.waker.take())
		};
		if waiter {
			shared.settled.notify_all();
		}
		if let Some(waker) = waker {
			waker.wake();
		}
	}

	/// Hands a submission failure to whoever waits.
	fn fail(&mut self, shared: &Shared, id: u64, error: io::Error) {
		let (waiter, waker) = {
			let mut events = shared.events.lock().unwrap();
			let Some(entry) = events.get_mut(id) else {
				return;
			};
			if entry.stat == Stat::Abandoned {
				events.remove(id);
				return;
			}
			entry.response = Some(Err(error));
			entry.stat = Stat::Done;
			(entry.waiter, entry.waker.take())
		};
		if waiter {
			shared.settled.notify_all();
		}
		if let Some(waker) = waker {
			waker.wake();
		}
	}

	/// Hands the pending submissions and the cancels to the backend.
	fn drain(&mut self, shared: &Shared) {
		let mut wakers = Vec::new();
		let (pending, cancels) = {
			let mut events = shared.events.lock().unwrap();
			let Events { entries, deadlines } = &mut *events;
			let mut pending = Vec::new();
			let mut cancels = Vec::new();
			for (id, entry) in entries.iter_mut() {
				if entry.cancel {
					entry.cancel = false;
					match entry.stat {
						// Never submitted: there is nothing to stop. A timer is
						// such a one, and its deadline goes with the cancel.
						Stat::Pending => {
							entry.request = None;
							entry.response = Some(Err(not_submitted()));
							entry.stat = Stat::Done;
							if let Some(deadline) = entry.deadline.take() {
								deadlines.remove(&(deadline, *id));
							}
							if let Some(waker) = entry.waker.take() {
								wakers.push(waker);
							}
						}
						// The future went while the backend held it; an
						// abandoned entry still has to be stopped and
						// reaped, or it would sit in the map holding what it
						// keeps alive for as long as the backend waits.
						Stat::InFlight | Stat::Abandoned => cancels.push(*id),
						_ => {}
					}
					continue;
				}
				if entry.stat == Stat::Pending {
					if let Some(request) = entry.request.take() {
						// The backend has it from here; a future that goes in
						// the meantime is marked abandoned and cancelled below.
						entry.stat = Stat::InFlight;
						pending.push((*id, request));
					}
				}
			}
			(pending, cancels)
		};
		for waker in wakers {
			waker.wake();
		}
		for id in cancels {
			self.backend.cancel(id);
		}
		for (id, request) in pending {
			let result = match request {
				Request::Read {
					fd,
					fdoff,
					memory,
					extents,
				} => self.backend.read(id, fd, fdoff, memory, extents),
				Request::Write {
					fd,
					fdoff,
					memory,
					extents,
				} => self.backend.write(id, fd, fdoff, memory, extents),
				Request::Fsync { fd } => self.backend.fsync(id, fd),
				Request::Flush { fd } => self.backend.flush(id, fd),
				Request::Recv {
					fd,
					memory,
					extents,
				} => self.backend.recv(id, fd, memory, extents),
				Request::RecvWithFds {
					fd,
					memory,
					extents,
				} => self.backend.recv_with_fds(id, fd, memory, extents),
				Request::Send {
					fd,
					memory,
					extents,
				} => self.backend.send(id, fd, memory, extents),
				Request::SendWithFds {
					fd,
					memory,
					extents,
					fds,
				} => self.backend.send_with_fds(id, fd, memory, extents, fds),
				Request::Accept { fd } => self.backend.accept(id, fd),
				Request::Readable { fd } => self.backend.readable(id, fd),
				Request::Writable { fd } => self.backend.writable(id, fd),
			};
			if let Err(error) = result {
				self.fail(shared, id, error);
				continue;
			}
			// The future may have gone, or asked to stop, while the request was
			// in this loop: the backend has it, so the cancel goes now.
			let stop = {
				let mut events = shared.events.lock().unwrap();
				match events.get_mut(id) {
					Some(entry) => {
						let asked = entry.cancel;
						entry.cancel = false;
						asked || entry.stat == Stat::Abandoned
					}
					None => true,
				}
			};
			if stop {
				self.backend.cancel(id);
			}
		}
	}

	/// Drives the reactor until `id` is settled, for a drop that has to wait
	/// for the backend to let go and no other thread will reap for it.
	fn drive_until(&mut self, shared: &Shared, id: u64) -> io::Result<()> {
		loop {
			self.drain(shared);
			if shared.is_settled(id) {
				return Ok(());
			}
			self.reap(shared)?;
			if shared.is_settled(id) {
				return Ok(());
			}
			self.wait(shared, None)?;
		}
	}

	/// Drives the reactor until nothing is in flight, for a reactor going away
	/// with operations still out.
	fn drive_to_idle(&mut self, shared: &Shared) -> io::Result<()> {
		loop {
			self.drain(shared);
			if shared.is_idle() {
				return Ok(());
			}
			self.reap(shared)?;
			if shared.is_idle() {
				return Ok(());
			}
			self.wait(shared, None)?;
		}
	}
}

/// The reactor's table: every operation, keyed by the id the submitter minted,
/// and — separately — the deadlines of the timers among them.
///
/// A timer is an entry with no request: it never reaches a backend, and the
/// deadline here is what settles it instead. One lock covers an entry and its
/// deadline, so the two cannot drift apart.
struct Events {
	entries: HashMap<u64, Entry>,
	/// `(deadline, id)` for every timer still waiting, sorted so the nearest is
	/// `first_key_value`. The id is there because two timers may share a
	/// deadline.
	deadlines: BTreeMap<(Instant, u64), ()>,
}

impl Events {
	fn new() -> Events {
		Events {
			entries: HashMap::new(),
			deadlines: BTreeMap::new(),
		}
	}

	fn get(&self, id: u64) -> Option<&Entry> {
		self.entries.get(&id)
	}

	fn get_mut(&mut self, id: u64) -> Option<&mut Entry> {
		self.entries.get_mut(&id)
	}

	/// Inserts an entry, recording its deadline when it is a timer.
	fn insert(&mut self, id: u64, entry: Entry) {
		if let Some(deadline) = entry.deadline {
			self.deadlines.insert((deadline, id), ());
		}
		self.entries.insert(id, entry);
	}

	/// Removes an entry and the deadline it held, if any.
	fn remove(&mut self, id: u64) -> Option<Entry> {
		let entry = self.entries.remove(&id)?;
		if let Some(deadline) = entry.deadline {
			self.deadlines.remove(&(deadline, id));
		}
		Some(entry)
	}

	/// How long a caller may wait: its own timeout, the nearest deadline, or
	/// whichever comes first. `None` is "until something happens".
	fn within(&self, timeout: Option<Duration>) -> Option<Duration> {
		let nearest = self
			.deadlines
			.first_key_value()
			.map(|((deadline, _), _)| deadline.saturating_duration_since(Instant::now()));
		match (timeout, nearest) {
			(Some(timeout), Some(nearest)) => Some(timeout.min(nearest)),
			(Some(timeout), None) => Some(timeout),
			(None, Some(nearest)) => Some(nearest),
			(None, None) => None,
		}
	}
}

struct Shared {
	next: AtomicU64,
	/// Every operation, keyed by the id the submitter minted, and the timers'
	/// deadlines. The table is the lock: an entry's fields are read and written
	/// only under it. Waking is the exception — a `wake` can poll a future that
	/// takes the same lock, so it happens after the guard is dropped.
	events: Mutex<Events>,
	/// What wakes a blocked [`Reactor::poll`], and what every [`Event`] this
	/// reactor hands out raises.
	event: Event,
	/// Set by [`Submitter::stop`], and by [`Reactor`]'s drop under
	/// [`Shared::events`] so a racing submission is refused or cleaned up, not
	/// left in a table nobody drives; no new submission is taken afterwards.
	stopped: AtomicBool,
	/// Set while the reactor is inside its backend wait: a submission that
	/// lands now is one the reactor cannot see without being woken, and one
	/// that lands while it is not waiting is taken by the next drain.
	parked: AtomicBool,
	/// Set by [`Shared::wake_reactor`] for a wake raised on the reactor's own
	/// thread, taken by [`Reactor::poll`] before its wait: what that thread
	/// readied is run by the loop it returns to, so the wait must not block.
	ready: AtomicBool,
	/// The backend, and the one pass at a time the reactor takes over it.
	core: Mutex<Core>,
	/// How the backend's cancellation error is told from real work; see
	/// [`crate::backend::cancelled`]. A plain function so a racing `until` can
	/// read it without the core lock.
	cancelled: fn(&io::Error) -> bool,
	/// Raised by [`Core::settle`] when an entry whose [`Entry::waiter`] is set
	/// lands: what a drop from another thread waits on.
	settled: Condvar,
	/// The thread that drives this reactor: the one that last called
	/// [`Reactor::poll`] or owns the [`Executor`](crate::Executor). A drop on
	/// it drives the reactor inline, because nobody else will; on any other
	/// thread it notifies the reactor and waits on [`Shared::settled`].
	reactor_thread: Mutex<Option<ThreadId>>,
	/// What [`Reactor::poll_fd`] borrows, asked of the backend once at
	/// creation: a second ask would take the core lock.
	poll_fd: io::Result<Event>,
	/// How many completions were dropped while their operation still used the
	/// caller's memory, and so had to be waited out: one per missed cooperative
	/// cancel. A teardown that has already taken the entry does not count — the
	/// drop finds nothing to wait for.
	fallback_drops: AtomicU64,
}

impl Shared {
	/// Records the calling thread as the reactor's driver.
	fn enter_reactor(&self) {
		*self.reactor_thread.lock().unwrap() = Some(thread::current().id());
	}

	fn on_reactor_thread(&self) -> bool {
		*self.reactor_thread.lock().unwrap() == Some(thread::current().id())
	}

	/// Ends the reactor's wait, or records the wake when it is the reactor's
	/// own thread that raises it.
	///
	/// That thread is running the loop, so it will tick what the wake readied
	/// before it reaches another wait; `ready` keeps that wait from blocking on
	/// it. A thread other than the reactor's races with the wait — a raise that
	/// lands between the drain and it would be lost — so it raises the event,
	/// unconditionally.
	fn wake_reactor(&self) {
		if self.on_reactor_thread() {
			self.ready.store(true, Ordering::Release);
		} else {
			let _ = self.event.notify();
		}
	}

	/// Whether `id` has been handed to its future, or gone entirely.
	fn is_settled(&self, id: u64) -> bool {
		self.is_settled_locked(&self.events.lock().unwrap(), id)
	}

	/// Whether any operation is still in flight.
	fn is_idle(&self) -> bool {
		!self
			.events
			.lock()
			.unwrap()
			.entries
			.values()
			.any(|entry| matches!(entry.stat, Stat::Pending | Stat::InFlight))
	}
}

/// A wake-up that reaches a reactor's driver; see [`Shared::wake_reactor`].
///
/// It is `Send + Sync` and the `Waker` made from it clones by refcount, so a
/// task may hand one to another thread; [`ReactorWaker::around`] wraps a task's
/// own waker, and is what a spawned task polls with.
pub(super) struct ReactorWaker {
	shared: Arc<Shared>,
	/// The task waker this one wraps, when it wraps one: a wake has to run the
	/// task as well as reach the driver. `None` for the future `block_on` polls.
	inner: Option<Waker>,
}

impl ReactorWaker {
	fn new(shared: Arc<Shared>) -> ReactorWaker {
		Self {
			shared,
			inner: None,
		}
	}

	/// `inner`, with this wake-up behind it.
	pub(super) fn around(&self, inner: &Waker) -> Waker {
		Waker::from(Arc::new(Self {
			shared: Arc::clone(&self.shared),
			inner: Some(inner.clone()),
		}))
	}
}

impl Wake for ReactorWaker {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		// The task is queued before the driver is told: the other order lets a
		// driver woken first find nothing to run and park again.
		if let Some(inner) = &self.inner {
			inner.wake_by_ref();
		}
		self.shared.wake_reactor();
	}
}

/// The completion backend, the operations in flight, and the timers among them.
pub struct Reactor {
	shared: Arc<Shared>,
}

impl Reactor {
	/// Creates the backend.
	pub fn new(config: Config) -> io::Result<(Reactor, Submitter)> {
		config.validate()?;
		let (backend, event) = create(&config)?;
		// Asked once, here: the descriptor never changes, and asking later
		// would take the core lock, which a blocked `poll` holds.
		let poll_fd = backend.poll_fd();
		let shared = Arc::new(Shared {
			next: AtomicU64::new(0),
			events: Mutex::new(Events::new()),
			event,
			stopped: AtomicBool::new(false),
			parked: AtomicBool::new(false),
			ready: AtomicBool::new(false),
			core: Mutex::new(Core::new(backend)),
			cancelled: backend::cancelled(),
			poll_fd,
			settled: Condvar::new(),
			reactor_thread: Mutex::new(None),
			fallback_drops: AtomicU64::new(0),
		});
		Ok((
			Reactor {
				shared: Arc::clone(&shared),
			},
			Submitter { shared },
		))
	}

	/// Records this thread as the reactor's driver; see [`Shared::reactor_thread`].
	pub(super) fn enter_reactor(&self) {
		self.shared.enter_reactor();
	}

	/// The wake-up that ends this reactor's wait; see [`ReactorWaker`].
	pub(super) fn waker(&self) -> ReactorWaker {
		ReactorWaker::new(Arc::clone(&self.shared))
	}

	/// Drives the backend.
	pub fn poll(&mut self, timeout: Option<Duration>) -> io::Result<()> {
		self.shared.enter_reactor();
		let mut core = self.shared.core.lock().unwrap();
		core.drain(&self.shared);
		if timeout == Some(Duration::ZERO) {
			core.settle_timers(&self.shared);
			return core.reap(&self.shared);
		}
		// A submission from another thread has to raise the wake-up of a
		// reactor that is waiting, or it would sit unseen; a submission from
		// inside a future this thread is about to poll must not, or the wait a
		// device holds on that same wake-up resolves at once and the thread
		// spins. The flag is what tells them apart, and it is set before the
		// second drain so nothing queued in between is missed.
		self.shared.parked.store(true, Ordering::Release);
		core.drain(&self.shared);
		let result = if self.shared.ready.swap(false, Ordering::AcqRel) {
			// A wake landed on this thread while it was running: its loop runs
			// what that readied before it waits again, so this pass takes what
			// the backend already holds and returns instead of blocking.
			core.settle_timers(&self.shared);
			core.reap(&self.shared)
		} else {
			core.wait(&self.shared, timeout)
		};
		self.shared.parked.store(false, Ordering::Release);
		result
	}

	/// The descriptor a [`PollMode::Fd`](crate::PollMode::Fd) caller waits on.
	pub fn poll_fd(&self) -> io::Result<BorrowedDescriptor<'_>> {
		match &self.shared.poll_fd {
			Ok(event) => Ok(event.descriptor()),
			Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
		}
	}

	/// The reactor's own wake-up; see [`Event`].
	#[doc(hidden)]
	pub fn event(&self) -> Event {
		self.shared.event.clone()
	}

	/// Whether any operation is still in flight.
	#[doc(hidden)]
	pub fn is_idle(&self) -> bool {
		self.shared.is_idle()
	}

	/// How many completions this reactor's threads dropped while their
	/// operation still named the caller's memory: a test witness for a missed
	/// cooperative cancel, and a diagnostic.
	pub fn fallback_drops(&self) -> u64 {
		self.shared.fallback_drops.load(Ordering::Relaxed)
	}

	/// Stopped and every in-flight operation landed.
	#[doc(hidden)]
	pub fn is_stopped(&self) -> bool {
		self.shared.stopped.load(Ordering::Acquire) && self.is_idle()
	}
}

/// Stops the reactor and cancels every operation still out, driving them to
/// their ends so nothing is left touching memory after the reactor goes. A
/// completion dropped after this finds its entry gone and returns at once, and
/// a submission after it is refused the same as one after [`Submitter::stop`].
impl Drop for Reactor {
	fn drop(&mut self) {
		let shared = &self.shared;
		{
			let mut events = shared.events.lock().unwrap();
			// Set under the table's lock, so a submission racing this drop meets
			// one or the other: it sees `stopped` and is refused, or its entry is
			// here in time for the cleanup below. A check outside the lock would
			// let one insert after the table was cleared, into a reactor nobody
			// drives.
			shared.stopped.store(true, Ordering::Release);
			// A pending request never reached a backend; it goes with the map.
			// A timer is such a one, and its deadline goes with it.
			events
				.entries
				.retain(|_, entry| entry.stat != Stat::Pending);
			events.deadlines.clear();
			for entry in events.entries.values_mut() {
				entry.cancel = true;
			}
		}
		let _ = shared.event.notify();
		if let Ok(mut core) = shared.core.lock() {
			// A settle only happens while someone drives, and here that is this
			// thread: nothing else will reap, so the pass runs to the end.
			while !shared.is_idle() {
				if core.drive_to_idle(shared).is_err() {
					break;
				}
			}
		}
		let mut events = shared.events.lock().unwrap();
		events.entries.clear();
		events.deadlines.clear();
		shared.settled.notify_all();
	}
}

/// An owned resource an operation can name, and a caller can take back.
///
/// [`Submitter`] operations borrow one, and each keeps its own clone for as long
/// as it lives: what a backend works on stays a live descriptor whatever the
/// caller does with its own clones. [`Handle::take`] resolves only once every
/// other clone has let go, which is what makes the descriptor safe to close
/// afterwards.
pub struct Handle<S> {
	value: Arc<S>,
	/// Dropped after `value`, so a clone releases the resource before it wakes
	/// a taker; see [`Release`].
	release: Release,
}

impl<S> Handle<S> {
	/// Takes ownership of `value`.
	pub fn new(value: S) -> Handle<S> {
		Self {
			value: Arc::new(value),
			release: Release::new(),
		}
	}

	/// Waits until every other clone has let go, then returns the resource;
	/// `None` if another [`take`](Handle::take) is already waiting for it.
	///
	/// A taker registers before it tries, and a clone releases before it wakes:
	/// a release already past is seen by the try, and one that follows raises
	/// the registered waker.
	pub fn take(self) -> impl Future<Output = Option<S>> {
		let value = self.value;
		Take::new(value, self.release)
	}

	/// [`Handle::take`] without waiting: `Err(self)` while another clone still
	/// names the resource.
	pub fn try_take(self) -> Result<S, Handle<S>> {
		match Arc::try_unwrap(self.value) {
			Ok(value) => Ok(value),
			Err(value) => Err(Self {
				value,
				release: self.release,
			}),
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> From<S> for Handle<S> {
	fn from(value: S) -> Handle<S> {
		Handle::new(value)
	}
}

impl<S: AsDescriptor> Handle<S> {
	/// The number the backend works on.
	///
	/// Not the public `AsDescriptor` impl: on unix that is the blanket over
	/// `AsFd`, and `S: AsDescriptor` does not imply `S: AsFd`.
	fn raw(&self) -> RawHandle {
		RawHandle::from_descriptor(self.value.as_descriptor())
	}
}

impl<S> Clone for Handle<S> {
	fn clone(&self) -> Handle<S> {
		Self {
			value: Arc::clone(&self.value),
			release: self.release.clone(),
		}
	}
}

impl<S> Deref for Handle<S> {
	type Target = S;

	fn deref(&self) -> &S {
		&self.value
	}
}

/// A clone's share of the hold on the resource a [`Handle`] named.
///
/// Its drop is the wake, and it is the handle's last field: the resource is let
/// go first. A taker woken here cannot look while the resource is still held and
/// then find no wake left to come.
struct Release {
	waker: Arc<WakerSlot>,
}

impl Release {
	fn new() -> Release {
		Self {
			waker: Arc::new(WakerSlot::new()),
		}
	}

	fn wake(&self) {
		self.waker.wake();
	}
}

impl Clone for Release {
	fn clone(&self) -> Release {
		Self {
			waker: Arc::clone(&self.waker),
		}
	}
}

impl Drop for Release {
	fn drop(&mut self) {
		self.waker.wake();
	}
}

/// Where a taker parks; a clone's release wakes it.
///
/// One taker at a time: [`Handle::take`] marks the slot taken, and a second take
/// gives up rather than share the one waker.
struct WakerSlot {
	taken: AtomicBool,
	waker: Mutex<Option<Waker>>,
}

impl WakerSlot {
	fn new() -> WakerSlot {
		Self {
			taken: AtomicBool::new(false),
			waker: Mutex::new(None),
		}
	}

	fn register(&self, waker: &Waker) {
		*self.waker.lock().unwrap() = Some(waker.clone());
	}

	/// Wakes a parked taker, if any. The waker is taken under the lock and
	/// raised outside it: a wake may poll a future that registers here again.
	fn wake(&self) {
		let waker = self.waker.lock().unwrap().take();
		if let Some(waker) = waker {
			waker.wake();
		}
	}
}

/// The future [`Handle::take`] returns.
struct Take<S> {
	/// `None` once it resolved, so its drop has nothing to release.
	value: Option<Arc<S>>,
	/// Dropped after `value`: the taker's own hold goes first, then the wake.
	release: Release,
	/// Whether this take marked the slot: the second one does not.
	claimed: bool,
}

impl<S> Take<S> {
	fn new(value: Arc<S>, release: Release) -> Take<S> {
		Self {
			value: Some(value),
			release,
			claimed: false,
		}
	}
}

impl<S> Future for Take<S> {
	type Output = Option<S>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S>> {
		let this = self.get_mut();
		let value = this
			.value
			.take()
			.expect("a take future is not polled after it resolved");
		if !this.claimed {
			if this
				.release
				.waker
				.taken
				.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
				.is_err()
			{
				// Another take has it: this clone gives up, and its own
				// release may be the one that other take is waiting for.
				drop(value);
				this.release.wake();
				return Poll::Ready(None);
			}
			this.claimed = true;
		}
		// Register before the try: a release that follows raises this waker,
		// and one already past is what the try finds.
		this.release.waker.register(cx.waker());
		match Arc::try_unwrap(value) {
			Ok(value) => Poll::Ready(Some(value)),
			Err(value) => {
				this.value = Some(value);
				Poll::Pending
			}
		}
	}
}

impl<S> Drop for Take<S> {
	fn drop(&mut self) {
		// A take that ends, however it ends, leaves the slot for the next one.
		if self.claimed {
			self.release.waker.taken.store(false, Ordering::Release);
		}
	}
}

/// The submit side of a [`Reactor`].
///
/// An operation names its resource as a [`Handle`]: the operation keeps its own
/// hold on the handle until it settles, so the descriptor stays open however the
/// caller treats its own — a number the caller closed and the kernel handed to
/// another file can never be what an operation reads or writes. A caller that
/// holds a plain number has to own a descriptor first:
/// [`BorrowedDescriptor`](crate::BorrowedDescriptor)'s `try_clone_to_owned`
/// duplicates one into an [`OwnedDescriptor`](crate::OwnedDescriptor), and a
/// resource comes to a facade through [`Facade::new`](crate::Facade::new).
///
/// A transfer is one kernel call: it completes with what that call moved,
/// possibly less than the extents name, and `0` at the end of a file. The
/// backend does not resume it; filling a range takes a loop, which the
/// facade's `*_exact` and `*_all` methods are.
#[derive(Clone)]
pub struct Submitter {
	shared: Arc<Shared>,
}

impl Submitter {
	/// The reactor's wake-up, for whoever has no waker of its own to raise —
	/// an [`Event`] with several waiters. Raising it ends a blocked
	/// [`Reactor::poll`].
	pub fn event(&self) -> Event {
		self.shared.event.clone()
	}

	/// This reactor's fallback drops; see [`Reactor::fallback_drops`].
	pub fn fallback_drops(&self) -> u64 {
		self.shared.fallback_drops.load(Ordering::Relaxed)
	}

	/// Wakes a reactor that is waiting, and nothing else.
	///
	/// The wake-up is a counter: raising it while nobody waits leaves it set,
	/// and the next wait resolves at once — a device that waits on the same
	/// wake-up then polls its own submissions in a loop instead of sleeping.
	/// A submission the reactor is about to take needs no raise:
	/// [`Reactor::poll`] drains the queue before it parks.
	#[doc(hidden)]
	pub fn wake(&self) {
		if self.shared.parked.load(Ordering::Acquire) {
			let _ = self.shared.event.notify();
		}
	}

	/// Reads the ranges of `memory` named by `extents`, from `fdoff`, into the
	/// file `fd` names, in one call.
	#[track_caller]
	pub fn read<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			Request::Read {
				fd: raw,
				fdoff,
				memory,
				extents: extents.into_iter().collect(),
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Writes the ranges of `memory` named by `extents`, from `fdoff`, out of
	/// the file `fd` names.
	#[track_caller]
	pub fn write<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			Request::Write {
				fd: raw,
				fdoff,
				memory,
				extents: extents.into_iter().collect(),
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Makes everything written to `fd` durable.
	#[track_caller]
	pub fn fsync<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(Request::Fsync { fd: raw }, Some(Descriptor::new(fd)))
	}

	/// Reports the deferred write-back errors of `fd` without forcing it to
	/// disk.
	#[track_caller]
	pub fn flush<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(Request::Flush { fd: raw }, Some(Descriptor::new(fd)))
	}

	/// Receives into the ranges of `memory` named by `extents`.
	#[track_caller]
	pub(super) fn recv<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			Request::Recv {
				fd: raw,
				memory,
				extents: extents.into_iter().collect(),
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Receives into the ranges of `memory` named by `extents`, taking the
	/// descriptors a control message carries with them.
	#[track_caller]
	pub(super) fn recv_with_fds<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			Request::RecvWithFds {
				fd: raw,
				memory,
				extents: extents.into_iter().collect(),
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Sends the ranges of `memory` named by `extents`.
	#[track_caller]
	pub(super) fn send<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			Request::Send {
				fd: raw,
				memory,
				extents: extents.into_iter().collect(),
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Sends the ranges of `memory` named by `extents`, carrying `fds` with the
	/// bytes. The descriptors travel with the bytes that leave first, which is
	/// the whole call: nothing resumes it.
	#[track_caller]
	pub(super) fn send_with_fds<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			Request::SendWithFds {
				fd: raw,
				memory,
				extents: extents.into_iter().collect(),
				fds,
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Accepts a connection on the listening socket `fd`: the response owns the
	/// socket the kernel reports, and closing it is what dropping the response
	/// does.
	#[track_caller]
	pub(super) fn accept<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(Request::Accept { fd: raw }, Some(Descriptor::new(fd)))
	}

	/// Waits for `fd` to become readable.
	#[track_caller]
	pub(super) fn wait_readable<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(Request::Readable { fd: raw }, Some(Descriptor::new(fd)))
	}

	/// Waits for `fd` to become writable.
	#[track_caller]
	pub(super) fn wait_writable<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(Request::Writable { fd: raw }, Some(Descriptor::new(fd)))
	}

	/// Waits for this reactor's own wake-up (`Submitter::event`), which a
	/// channel send or a `notify` raises.
	#[track_caller]
	pub fn wait(&self) -> io::Result<Completion> {
		if !cfg!(unix) {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"this event cannot be waited on",
			));
		}
		self.enqueue(
			Request::Readable {
				fd: self.shared.event.handle(),
			},
			None,
		)
	}

	/// Completes once `duration` has passed on the reactor's clock.
	///
	/// No backend sees this: the timer is an entry in the reactor's own table
	/// whose deadline a wait is capped at, and the reactor settles it there.
	#[track_caller]
	pub fn timeout(&self, duration: Duration) -> io::Result<Completion> {
		self.arm(Instant::now() + duration)
	}

	/// Registers a timer due at `deadline`; the entry `enqueue` makes, without
	/// the request a backend would need.
	#[track_caller]
	fn arm(&self, deadline: Instant) -> io::Result<Completion> {
		let id = {
			// The check and the insert are one lock hold; see [`Reactor`]'s drop.
			let mut events = self.shared.events.lock().unwrap();
			if self.shared.stopped.load(Ordering::Acquire) {
				return Err(stopped());
			}
			let id = self.shared.next.fetch_add(1, Ordering::Relaxed) + 1;
			events.insert(
				id,
				Entry {
					request: None,
					response: None,
					waker: None,
					stat: Stat::Pending,
					cancel: false,
					names_memory: false,
					kind: "timeout",
					origin: Location::caller(),
					waiter: false,
					deadline: Some(deadline),
					_descriptor: None,
				},
			);
			id
		};
		self.wake();
		Ok(Completion {
			guard: SlotGuard {
				id,
				shared: Arc::clone(&self.shared),
			},
			userdata: Some(()),
		})
	}

	/// Wake and stop the reactor: no further submission is taken — each is
	/// refused with [`BrokenPipe`](io::ErrorKind::BrokenPipe) — while the
	/// operations already in flight are left to land. Dropping the reactor
	/// stops it the same way.
	pub fn stop(&self) {
		self.shared.stopped.store(true, Ordering::Release);
		let _ = self.shared.event.notify();
	}

	/// Registers `request` under a fresh id and returns the completion that
	/// awaits it, with `descriptor` held for as long as the operation lives.
	#[track_caller]
	fn enqueue(&self, request: Request, descriptor: Option<Descriptor>) -> io::Result<Completion> {
		let names_memory = request.names_memory();
		let kind = request.kind();
		let id = {
			// The check and the insert are one lock hold; see [`Reactor`]'s drop.
			let mut events = self.shared.events.lock().unwrap();
			if self.shared.stopped.load(Ordering::Acquire) {
				return Err(stopped());
			}
			let id = self.shared.next.fetch_add(1, Ordering::Relaxed) + 1;
			events.insert(
				id,
				Entry {
					request: Some(request),
					response: None,
					waker: None,
					stat: Stat::Pending,
					cancel: false,
					names_memory,
					kind,
					origin: Location::caller(),
					waiter: false,
					deadline: None,
					_descriptor: descriptor,
				},
			);
			id
		};
		self.wake();
		Ok(Completion {
			guard: SlotGuard {
				id,
				shared: Arc::clone(&self.shared),
			},
			userdata: Some(()),
		})
	}

	/// A channel bound to this reactor's wake-up.
	///
	/// The send end queues and raises the wake-up where a receiver waits;
	/// the receive end is a `Future` resolving to one value. Both ends are
	/// the caller's, and they keep working after this handle goes.
	pub fn channel<T>(&self) -> (channel::Sender<T>, channel::Receiver<T>) {
		channel::Shared::split(self.shared.event.clone())
	}
}

/// The cancellation primitive.
///
/// Cloning gives another handle on the same `Cancel`; [`Cancel::child`] gives one
/// that the parent cancels too. The scope and the shape of the tree are the
/// caller's: the library only hands out the primitive.
///
/// A [`Cancel`] carries the wake-ups of the reactors its operations were
/// submitted to and raises them when it fires: a [`Submitter`] is `Send`, so
/// the reactor that has to carry the cancel to the backend is not always the
/// one the waiter's own waker reaches.
#[derive(Clone)]
pub struct Cancel {
	inner: Arc<CancelInner>,
}

struct CancelInner {
	cancelled: AtomicBool,
	/// Tasks waiting for the cancel, keyed by their registration and taken and
	/// woken when it fires. A waiter re-registers in place on every poll, so the
	/// map only holds the live ones; see `Wait`.
	wakers: Mutex<HashMap<u64, Waker>>,
	/// The key the next [`Wait`] registers under.
	next: AtomicU64,
	/// Reactor wake-ups to raise, so a thread parked in `Reactor::poll` returns.
	/// Deduplicated: a cancel waited on from many operations of one reactor
	/// keeps that reactor's single wake-up, and the list only grows with the
	/// reactors the cancel spans.
	events: Mutex<Vec<Event>>,
	/// Signals that fire when this one does. Held weakly: a child is owned by
	/// whoever asked for it, and a parent that kept it alive would leak one
	/// per driver reset.
	children: Mutex<Vec<Weak<CancelInner>>>,
}

impl CancelInner {
	fn new() -> CancelInner {
		Self {
			cancelled: AtomicBool::new(false),
			wakers: Mutex::new(HashMap::new()),
			next: AtomicU64::new(0),
			events: Mutex::new(Vec::new()),
			children: Mutex::new(Vec::new()),
		}
	}

	fn fire(&self) {
		if self.cancelled.swap(true, Ordering::AcqRel) {
			return;
		}
		let wakers = std::mem::take(&mut *self.wakers.lock().unwrap());
		let events = std::mem::take(&mut *self.events.lock().unwrap());
		let children = std::mem::take(&mut *self.children.lock().unwrap());
		for (_, waker) in wakers {
			waker.wake();
		}
		for event in &events {
			let _ = event.notify();
		}
		for child in &children {
			if let Some(child) = child.upgrade() {
				child.fire();
			}
		}
	}
}

impl Cancel {
	/// A fresh cancel, unraised.
	pub fn new() -> Cancel {
		Self {
			inner: Arc::new(CancelInner::new()),
		}
	}

	/// Raises the cancel. Idempotent.
	pub fn cancel(&self) {
		self.inner.fire();
	}

	/// Whether the cancel has fired.
	pub fn is_cancelled(&self) -> bool {
		self.inner.cancelled.load(Ordering::Acquire)
	}

	/// A cancel that fires when this one does; cancelling the child leaves the
	/// parent alone. A child of an already-raised `Cancel` starts out raised.
	pub fn child(&self) -> Cancel {
		let child = Cancel::new();
		if self.is_cancelled() {
			child.cancel();
			return child;
		}
		let mut children = self.inner.children.lock().unwrap();
		// The parent may have fired between the check and the lock; the lock
		// orders this against `fire`, so the check is made again under it.
		if self.is_cancelled() {
			drop(children);
			child.cancel();
			return child;
		}
		// Whatever its holder dropped is gone: the list is the live children
		// only, so a device that makes one per reset does not grow it.
		children.retain(|child| child.strong_count() > 0);
		children.push(Arc::downgrade(&child.inner));
		child
	}

	/// How many children the cancel holds; a test measure of the pruning.
	#[cfg(test)]
	fn children(&self) -> usize {
		self.inner.children.lock().unwrap().len()
	}

	/// A future that resolves once the cancel fires.
	pub fn wait(&self) -> impl Future<Output = ()> {
		Wait::new(Arc::clone(&self.inner))
	}

	/// Registers a reactor wake-up to raise when the cancel fires. The same
	/// reactor registered twice is one entry: [`Event::same`] compares identity,
	/// so the list is bounded by the distinct reactors the cancel's waiters run
	/// on, not by how many times the cancel is waited on.
	fn attach(&self, event: Event) {
		let mut events = self.inner.events.lock().unwrap();
		// A cancel that fired before the attach raises it at once.
		if self.is_cancelled() {
			drop(events);
			let _ = event.notify();
			return;
		}
		if events.iter().any(|held| held.same(&event)) {
			return;
		}
		events.push(event);
	}

	/// How many waiters are registered; a test measure of the deregistration.
	#[cfg(test)]
	fn waiting(&self) -> usize {
		self.inner.wakers.lock().unwrap().len()
	}
}

impl Default for Cancel {
	fn default() -> Cancel {
		Cancel::new()
	}
}

/// The future [`Cancel::wait`] returns.
///
/// It holds a registration in the cancel's waiter map, replaced in place on
/// every poll and removed when the future goes — resolved, dropped, or taken
/// with an `until` that returned first. A cancel outliving millions of waits
/// therefore keeps only the waiters still parked.
struct Wait {
	inner: Arc<CancelInner>,
	key: u64,
	/// Whether the map holds `key`; a fired cancel takes the whole map, so a
	/// later drop has nothing to remove.
	registered: bool,
}

impl Wait {
	fn new(inner: Arc<CancelInner>) -> Wait {
		let key = inner.next.fetch_add(1, Ordering::Relaxed);
		Wait {
			inner,
			key,
			registered: false,
		}
	}
}

impl Future for Wait {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		let this = self.get_mut();
		let mut wakers = this.inner.wakers.lock().unwrap();
		if this.inner.cancelled.load(Ordering::Acquire) {
			// The cancel has fired; nothing to register, and nothing to remove.
			this.registered = false;
			return Poll::Ready(());
		}
		wakers.insert(this.key, cx.waker().clone());
		this.registered = true;
		Poll::Pending
	}
}

impl Drop for Wait {
	fn drop(&mut self) {
		if self.registered {
			self.inner.wakers.lock().unwrap().remove(&self.key);
		}
	}
}

/// Gives up the entry when the completion goes without being awaited: the
/// result then lands with nobody to take it, and the backend's operation is
/// cancelled.
///
/// It is a field rather than a [`Drop`] impl on [`Completion`] so that a
/// completion is not `Drop`: dropping one still releases the entry, while the
/// userdata can be moved out by [`Completion::tag`].
///
/// An operation still in flight that names the caller's memory is a bug: the
/// kernel would go on reading or writing memory the caller may free. The drop
/// stays until the backend has let go — the semantics the caller needs — and
/// reports the bug. A build with debug assertions panics on it too, once the
/// backend is off the memory; a release build only reports.
struct SlotGuard {
	id: u64,
	shared: Arc<Shared>,
}

impl Drop for SlotGuard {
	fn drop(&mut self) {
		let shared = &self.shared;
		let id = self.id;
		// The decision and what it changes are one lock hold. A cross-thread
		// drop that read `Pending` and then released the lock could lose it to a
		// `drain` that marks the entry `InFlight` and pushes it, leaving the
		// request running with nobody waiting for it.
		let abandoned = {
			let mut events = shared.events.lock().unwrap();
			let Some(entry) = events.get(id) else {
				// Settled and taken, or the reactor's teardown removed it.
				return;
			};
			let (stat, names_memory, kind, origin) =
				(entry.stat, entry.names_memory, entry.kind, entry.origin);
			match stat {
				// Nobody has it yet, and nobody will: it never reaches a
				// backend. A landed response goes too — dropping it closes any
				// descriptor it carries.
				Stat::Pending | Stat::Done => {
					events.remove(id);
					return;
				}
				// The backend has it, but it touches no memory of the caller's,
				// so the result is all that is lost. The operation waits for
				// nothing, so it may wait forever: the drop asks for a cancel,
				// and the entry goes — with whatever it holds open — once the
				// backend reports the stopped operation.
				Stat::InFlight if !names_memory => {
					if let Some(entry) = events.get_mut(id) {
						entry.stat = Stat::Abandoned;
						entry.cancel = true;
					}
					drop(events);
					// Nothing else is coming to wake the reactor: the
					// completion that would have is the one given up.
					if shared.parked.load(Ordering::Acquire) {
						let _ = shared.event.notify();
					}
					return;
				}
				// The backend has it and it touches the caller's memory: the
				// drop has to wait for it to let go. The flags are set here,
				// under the same lock, so the reactor sees the ask whichever
				// thread it drains from.
				Stat::InFlight => {
					if let Some(entry) = events.get_mut(id) {
						entry.cancel = true;
						entry.waiter = true;
					}
				}
				Stat::Abandoned => return,
			}
			(kind, origin)
		};
		let (kind, origin) = abandoned;
		shared.fallback_drops.fetch_add(1, Ordering::Relaxed);
		let started = Instant::now();
		wait_released(shared, id);
		shared.events.lock().unwrap().remove(id);
		let elapsed = started.elapsed();
		log::error!(
			id,
			op = kind,
			origin:? = origin,
			elapsed:? = elapsed;
			"a completion naming caller memory was dropped: the op waited out its end"
		);
		// The panic comes after the wait, so what it unwinds through holds no
		// lock and names memory the backend has already let go. A thread
		// already unwinding panics abort the process instead, so it only gets
		// the report.
		#[cfg(debug_assertions)]
		if !thread::panicking() {
			panic!(
				"completion {id} ({kind}) at {origin} was dropped with its operation in \
				 flight, naming caller memory; it was waited out ({elapsed:?})"
			);
		}
	}
}

/// Waits for the backend to let go of `id`, so the caller may free the memory
/// the operation named. The entry is already marked cancelled and waiting.
fn wait_released(shared: &Shared, id: u64) {
	let _ = shared.event.notify();
	if shared.on_reactor_thread() {
		// Nobody else will reap: the reactor is not polling while user code
		// runs on its thread, so the pass is driven here.
		match shared.core.try_lock() {
			Ok(mut core) => {
				let _ = core.drive_until(shared, id);
			}
			Err(TryLockError::WouldBlock) => panic!(
				"a completion was dropped from inside the reactor: the entry \
				 cannot be waited for while its own thread drives the backend"
			),
			Err(TryLockError::Poisoned(_)) => {}
		}
		// The inline pass may have eaten a wake-up the next `block_on` round is
		// about to sleep through.
		let _ = shared.event.notify();
	} else {
		let mut events = shared.events.lock().unwrap();
		while !shared.is_settled_locked(&events, id) {
			events = shared.settled.wait(events).unwrap();
		}
	}
}

impl Shared {
	/// [`Shared::is_settled`], with the events lock already held.
	fn is_settled_locked(&self, events: &Events, id: u64) -> bool {
		match events.get(id) {
			None => true,
			Some(entry) => entry.stat != Stat::InFlight && entry.stat != Stat::Pending,
		}
	}
}

/// The response of an operation cancelled before it reached the backend. A
/// type of its own so that an `Interrupted` the kernel reports is not mistaken
/// for it: that is a failure of the operation, not a cancellation.
#[derive(Debug)]
struct NotSubmitted;

impl std::fmt::Display for NotSubmitted {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("cancelled before submission")
	}
}

impl std::error::Error for NotSubmitted {}

fn not_submitted() -> io::Error {
	io::Error::other(NotSubmitted)
}

/// The error a submission gets once the reactor is stopped — by
/// [`Submitter::stop`], or by the reactor being dropped.
fn stopped() -> io::Error {
	io::Error::new(io::ErrorKind::BrokenPipe, "the reactor has been stopped")
}

fn is_not_submitted(error: &io::Error) -> bool {
	error
		.get_ref()
		.is_some_and(|inner| inner.is::<NotSubmitted>())
}

/// The outcome of a [`Completion::until`] whose cancel fired: the caller's own
/// value. The response of the cancelled operation goes with it, closing any
/// descriptor it carried.
#[derive(Debug)]
pub struct Cancelled<U = ()>(pub U);

/// Waiter for an operation's completion, carrying the caller's own value.
///
/// Resolves to `(userdata, response)`: the response is what the backend
/// produced, passed through as it is, and `U` is a value of the caller's own
/// handed back when the completion lands. The value lives in the future rather
/// than in the entry, which is what lets one reactor serve callers that each
/// carry something different.
pub struct Completion<U = ()> {
	guard: SlotGuard,
	userdata: Option<U>,
}

impl<U> Completion<U> {
	/// Hands back `userdata` with the result, replacing whatever was there.
	///
	/// The tag is what a caller carries to act on the result: a driver keeping
	/// many requests in flight tags each with the guest chain it came from and
	/// reads it back out of the future.
	pub fn tag<V>(self, userdata: V) -> Completion<V> {
		let Completion { guard, .. } = self;
		Completion {
			guard,
			userdata: Some(userdata),
		}
	}

	/// Asks the backend to stop the operation, as [`Completion::until`] does
	/// when its cancel fires.
	fn request_cancel(&self) {
		let shared = &self.guard.shared;
		{
			let mut events = shared.events.lock().unwrap();
			if let Some(entry) = events.get_mut(self.guard.id) {
				entry.cancel = true;
			}
		}
		if shared.parked.load(Ordering::Acquire) {
			let _ = shared.event.notify();
		}
	}

	/// Whether the response is the cancellation this operation was asked for,
	/// rather than something it produced.
	fn was_cancelled(&self, response: &io::Result<Response>) -> bool {
		match response {
			// Anything that landed is the operation's own result, even a short
			// one: bytes already moved must not be thrown away.
			Ok(_) => false,
			Err(error) => is_not_submitted(error) || (self.guard.shared.cancelled)(error),
		}
	}
}

impl<U: Unpin> Completion<U> {
	/// Races the operation against `cancel`, giving the [`Until`] future.
	pub fn until(self, cancel: &Cancel) -> Until<U> {
		cancel.attach(self.guard.shared.event.clone());
		let wait = Wait::new(Arc::clone(&cancel.inner));
		Until {
			completion: self,
			wait,
			requested: false,
		}
	}
}

/// The future [`Completion::until`] returns: the operation raced against a
/// cancel.
///
/// The operation first returns `Ok((userdata, response))`. The cancel first
/// asks the backend to stop and then keeps waiting until it has let go — the
/// future is only `Ready` once the kernel is done with the spans.
///
/// The result is `Err(Cancelled(userdata))` only when the operation was in
/// fact stopped: its response is the cancellation error, and it is dropped,
/// closing any descriptor it carried. An operation that finished anyway — it
/// won the race, or the backend could not stop it — keeps its result and
/// returns `Ok`, even though a cancel was asked for: a receive that already
/// moved bytes has them, and discarding them would lose them.
pub struct Until<U> {
	completion: Completion<U>,
	/// The cancel's waiting side; polled until it fires, then never again.
	wait: Wait,
	/// Whether the cancel fired and the backend was asked to stop.
	requested: bool,
}

impl<U: Unpin> Future for Until<U> {
	type Output = Result<(U, io::Result<Response>), Cancelled<U>>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		// The operation is looked at first: one that is already settled
		// completed first, whatever the cancel has done since.
		match Pin::new(&mut this.completion).poll(cx) {
			Poll::Ready((userdata, response)) => {
				if this.requested && this.completion.was_cancelled(&response) {
					Poll::Ready(Err(Cancelled(userdata)))
				} else {
					Poll::Ready(Ok((userdata, response)))
				}
			}
			Poll::Pending => {
				if !this.requested && Pin::new(&mut this.wait).poll(cx).is_ready() {
					this.requested = true;
					this.completion.request_cancel();
				}
				Poll::Pending
			}
		}
	}
}

impl<U: Unpin> Future for Completion<U> {
	type Output = (U, io::Result<Response>);

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		// Nothing here is self-referential, so the pin is `get_mut`-able and
		// the userdata can be moved out on readiness.
		let this = self.get_mut();
		let ready = {
			let mut events = this.guard.shared.events.lock().unwrap();
			match events.get_mut(this.guard.id) {
				Some(entry) => match entry.response.take() {
					Some(response) => {
						events.remove(this.guard.id);
						Some(response)
					}
					None => {
						entry.waker = Some(cx.waker().clone());
						None
					}
				},
				None => unreachable!("polled after completion"),
			}
		};
		match ready {
			Some(response) => {
				let userdata = this
					.userdata
					.take()
					.expect("a completion is not polled after it resolved");
				Poll::Ready((userdata, response))
			}
			None => Poll::Pending,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A reactor on this target's own backend.
	fn platform() -> (Reactor, Submitter) {
		Reactor::new(Config::default()).unwrap()
	}

	/// Turns the reactor until the raced operation lands.
	///
	/// A turn waits for a completion, with a second as its bound: the operation
	/// is the only one out, so a backend that never lands it fails the test
	/// here instead of hanging it. Polling without waiting would make the round
	/// count a race against how soon the kernel posts the completion.
	fn race_to_completion(reactor: &mut Reactor, submitter: &Submitter, cancel: &Cancel) {
		// A pipe cannot be synced, so the operation lands on an error; that it
		// lands at all is all this helper needs.
		let (reader, _writer) = std::io::pipe().unwrap();
		let completion = submitter.fsync(&Handle::new(reader)).unwrap();
		let mut until = std::pin::pin!(completion.until(cancel));
		let mut cx = Context::from_waker(Waker::noop());
		for _ in 0..8 {
			if until.as_mut().poll(&mut cx).is_ready() {
				return;
			}
			reactor.poll(Some(Duration::from_secs(1))).unwrap();
		}
		panic!("the operation did not complete");
	}

	/// A cancel outlives the operations raced against it without keeping them: a
	/// waiter's registration goes when its `until` lands. Millions of operations
	/// through one per-device cancel would otherwise grow its waiter map without
	/// bound.
	#[test]
	fn a_cancel_keeps_no_waiter_that_finished() {
		let (mut reactor, submitter) = platform();
		let cancel = Cancel::new();
		for _ in 0..64 {
			race_to_completion(&mut reactor, &submitter, &cancel);
		}
		assert_eq!(cancel.waiting(), 0, "a finished waiter was left behind");
	}

	/// A cancel owns the children it made only while their holders keep them:
	/// a device makes one per driver reset, and a parent that outlived them
	/// would hold one per reset.
	#[test]
	fn a_cancel_keeps_no_child_that_was_dropped() {
		let cancel = Cancel::new();
		for _ in 0..64 {
			drop(cancel.child());
		}
		assert!(
			cancel.children() <= 1,
			"the children outlived their holders: {}",
			cancel.children()
		);
	}

	/// A cancel waited on from many operations of one reactor holds that
	/// reactor's wake-up once. The list grows with the reactors the cancel
	/// spans, not with the number of waits — so a later reactor still gets its
	/// wake-up registered instead of falling off a full list.
	#[test]
	fn a_cancel_keeps_one_wake_up_per_reactor() {
		let (_first, first) = platform();
		let (_second, second) = platform();
		let cancel = Cancel::new();
		for _ in 0..16 {
			cancel.attach(first.event());
		}
		cancel.attach(second.event());
		let events = cancel.inner.events.lock().unwrap();
		assert_eq!(events.len(), 2, "one wake-up per reactor, not per attach");
		assert!(
			events.iter().any(|held| held.same(&second.event())),
			"the second reactor was kept"
		);
	}
}
