//! Core completion engine.

use std::cell::Cell;
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

use crate::desc::{BorrowedDescriptor, RawDescriptor, RawHandle};
use crate::memory::Extents;
use crate::{AsDescriptor, Extent, Memory, OwnedDescriptor, OwnedSocket};

use crate::backend::{self, Backend, BackendEnum, Config, create};
use crate::channel;
use crate::event::Event;

/// Completed operation result.
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
	/// Returns the transferred byte count, closing any received descriptors; other response variants return an error.
	pub fn count(self) -> io::Result<usize> {
		match self {
			Response::Count(count) | Response::CountWithFds(count, _) => Ok(count),
			Response::Accepted(_) => {
				Err(io::Error::other("an accept reported a socket, not a count"))
			}
			Response::Done => Err(io::Error::other("a transfer reported no count")),
		}
	}
}

enum Request {
	Read {
		fd: RawHandle,
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
	SendWithFds {
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
		fds: Vec<OwnedDescriptor>,
	},
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
	Writable {
		fd: RawHandle,
	},
}

impl Request {
	/// Control buffers are backend-owned; only payload buffers count as caller memory.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stat {
	Pending,
	InFlight,
	Done,
	Abandoned,
}

/// Cancellation stays recorded after dispatch so a later drop can distinguish cancellation from an unsignalled abandonment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GiveUp {
	None,
	Asked,
	Handled,
}

impl GiveUp {
	fn asked(self) -> bool {
		self != GiveUp::None
	}

	fn ask(&mut self) {
		if *self == GiveUp::None {
			*self = GiveUp::Asked;
		}
	}

	fn handled(&mut self) {
		if *self == GiveUp::Asked {
			*self = GiveUp::Handled;
		}
	}
}

struct Entry {
	request: Option<Request>,
	response: Option<io::Result<Response>>,
	waker: Option<Waker>,
	stat: Stat,
	give_up: GiveUp,
	names_memory: bool,
	kind: &'static str,
	origin: &'static Location<'static>,
	submitted: Instant,
	waiter: bool,
	deadline: Option<Instant>,
	/// The descriptor the operation names, held until the entry goes: the
	/// backend works on the number, which must stay live until then.
	descriptor: Option<Descriptor>,
}

struct Descriptor {
	raw: RawDescriptor,
	_value: Arc<dyn Send + Sync>,
	/// Drop after `_value` so a woken taker sees the resource released.
	_release: Release,
}

impl Descriptor {
	fn new<S: AsDescriptor + Send + Sync + 'static>(handle: &Handle<S>) -> Descriptor {
		// Bound first: an `Arc::clone` under the erased type would have to infer
		// its own parameter as the trait object.
		let value = Arc::clone(&handle.value);
		Self {
			raw: handle.raw().into_raw(),
			_value: value,
			_release: handle.release.clone(),
		}
	}
}

/// Lock order: `Shared::core` before `Shared::events`.
struct Core {
	backend: BackendEnum,
}

impl Core {
	fn new(backend: BackendEnum) -> Core {
		Self { backend }
	}

	fn wait(&mut self, shared: &Shared, timeout: Option<Duration>) -> io::Result<()> {
		let within = shared.events.lock().unwrap().within(timeout);
		match self.backend.poll(within) {
			Ok(id) => self.settle(shared, id),
			Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
			Err(error) => return Err(error),
		}
		self.settle_timers(shared);
		self.reap(shared)
	}

	fn settle_timers(&mut self, shared: &Shared) {
		let now = Instant::now();
		let mut wakers = Vec::new();
		{
			let mut events = shared.events.lock().unwrap();
			while let Some((&(deadline, id), _)) = events.deadlines.first_key_value() {
				if deadline > now {
					break;
				}
				events.deadlines.remove(&(deadline, id));
				let Some(entry) = events.entries.get_mut(&id) else {
					continue;
				};
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

	fn reap(&mut self, shared: &Shared) -> io::Result<()> {
		loop {
			match self.backend.poll(Some(Duration::ZERO)) {
				Ok(id) => self.settle(shared, id),
				Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
				Err(error) => return Err(error),
			}
		}
	}

	fn settle(&mut self, shared: &Shared, id: u64) {
		let response = self.backend.take(id);
		let (waiter, waker) = {
			let mut events = shared.events.lock().unwrap();
			let Some(entry) = events.get_mut(id) else {
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

	fn drain(&mut self, shared: &Shared) {
		let mut wakers = Vec::new();
		let (pending, cancels) = {
			let mut events = shared.events.lock().unwrap();
			let Events { entries, deadlines } = &mut *events;
			let mut pending = Vec::new();
			let mut cancels = Vec::new();
			for (id, entry) in entries.iter_mut() {
				if entry.give_up == GiveUp::Asked {
					entry.give_up.handled();
					match entry.stat {
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
						// Abandoned operations must still be cancelled and reaped to release their resources.
						Stat::InFlight | Stat::Abandoned => cancels.push(*id),
						_ => {}
					}
					continue;
				}
				if entry.stat == Stat::Pending {
					if let Some(request) = entry.request.take() {
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
			// Cancellation may have raced submission while the events lock was released.
			let stop = {
				let mut events = shared.events.lock().unwrap();
				match events.get_mut(id) {
					Some(entry) => {
						let asked = entry.give_up.asked();
						entry.give_up.handled();
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

struct Events {
	entries: HashMap<u64, Entry>,
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

	fn insert(&mut self, id: u64, entry: Entry) {
		if let Some(deadline) = entry.deadline {
			self.deadlines.insert((deadline, id), ());
		}
		self.entries.insert(id, entry);
	}

	fn remove(&mut self, id: u64) -> Option<Entry> {
		let entry = self.entries.remove(&id)?;
		if let Some(deadline) = entry.deadline {
			self.deadlines.remove(&(deadline, id));
		}
		Some(entry)
	}

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
	/// Wake outside this lock: a waker may re-enter the operation table.
	events: Mutex<Events>,
	event: Event,
	stopped: AtomicBool,
	parked: AtomicBool,
	/// A driver-thread wake must prevent the next poll from sleeping.
	ready: AtomicBool,
	core: Mutex<Core>,
	cancelled: fn(&io::Error) -> bool,
	settled: Condvar,
	reactor_thread: Mutex<Option<ThreadId>>,
	poll_fd: io::Result<Event>,
}

impl Shared {
	fn enter_reactor(&self) {
		*self.reactor_thread.lock().unwrap() = Some(thread::current().id());
	}

	fn on_reactor_thread(&self) -> bool {
		*self.reactor_thread.lock().unwrap() == Some(thread::current().id())
	}

	/// Cross-thread wakes must notify unconditionally to cover the gap between draining and parking.
	fn wake_reactor(&self) {
		if self.on_reactor_thread() {
			self.ready.store(true, Ordering::Release);
		} else {
			let _ = self.event.notify();
		}
	}

	fn is_settled(&self, id: u64) -> bool {
		self.is_settled_locked(&self.events.lock().unwrap(), id)
	}

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

pub(super) struct ReactorWaker {
	shared: Arc<Shared>,
}

impl ReactorWaker {
	fn new(shared: Arc<Shared>) -> ReactorWaker {
		Self { shared }
	}
}

impl Wake for ReactorWaker {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		self.shared.wake_reactor();
	}
}

/// The completion backend, the operations in flight, and the timers among them.
pub struct Reactor {
	shared: Arc<Shared>,
}

impl Reactor {
	pub fn new(config: Config) -> io::Result<(Reactor, Submitter)> {
		config.validate()?;
		let (backend, event) = create(&config)?;
		// Cache this before polling: a blocked poll holds the core lock.
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
		});
		Ok((
			Reactor {
				shared: Arc::clone(&shared),
			},
			Submitter { shared },
		))
	}

	pub(super) fn enter_reactor(&self) {
		self.shared.enter_reactor();
	}

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
		// Mark parked before the second drain so concurrent submissions cannot be missed.
		self.shared.parked.store(true, Ordering::Release);
		core.drain(&self.shared);
		let result = if self.shared.ready.swap(false, Ordering::AcqRel) {
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

	/// Stopped and every in-flight operation landed.
	#[doc(hidden)]
	pub fn is_stopped(&self) -> bool {
		self.shared.stopped.load(Ordering::Acquire) && self.is_idle()
	}
}

/// Cancels and drains outstanding operations before releasing their memory; subsequent submissions fail.
impl Drop for Reactor {
	fn drop(&mut self) {
		let shared = &self.shared;
		{
			let mut events = shared.events.lock().unwrap();
			// Synchronize with insertion so a racing submission is either rejected or included in teardown.
			shared.stopped.store(true, Ordering::Release);
			events
				.entries
				.retain(|_, entry| entry.stat != Stat::Pending);
			events.deadlines.clear();
			for entry in events.entries.values_mut() {
				entry.give_up.ask();
			}
		}
		let _ = shared.event.notify();
		if let Ok(mut core) = shared.core.lock() {
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

/// An owned resource shared by operations; [`Handle::take`] waits for all other holders to release it.
pub struct Handle<S> {
	value: Arc<S>,
	/// Drop after `value` so a woken taker sees the resource released.
	release: Release,
}

impl<S> Handle<S> {
	pub fn new(value: S) -> Handle<S> {
		Self {
			value: Arc::new(value),
			release: Release::new(),
		}
	}

	/// Returns the resource after all other holders release it, or `None` if another take is already waiting.
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

	/// Wake outside the lock: the waker may register here again.
	fn wake(&self) {
		let waker = self.waker.lock().unwrap().take();
		if let Some(waker) = waker {
			waker.wake();
		}
	}
}

struct Take<S> {
	value: Option<Arc<S>>,
	/// Drop after `value` so a woken taker sees the resource released.
	release: Release,
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
				drop(value);
				this.release.wake();
				return Poll::Ready(None);
			}
			this.claimed = true;
		}
		// Register before checking ownership to avoid losing a concurrent release.
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
		if self.claimed {
			self.release.waker.taken.store(false, Ordering::Release);
		}
	}
}

/// A diagnostic snapshot; descriptor numbers are borrowed metadata and keep no resource open.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InFlight {
	/// The id the reactor minted for the operation.
	pub id: u64,
	/// The operation's name: `"read"`, `"send"`, `"timeout"`, and the rest.
	pub op: &'static str,
	/// The call site that created the operation.
	pub origin: &'static Location<'static>,
	/// Where the operation is in its life.
	pub state: InFlightState,
	/// The descriptor the operation names, as a plain number; `None` for a
	/// timer, which names none. The snapshot keeps nothing open.
	pub descriptor: Option<RawDescriptor>,
	/// How long since the operation was submitted.
	pub age: Duration,
	/// Whether cancellation was requested, including requests not yet sent to the backend.
	pub cancel_requested: bool,
	/// Whether the operation reads or writes caller memory.
	pub names_memory: bool,
	/// A timer's deadline; `None` for an operation a backend carries.
	pub deadline: Option<Instant>,
}

impl InFlight {
	fn of(id: u64, entry: &Entry, now: Instant) -> InFlight {
		Self {
			id,
			op: entry.kind,
			origin: entry.origin,
			state: InFlightState::of(entry.stat),
			descriptor: entry.descriptor.as_ref().map(|held| held.raw),
			age: now.saturating_duration_since(entry.submitted),
			cancel_requested: entry.give_up.asked(),
			names_memory: entry.names_memory,
			deadline: entry.deadline,
		}
	}
}

/// Where an operation is in its life, as [`InFlight::state`] reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InFlightState {
	/// Submitted, waiting for the reactor to hand it to a backend.
	Pending,
	/// A backend has it.
	InFlight,
	/// The result has landed and waits for its future.
	Done,
	/// The future went without the result.
	Abandoned,
}

impl InFlightState {
	fn of(stat: Stat) -> InFlightState {
		match stat {
			Stat::Pending => InFlightState::Pending,
			Stat::InFlight => InFlightState::InFlight,
			Stat::Done => InFlightState::Done,
			Stat::Abandoned => InFlightState::Abandoned,
		}
	}
}

/// Submits operations to a [`Reactor`], retaining each resource until completion.
///
/// Transfers may complete short; use the facade's `*_exact` and `*_all` methods to fill a range.
#[derive(Clone)]
pub struct Submitter {
	shared: Arc<Shared>,
}

impl Submitter {
	/// The event that wakes a blocked [`Reactor::poll`].
	pub fn event(&self) -> Event {
		self.shared.event.clone()
	}

	/// Snapshots all retained operations in submission order. Takes the operation-table lock; intended for diagnostics, not per-operation polling.
	pub fn in_flight(&self) -> Vec<InFlight> {
		let now = Instant::now();
		let mut held: Vec<InFlight> = {
			let events = self.shared.events.lock().unwrap();
			events
				.entries
				.iter()
				.map(|(&id, entry)| InFlight::of(id, entry, now))
				.collect()
		};
		held.sort_by_key(|entry| entry.id);
		held
	}

	/// Wakes the reactor if it is parked.
	#[doc(hidden)]
	pub fn wake(&self) {
		if self.shared.parked.load(Ordering::Acquire) {
			let _ = self.shared.event.notify();
		}
	}

	/// Reads from `fd` at `fdoff` into the named ranges of `memory`.
	#[track_caller]
	pub fn read<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		self.read_at(Location::caller(), fd, fdoff, memory, extents)
	}

	pub(super) fn read_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			Request::Read {
				fd: raw,
				fdoff,
				memory,
				extents: extents.into_iter().collect(),
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Writes the named ranges of `memory` to `fd` at `fdoff`.
	#[track_caller]
	pub fn write<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		self.write_at(Location::caller(), fd, fdoff, memory, extents)
	}

	pub(super) fn write_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
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
		self.fsync_at(Location::caller(), fd)
	}

	pub(super) fn fsync_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			Request::Fsync { fd: raw },
			Some(Descriptor::new(fd)),
		)
	}

	/// Reports the deferred write-back errors of `fd` without forcing it to
	/// disk.
	#[track_caller]
	pub fn flush<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		self.flush_at(Location::caller(), fd)
	}

	pub(super) fn flush_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			Request::Flush { fd: raw },
			Some(Descriptor::new(fd)),
		)
	}

	/// Receives into the ranges of `memory` named by `extents`.
	#[track_caller]
	pub fn recv<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		self.recv_at(Location::caller(), fd, memory, extents)
	}

	pub(super) fn recv_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
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
	pub fn recv_with_fds<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		self.recv_with_fds_at(Location::caller(), fd, memory, extents)
	}

	pub(super) fn recv_with_fds_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
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
	pub fn send<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		self.send_at(Location::caller(), fd, memory, extents)
	}

	pub(super) fn send_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			Request::Send {
				fd: raw,
				memory,
				extents: extents.into_iter().collect(),
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Sends the named ranges of `memory` with `fds`; a short send does not retry.
	#[track_caller]
	pub fn send_with_fds<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<Completion> {
		self.send_with_fds_at(Location::caller(), fd, memory, extents, fds)
	}

	pub(super) fn send_with_fds_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
		memory: Arc<dyn Memory>,
		extents: impl IntoIterator<Item = Extent>,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			Request::SendWithFds {
				fd: raw,
				memory,
				extents: extents.into_iter().collect(),
				fds,
			},
			Some(Descriptor::new(fd)),
		)
	}

	/// Accepts a connection, returning ownership of the socket in [`Response::Accepted`].
	#[track_caller]
	pub fn accept<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		self.accept_at(Location::caller(), fd)
	}

	pub(super) fn accept_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			Request::Accept { fd: raw },
			Some(Descriptor::new(fd)),
		)
	}

	/// Waits for `fd` to become readable.
	#[track_caller]
	pub fn wait_readable<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		self.wait_readable_at(Location::caller(), fd)
	}

	pub(super) fn wait_readable_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			Request::Readable { fd: raw },
			Some(Descriptor::new(fd)),
		)
	}

	/// Waits for `fd` to become writable.
	#[track_caller]
	pub fn wait_writable<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		self.wait_writable_at(Location::caller(), fd)
	}

	pub(super) fn wait_writable_at<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		origin: &'static Location<'static>,
		fd: &Handle<S>,
	) -> io::Result<Completion> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			Request::Writable { fd: raw },
			Some(Descriptor::new(fd)),
		)
	}

	/// Waits for this reactor's [`Submitter::event`] to be notified.
	#[track_caller]
	pub fn wait(&self) -> io::Result<Completion> {
		if !cfg!(unix) {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"this event cannot be waited on",
			));
		}
		self.enqueue(
			Location::caller(),
			Request::Readable {
				fd: self.shared.event.handle(),
			},
			None,
		)
	}

	/// Completes after `duration`.
	#[track_caller]
	pub fn timeout(&self, duration: Duration) -> io::Result<Completion> {
		self.arm(Location::caller(), Instant::now() + duration)
	}

	fn arm(&self, origin: &'static Location<'static>, deadline: Instant) -> io::Result<Completion> {
		let id = {
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
					give_up: GiveUp::None,
					names_memory: false,
					kind: "timeout",
					origin,
					submitted: Instant::now(),
					waiter: false,
					deadline: Some(deadline),
					descriptor: None,
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

	/// Refuses new submissions with [`BrokenPipe`](io::ErrorKind::BrokenPipe) and wakes the reactor; existing operations are left to complete.
	pub fn stop(&self) {
		self.shared.stopped.store(true, Ordering::Release);
		let _ = self.shared.event.notify();
	}

	/// Facade call sites are captured before polling: `#[track_caller]` cannot cross `Future::poll`.
	fn enqueue(
		&self,
		origin: &'static Location<'static>,
		request: Request,
		descriptor: Option<Descriptor>,
	) -> io::Result<Completion> {
		let names_memory = request.names_memory();
		let kind = request.kind();
		let id = {
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
					give_up: GiveUp::None,
					names_memory,
					kind,
					origin,
					submitted: Instant::now(),
					waiter: false,
					deadline: None,
					descriptor,
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

	/// Creates a channel bound to this reactor's wake-up.
	pub fn channel<T>(&self) -> (channel::Sender<T>, channel::Receiver<T>) {
		channel::Shared::split(self.shared.event.clone())
	}
}

/// A clonable cancellation signal. Clones share the signal; [`Cancel::child`] creates a signal also cancelled by its parent.
#[derive(Clone)]
pub struct Cancel {
	inner: Arc<CancelInner>,
}

struct CancelInner {
	cancelled: AtomicBool,
	wakers: Mutex<HashMap<u64, Waker>>,
	next: AtomicU64,
	events: Mutex<Vec<Event>>,
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
		// Recheck under the lock to cover cancellation racing child registration.
		if self.is_cancelled() {
			drop(children);
			child.cancel();
			return child;
		}
		children.retain(|child| child.strong_count() > 0);
		children.push(Arc::downgrade(&child.inner));
		child
	}

	/// Resolves once the cancel fires, at once if it already has. Dropping the
	/// future before then takes its waker out of the cancel.
	pub fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
		Wait::new(Arc::clone(&self.inner))
	}

	#[cfg(test)]
	fn children(&self) -> usize {
		self.inner.children.lock().unwrap().len()
	}

	fn attach(&self, event: Event) {
		let mut events = self.inner.events.lock().unwrap();
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

struct Wait {
	inner: Arc<CancelInner>,
	key: u64,
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

thread_local! {
	static TASK_CANCEL: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn cancel_task_future<F>(future: F) {
	struct Restore(bool);

	impl Drop for Restore {
		fn drop(&mut self) {
			TASK_CANCEL.set(self.0);
		}
	}

	// A cancelled task is an explicit give-up, even when its opaque future hides a Completion.
	let _restore = Restore(TASK_CANCEL.replace(true));
	drop(future);
}

struct SlotGuard {
	id: u64,
	shared: Arc<Shared>,
}

impl Drop for SlotGuard {
	fn drop(&mut self) {
		let shared = &self.shared;
		let id = self.id;
		// Keep the state check and transition atomic with submission, or a pending request could become in-flight after the drop decides not to wait.
		let abandoned = {
			let mut events = shared.events.lock().unwrap();
			let Some(entry) = events.get_mut(id) else {
				return;
			};
			if TASK_CANCEL.get() {
				entry.give_up.ask();
			}
			let (stat, names_memory, kind, origin, give_up) = (
				entry.stat,
				entry.names_memory,
				entry.kind,
				entry.origin,
				entry.give_up,
			);
			match stat {
				Stat::Pending | Stat::Done => {
					events.remove(id);
					return;
				}
				// Cancel without blocking: memory-free operations may wait indefinitely.
				Stat::InFlight if !names_memory => {
					if let Some(entry) = events.get_mut(id) {
						entry.stat = Stat::Abandoned;
						entry.give_up.ask();
					}
					drop(events);
					if shared.parked.load(Ordering::Acquire) {
						let _ = shared.event.notify();
					}
					return;
				}
				Stat::InFlight => {
					if let Some(entry) = events.get_mut(id) {
						entry.give_up.ask();
						entry.waiter = true;
					}
				}
				Stat::Abandoned => return,
			}
			(kind, origin, give_up)
		};
		let (kind, origin, give_up) = abandoned;
		let started = Instant::now();
		wait_released(shared, id);
		shared.events.lock().unwrap().remove(id);
		let elapsed = started.elapsed();
		if give_up.asked() {
			log::debug!(
				id,
				op = kind,
				origin:? = origin,
				elapsed:? = elapsed;
				"a completion naming caller memory was dropped: the op waited out its end"
			);
			return;
		}
		log::error!(
			id,
			op = kind,
			origin:? = origin,
			elapsed:? = elapsed;
			"a completion naming caller memory was dropped: the op waited out its end"
		);
		// Panic only after releasing kernel access and locks; never double-panic during unwinding.
		#[cfg(debug_assertions)]
		if !thread::panicking() {
			panic!(
				"completion {id} ({kind}) at {origin} was dropped with its operation in \
				 flight, naming caller memory; it was waited out ({elapsed:?})"
			);
		}
	}
}

fn wait_released(shared: &Shared, id: u64) {
	let _ = shared.event.notify();
	if shared.on_reactor_thread() {
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
		// The inline drive may have consumed a wake needed by the next `block_on` iteration.
		let _ = shared.event.notify();
	} else {
		let mut events = shared.events.lock().unwrap();
		while !shared.is_settled_locked(&events, id) {
			events = shared.settled.wait(events).unwrap();
		}
	}
}

impl Shared {
	fn is_settled_locked(&self, events: &Events, id: u64) -> bool {
		match events.get(id) {
			None => true,
			Some(entry) => entry.stat != Stat::InFlight && entry.stat != Stat::Pending,
		}
	}
}

/// Distinct from kernel `Interrupted` errors, which must not be mistaken for cancellation.
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

fn stopped() -> io::Error {
	io::Error::new(io::ErrorKind::BrokenPipe, "the reactor has been stopped")
}

fn is_not_submitted(error: &io::Error) -> bool {
	error
		.get_ref()
		.is_some_and(|inner| inner.is::<NotSubmitted>())
}

/// Userdata returned when [`Completion::until`] cancels an operation.
#[derive(Debug)]
pub struct Cancelled<U = ()>(pub U);

/// Resolves to `(userdata, response)`.
///
/// Dropping an in-flight operation that accesses caller memory waits for the kernel to release it. An unsignalled drop logs an error and panics in debug builds unless already unwinding; a requested cancellation does not panic.
pub struct Completion<U = ()> {
	guard: SlotGuard,
	userdata: Option<U>,
}

impl<U> Completion<U> {
	/// Replaces the userdata returned alongside the result.
	pub fn tag<V>(self, userdata: V) -> Completion<V> {
		let Completion { guard, .. } = self;
		Completion {
			guard,
			userdata: Some(userdata),
		}
	}

	fn request_cancel(&self) {
		let shared = &self.guard.shared;
		{
			let mut events = shared.events.lock().unwrap();
			if let Some(entry) = events.get_mut(self.guard.id) {
				entry.give_up.ask();
			}
		}
		if shared.parked.load(Ordering::Acquire) {
			let _ = shared.event.notify();
		}
	}

	fn was_cancelled(&self, response: &io::Result<Response>) -> bool {
		match response {
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

/// Waits for completion or cancellation, including release of kernel access to memory.
///
/// Returns `Err(Cancelled(userdata))` only if the operation was actually cancelled; an operation that finishes despite cancellation retains its result.
pub struct Until<U> {
	completion: Completion<U>,
	wait: Wait,
	requested: bool,
}

impl<U: Unpin> Future for Until<U> {
	type Output = Result<(U, io::Result<Response>), Cancelled<U>>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
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

	fn platform() -> (Reactor, Submitter) {
		Reactor::new(Config::default()).unwrap()
	}

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

	/// A long-lived cancel must not retain waiters after their operations finish.
	#[test]
	fn a_cancel_keeps_no_waiter_that_finished() {
		let (mut reactor, submitter) = platform();
		let cancel = Cancel::new();
		for _ in 0..64 {
			race_to_completion(&mut reactor, &submitter, &cancel);
		}
		assert_eq!(cancel.waiting(), 0, "a finished waiter was left behind");
	}

	/// A long-lived parent must not retain dropped children.
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

	/// Reactor registrations must grow with distinct reactors, not repeated waits.
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
