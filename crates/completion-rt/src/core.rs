//! Core completion engine.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::ops::Deref;
use std::panic::Location;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use crate::desc::{BorrowedDescriptor, RawDescriptor, RawHandle};
use crate::memory::Extents;
use crate::{AsDescriptor, Memory, OwnedDescriptor, OwnedSocket};

use crate::backend::{self, Backend, BackendEnum, Config, create};
use crate::event::Event;
use crate::park::Park;
use crate::timer::{TimerKey, TimerWheel};

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
	Allocate {
		fd: RawHandle,
		offset: u64,
		len: u64,
	},
	Punch {
		fd: RawHandle,
		offset: u64,
		len: u64,
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
			| Request::Allocate { .. }
			| Request::Punch { .. }
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
			Request::Allocate { .. } => "allocate",
			Request::Punch { .. } => "punch",
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

/// An operation named by its arguments; [`Reactor::submit`] supplies the descriptor from its handle.
pub enum Op {
	Read {
		offset: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Write {
		offset: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	},
	Fsync,
	Flush,
	Allocate {
		offset: u64,
		len: u64,
	},
	Punch {
		offset: u64,
		len: u64,
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
	SendWithFds {
		memory: Arc<dyn Memory>,
		extents: Extents,
		fds: Vec<OwnedDescriptor>,
	},
	Accept,
	Readable,
	Writable,
}

impl Op {
	fn into_request(self, fd: RawHandle) -> Request {
		match self {
			Op::Read {
				offset,
				memory,
				extents,
			} => Request::Read {
				fd,
				fdoff: offset,
				memory,
				extents,
			},
			Op::Write {
				offset,
				memory,
				extents,
			} => Request::Write {
				fd,
				fdoff: offset,
				memory,
				extents,
			},
			Op::Fsync => Request::Fsync { fd },
			Op::Flush => Request::Flush { fd },
			Op::Allocate { offset, len } => Request::Allocate { fd, offset, len },
			Op::Punch { offset, len } => Request::Punch { fd, offset, len },
			Op::Recv { memory, extents } => Request::Recv {
				fd,
				memory,
				extents,
			},
			Op::RecvWithFds { memory, extents } => Request::RecvWithFds {
				fd,
				memory,
				extents,
			},
			Op::Send { memory, extents } => Request::Send {
				fd,
				memory,
				extents,
			},
			Op::SendWithFds {
				memory,
				extents,
				fds,
			} => Request::SendWithFds {
				fd,
				memory,
				extents,
				fds,
			},
			Op::Accept => Request::Accept { fd },
			Op::Readable => Request::Readable { fd },
			Op::Writable => Request::Writable { fd },
		}
	}
}

/// One operation submitted to a [`Reactor`], held until its result is taken.
///
/// Dropping it before then settles the operation: one still in the kernel that
/// names caller memory is waited out before the drop returns. Unless
/// [`Token::cancel`] came first, that wait is a bug: it is logged as an error and
/// panics in a debug build. Forgetting a token leaks its entry, never the memory.
pub struct Token {
	shared: Rc<Shared>,
	id: u64,
	/// Set once the result is taken; the entry is gone and the drop has nothing to settle.
	spent: bool,
}

impl Token {
	/// The result if it has landed, which spends the token; otherwise `waker`
	/// is woken when it lands.
	pub fn poll(&mut self, waker: &Waker) -> Poll<io::Result<Response>> {
		assert!(
			!self.spent,
			"a token is not polled after its result was taken"
		);
		let mut events = self.shared.events.borrow_mut();
		let entry = events
			.get_mut(self.id)
			.expect("an entry lives as long as its token");
		match entry.response.take() {
			Some(response) => {
				events.remove(self.id);
				self.spent = true;
				Poll::Ready(response)
			}
			None => {
				if !entry.waker.as_ref().is_some_and(|w| w.will_wake(waker)) {
					entry.waker = Some(waker.clone());
				}
				Poll::Pending
			}
		}
	}

	/// Asks the operation to give up, without waiting for it. The result still
	/// lands; [`Reactor::is_cancelled`] tells a cancel from a failure.
	pub fn cancel(&self) {
		if let Some(entry) = self.shared.events.borrow_mut().get_mut(self.id) {
			entry.give_up.ask();
		}
		self.shared.park.unpark();
	}
}

impl Drop for Token {
	fn drop(&mut self) {
		if !self.spent {
			release(&self.shared, self.id);
		}
	}
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
	state: InFlightState,
	/// The holder went without the result; the entry goes when the backend lets go.
	detached: bool,
	give_up: GiveUp,
	names_memory: bool,
	kind: &'static str,
	origin: &'static Location<'static>,
	submitted: Instant,
	/// The descriptor the operation names, held until the entry goes: the
	/// backend works on the number, which must stay live until then.
	descriptor: Option<Descriptor>,
	/// The scope whose borrowed memory this operation names; see [`Lease`].
	lease: Option<u64>,
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
		let within = shared.wheel.within(timeout);
		match self.backend.poll(within, &shared.park) {
			Ok(id) => self.settle(shared, id),
			Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
			Err(error) => return Err(error),
		}
		self.settle_timers(shared);
		self.reap(shared)
	}

	fn settle_timers(&mut self, shared: &Shared) {
		shared.wheel.expire(Instant::now());
	}

	fn reap(&mut self, shared: &Shared) -> io::Result<()> {
		loop {
			match self.backend.poll(Some(Duration::ZERO), &shared.park) {
				Ok(id) => self.settle(shared, id),
				Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
				Err(error) => return Err(error),
			}
		}
	}

	fn settle(&mut self, shared: &Shared, id: u64) {
		let response = self.backend.take(id);
		let waker = {
			let mut events = shared.events.borrow_mut();
			let Some(entry) = events.get_mut(id) else {
				return;
			};
			if entry.detached {
				events.remove(id);
				return;
			}
			entry.response = Some(response);
			entry.state = InFlightState::Done;
			entry.waker.take()
		};
		if let Some(waker) = waker {
			waker.wake();
		}
	}

	fn fail(&mut self, shared: &Shared, id: u64, error: io::Error) {
		let waker = {
			let mut events = shared.events.borrow_mut();
			let Some(entry) = events.get_mut(id) else {
				return;
			};
			if entry.detached {
				events.remove(id);
				return;
			}
			entry.response = Some(Err(error));
			entry.state = InFlightState::Done;
			entry.waker.take()
		};
		if let Some(waker) = waker {
			waker.wake();
		}
	}

	fn drain(&mut self, shared: &Shared) {
		let mut wakers = Vec::new();
		let (pending, cancels) = {
			let mut events = shared.events.borrow_mut();
			let mut pending = Vec::new();
			let mut cancels = Vec::new();
			for (id, entry) in events.entries.iter_mut() {
				if entry.give_up == GiveUp::Asked {
					entry.give_up.handled();
					match entry.state {
						InFlightState::Pending => {
							entry.request = None;
							entry.response = Some(Err(not_submitted()));
							entry.state = InFlightState::Done;
							if let Some(waker) = entry.waker.take() {
								wakers.push(waker);
							}
						}
						InFlightState::InFlight => cancels.push(*id),
						InFlightState::Done => {}
					}
					continue;
				}
				if entry.state == InFlightState::Pending {
					if let Some(request) = entry.request.take() {
						entry.state = InFlightState::InFlight;
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
				Request::Allocate { fd, offset, len } => self.backend.allocate(id, fd, offset, len),
				Request::Punch { fd, offset, len } => self.backend.punch(id, fd, offset, len),
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
			// Cancellation may have raced submission while the events borrow was released.
			let stop = {
				let mut events = shared.events.borrow_mut();
				match events.get_mut(id) {
					Some(entry) => {
						let asked = entry.give_up.asked();
						entry.give_up.handled();
						asked || entry.detached
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
}

impl Events {
	fn new() -> Events {
		Events {
			entries: HashMap::new(),
		}
	}

	fn get(&self, id: u64) -> Option<&Entry> {
		self.entries.get(&id)
	}

	fn get_mut(&mut self, id: u64) -> Option<&mut Entry> {
		self.entries.get_mut(&id)
	}

	fn insert(&mut self, id: u64, entry: Entry) {
		self.entries.insert(id, entry);
	}

	fn remove(&mut self, id: u64) -> Option<Entry> {
		self.entries.remove(&id)
	}
}

struct Shared {
	next: Cell<u64>,
	/// Borrowed only briefly: a waker may re-enter the operation table, so
	/// wakes happen after the borrow ends.
	events: RefCell<Events>,
	event: Event,
	park: Arc<Park>,
	/// Set once: refusing new submissions and timers.
	stopped: Cell<bool>,
	/// Set once teardown has run, so [`Reactor::shutdown`] and the last drop run it once.
	torn_down: Cell<bool>,
	wheel: TimerWheel,
	core: RefCell<Core>,
	cancelled: fn(&io::Error) -> bool,
	poll_fd: io::Result<Event>,
	/// Whether the reactor was built for an external poller; only then does
	/// [`Reactor::poll_fd`] hand out a descriptor.
	embedded: bool,
}

impl Shared {
	fn mint(&self) -> u64 {
		let id = self.next.get() + 1;
		self.next.set(id);
		id
	}

	fn is_settled(&self, id: u64) -> bool {
		match self.events.borrow().get(id) {
			None => true,
			Some(entry) => entry.state == InFlightState::Done,
		}
	}

	fn is_idle(&self) -> bool {
		!self
			.events
			.borrow()
			.entries
			.values()
			.any(|entry| entry.state != InFlightState::Done)
	}
}

/// Wakes a blocked [`Reactor::poll`]: the wake is the park's own.
impl Wake for Park {
	fn wake(self: Arc<Self>) {
		self.unpark();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		self.unpark();
	}
}

/// The completion backend, the operations in flight, and the timers among them.
///
/// A handle: every clone drives and submits through the same reactor. Transfers
/// may complete short; use the facade's `*_exact` and `*_all` methods to fill a
/// range.
#[derive(Clone)]
pub struct Reactor {
	shared: Rc<Shared>,
}

impl Reactor {
	/// A reactor that parks its own thread in the backend: [`Reactor::poll`] blocks.
	pub fn new(config: Config) -> io::Result<Reactor> {
		Reactor::build(config, false)
	}

	/// A reactor for an external poller: it never parks, and the caller waits on
	/// [`Reactor::poll_fd`] and calls [`Reactor::poll`] itself.
	pub fn embedded(config: Config) -> io::Result<Reactor> {
		Reactor::build(config, true)
	}

	fn build(config: Config, embedded: bool) -> io::Result<Reactor> {
		config.validate(embedded)?;
		let (backend, event) = create(&config, embedded)?;
		// Cache this before polling: a blocked poll holds the core borrow.
		let poll_fd = backend.poll_fd();
		let park = Arc::new(Park::new(event.clone(), embedded));
		let wheel = TimerWheel::new(Arc::clone(&park));
		Ok(Reactor {
			shared: Rc::new(Shared {
				next: Cell::new(0),
				events: RefCell::new(Events::new()),
				event,
				park,
				stopped: Cell::new(false),
				torn_down: Cell::new(false),
				wheel,
				core: RefCell::new(Core::new(backend)),
				cancelled: backend::cancelled(),
				poll_fd,
				embedded,
			}),
		})
	}

	/// A [`Waker`] that wakes a blocked [`Reactor::poll`] from any thread.
	pub fn waker(&self) -> Waker {
		Waker::from(Arc::clone(&self.shared.park))
	}

	/// Drives the backend.
	pub fn poll(&self, timeout: Option<Duration>) -> io::Result<()> {
		let shared = &self.shared;
		let mut core = shared.core.borrow_mut();
		if timeout == Some(Duration::ZERO) {
			// A poll that does not wait never sleeps, so it may take a wake off the
			// state word: a submission leaves one set, and left set it would make the
			// next waiting poll return before sleeping with nothing to settle. That
			// path cannot clear the word itself — a wake raised after its drain must
			// still stop the park — so its compare-exchange is what consumes a wake.
			let _ = shared.park.park(|| Ok(()));
			core.drain(shared);
			core.settle_timers(shared);
			return core.reap(shared);
		}
		core.drain(shared);
		shared.park.park(|| core.wait(shared, timeout))
	}

	/// The descriptor an external poller waits on, for a reactor built with
	/// [`Reactor::embedded`].
	pub fn poll_fd(&self) -> io::Result<BorrowedDescriptor<'_>> {
		let shared = &self.shared;
		// A self-parking reactor writes its doorbell only while PARKED, so an
		// external poller waiting on the fd would miss cross-thread wakes.
		if !shared.embedded {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"a self-parking reactor has no poll_fd; build it with Reactor::embedded",
			));
		}
		match &shared.poll_fd {
			Ok(event) => Ok(event.descriptor()),
			Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
		}
	}

	/// Empties the notification an external poller woke on, for a reactor built
	/// with [`Reactor::embedded`].
	///
	/// Draining before driving is what keeps the level-triggered descriptor
	/// honest: a completion or a wake raised after this re-raises it, so the
	/// poller can wait on it again instead of reading it ready forever.
	pub fn drain_notification(&self) -> io::Result<()> {
		let shared = &self.shared;
		if !shared.embedded {
			return Err(not_embedded());
		}
		shared.core.borrow_mut().backend.drain_notification()
	}

	/// One pass for a reactor built with [`Reactor::embedded`]: hands what is
	/// queued to the backend, settles timers that have come due, and reaps
	/// completions, as a zero-timeout [`Reactor::poll`] does. It does not drain
	/// the notification — call [`Reactor::drain_notification`] first.
	///
	/// Returns the time until the next timer deadline, or `None` if no timer
	/// waits: what an external poller may wait on [`Reactor::poll_fd`] before
	/// this call again.
	pub fn turn(&self) -> io::Result<Option<Duration>> {
		let shared = &self.shared;
		if !shared.embedded {
			return Err(not_embedded());
		}
		let mut core = shared.core.borrow_mut();
		core.drain(shared);
		core.settle_timers(shared);
		core.reap(shared)?;
		Ok(shared.wheel.within(None))
	}

	/// Arms a timer that fires at `deadline`. It fires from this reactor's
	/// polls, and fails once the reactor stops.
	pub fn insert_timer(&self, deadline: Instant) -> io::Result<TimerKey> {
		if self.shared.stopped.get() {
			return Err(stopped());
		}
		Ok(self.shared.wheel.insert(deadline))
	}

	/// Polls the timer `key`: `Ready` once it has fired, otherwise `Pending`
	/// with `cx`'s waker recorded for the reactor to wake.
	pub fn poll_timer(&self, key: TimerKey, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.shared.wheel.poll(key, cx)
	}

	/// Disarms the timer `key`, whether or not it has fired.
	pub fn cancel_timer(&self, key: TimerKey) {
		self.shared.wheel.cancel(key)
	}

	/// Refuses new submissions and returns once the backend holds nothing.
	/// Without `force` the operations already out run to their own end, however
	/// long that takes; with it they are cancelled and land as the stopped error.
	/// Either way a completion that outlives the shutdown still resolves. Calling
	/// it again does nothing.
	///
	/// A backend failure while draining aborts the process: the kernel may still
	/// write into memory whose owner would otherwise be told it is free.
	pub fn shutdown(&self, force: bool) {
		teardown(&self.shared, force);
	}
}

/// The teardown behind [`Reactor::shutdown`] and the last drop of the shared
/// state; only the first call does anything. It ends with the backend holding
/// nothing, and leaves every entry whose token is still held to resolve.
fn teardown(shared: &Shared, force: bool) {
	if shared.torn_down.replace(true) {
		return;
	}
	let (wakers, cut) = {
		let mut events = shared.events.borrow_mut();
		shared.stopped.set(true);
		let mut wakers = Vec::new();
		let mut cut = Vec::new();
		for (id, entry) in events.entries.iter_mut().filter(|_| force) {
			// A cancel its holder asked for before the shutdown keeps its own result.
			let asked = entry.give_up.asked();
			if !asked {
				cut.push(*id);
			}
			entry.give_up.ask();
			if entry.state == InFlightState::Pending {
				entry.request = None;
				let response = if asked { not_submitted() } else { stopped() };
				land(entry, Err(response), &mut wakers);
			}
		}
		(wakers, cut)
	};
	for waker in wakers {
		waker.wake();
	}
	shared.park.unpark();
	shared.wheel.settle_stopped();
	let drained = {
		let mut core = shared.core.borrow_mut();
		core.drive_to_idle(shared)
	};
	if let Err(error) = drained {
		backend_lost(error, "the reactor shut down");
	}
	let mut wakers = Vec::new();
	{
		let mut events = shared.events.borrow_mut();
		for id in cut {
			let Some(entry) = events.get_mut(id) else {
				continue;
			};
			// What the shutdown cut short reads as the shutdown; what landed on
			// its own keeps its result.
			let cut = match &entry.response {
				Some(Err(error)) => is_not_submitted(error) || (shared.cancelled)(error),
				Some(Ok(_)) | None => false,
			};
			if cut {
				land(entry, Err(stopped()), &mut wakers);
			}
		}
	}
	for waker in wakers {
		waker.wake();
	}
}

fn land(entry: &mut Entry, response: io::Result<Response>, wakers: &mut Vec<Waker>) {
	entry.response = Some(response);
	entry.state = InFlightState::Done;
	wakers.extend(entry.waker.take());
}

impl Drop for Shared {
	fn drop(&mut self) {
		teardown(self, true);
	}
}

/// The backend failed while it may still hold caller memory. Returning would
/// tell the memory's owner it is free while the kernel can still write into it.
fn backend_lost(error: io::Error, during: &str) -> ! {
	log::error!(error:%, during; "the backend failed with caller memory possibly still in the kernel");
	std::process::abort();
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
	/// Its holder went without the result; the entry stays until the backend lets go.
	pub detached: bool,
	/// The descriptor the operation names, as a plain number; `None` for a
	/// timer, which names none. The snapshot keeps nothing open.
	pub descriptor: Option<RawDescriptor>,
	/// How long since the operation was submitted.
	pub age: Duration,
	/// Whether cancellation was requested, including requests not yet sent to the backend.
	pub cancel_requested: bool,
	/// Whether the operation reads or writes caller memory.
	pub names_memory: bool,
}

impl InFlight {
	fn of(id: u64, entry: &Entry, now: Instant) -> InFlight {
		Self {
			id,
			op: entry.kind,
			origin: entry.origin,
			state: entry.state,
			detached: entry.detached,
			descriptor: entry.descriptor.as_ref().map(|held| held.raw),
			age: now.saturating_duration_since(entry.submitted),
			cancel_requested: entry.give_up.asked(),
			names_memory: entry.names_memory,
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
	/// The result has landed and waits for its holder.
	Done,
}

/// Groups the operations naming memory a scope borrowed, so the scope can wait
/// until the kernel has let go of all of them before the borrow ends.
pub struct Lease {
	id: u64,
	/// Set by [`Reactor::settle`]; a submission under a closed lease is
	/// refused, so nothing joins after the barrier has started counting.
	closed: Cell<bool>,
	/// The reactor the lease belongs to: only its own reactor is driven by the
	/// barrier, so a submission elsewhere could outlive it.
	shared: Rc<Shared>,
}

impl Reactor {
	/// The event that wakes a blocked [`Reactor::poll`].
	#[doc(hidden)]
	pub fn event(&self) -> Event {
		self.shared.event.clone()
	}

	/// Snapshots all retained operations in submission order. Borrows the operation table; intended for diagnostics, not per-operation polling.
	pub fn in_flight(&self) -> Vec<InFlight> {
		let now = Instant::now();
		let mut held: Vec<InFlight> = {
			let events = self.shared.events.borrow();
			events
				.entries
				.iter()
				.map(|(&id, entry)| InFlight::of(id, entry, now))
				.collect()
		};
		held.sort_by_key(|entry| entry.id);
		held
	}

	/// Refuses new submissions with [`BrokenPipe`](io::ErrorKind::BrokenPipe) and wakes the reactor; existing operations are left to complete.
	pub fn stop(&self) {
		self.shared.stopped.set(true);
		self.shared.park.unpark();
	}

	/// Opens a lease for the operations a scope makes on memory it borrowed.
	/// Submissions under it are refused once it is [`settle`](Reactor::settle)d,
	/// and the scope waits on `settle` before the borrow ends.
	pub fn lease(&self) -> Lease {
		Lease {
			id: self.shared.mint(),
			closed: Cell::new(false),
			shared: Rc::clone(&self.shared),
		}
	}

	/// Closes `lease` to new submissions and asks every operation under it to
	/// give up. Returns whether one is still pending or in the kernel; the caller
	/// drives the reactor and asks again until it is not.
	pub fn settle(&self, lease: &Lease) -> bool {
		let (busy, pending) = {
			let mut events = self.shared.events.borrow_mut();
			lease.closed.set(true);
			let (mut busy, mut pending) = (false, false);
			for entry in events.entries.values_mut() {
				if entry.lease != Some(lease.id) {
					continue;
				}
				if entry.state != InFlightState::Done {
					entry.give_up.ask();
					busy = true;
					pending |= entry.state == InFlightState::Pending;
				}
			}
			(busy, pending)
		};
		// A pending entry is settled by the drain alone, with no completion to wake
		// the wait after it; one in the kernel is what the wait sleeps for.
		if pending {
			self.shared.park.unpark();
		}
		busy
	}

	/// Submits `op` on `fd`, holding `fd`'s resource until the operation's entry goes.
	pub fn submit<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		op: Op,
		origin: &'static Location<'static>,
		lease: Option<&Lease>,
	) -> io::Result<Token> {
		let raw = fd.raw();
		self.enqueue(
			origin,
			op.into_request(raw),
			Some(Descriptor::new(fd)),
			lease,
		)
	}

	/// Submits a wait on this reactor's event; see [`Reactor::event`].
	pub fn wait_event(&self, origin: &'static Location<'static>) -> io::Result<Token> {
		if !cfg!(unix) {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"this event cannot be waited on",
			));
		}
		self.enqueue(
			origin,
			Request::Readable {
				fd: self.shared.event.handle(),
			},
			None,
			None,
		)
	}

	/// Whether `error` reports an operation that was cancelled rather than failed.
	pub fn is_cancelled(&self, error: &io::Error) -> bool {
		is_not_submitted(error) || (self.shared.cancelled)(error)
	}

	/// Facade call sites are captured before polling: `#[track_caller]` cannot cross `Future::poll`.
	fn enqueue(
		&self,
		origin: &'static Location<'static>,
		request: Request,
		descriptor: Option<Descriptor>,
		lease: Option<&Lease>,
	) -> io::Result<Token> {
		let names_memory = request.names_memory();
		let kind = request.kind();
		let id = {
			let mut events = self.shared.events.borrow_mut();
			if self.shared.stopped.get() {
				return Err(stopped());
			}
			// Checked under the events borrow: `settle` closes the lease under it too,
			// so no entry can slip in after the barrier has counted.
			if let Some(lease) = lease {
				if !Rc::ptr_eq(&lease.shared, &self.shared) {
					return Err(io::Error::new(
						io::ErrorKind::InvalidInput,
						"lent memory submitted to a reactor other than its scope's",
					));
				}
				if lease.closed.get() {
					return Err(io::Error::new(
						io::ErrorKind::BrokenPipe,
						"lent memory submitted after its scope ended",
					));
				}
			}
			let id = self.shared.mint();
			events.insert(
				id,
				Entry {
					request: Some(request),
					response: None,
					waker: None,
					state: InFlightState::Pending,
					detached: false,
					give_up: GiveUp::None,
					names_memory,
					kind,
					origin,
					submitted: Instant::now(),
					descriptor,
					lease: lease.map(|lease| lease.id),
				},
			);
			id
		};
		self.shared.park.unpark();
		Ok(Token {
			shared: Rc::clone(&self.shared),
			id,
			spent: false,
		})
	}
}

fn release(shared: &Shared, id: u64) {
	let abandoned = {
		let mut events = shared.events.borrow_mut();
		let Some(entry) = events.get_mut(id) else {
			return;
		};
		let (state, detached, names_memory, kind, origin, give_up) = (
			entry.state,
			entry.detached,
			entry.names_memory,
			entry.kind,
			entry.origin,
			entry.give_up,
		);
		match state {
			InFlightState::Pending | InFlightState::Done => {
				events.remove(id);
				return;
			}
			InFlightState::InFlight if detached => return,
			// Cancel without blocking: memory-free operations may wait indefinitely.
			InFlightState::InFlight if !names_memory => {
				if let Some(entry) = events.get_mut(id) {
					entry.detached = true;
					entry.give_up.ask();
				}
				drop(events);
				shared.park.unpark();
				return;
			}
			InFlightState::InFlight => {
				if let Some(entry) = events.get_mut(id) {
					entry.give_up.ask();
				}
			}
		}
		(kind, origin, give_up)
	};
	let (kind, origin, give_up) = abandoned;
	let started = Instant::now();
	wait_released(shared, id);
	shared.events.borrow_mut().remove(id);
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

fn wait_released(shared: &Shared, id: u64) {
	match shared.core.try_borrow_mut() {
		Ok(mut core) => {
			if let Err(error) = core.drive_until(shared, id) {
				backend_lost(error, "a dropped operation was waited out");
			}
		}
		Err(_) => panic!(
			"a completion was dropped from inside the reactor: the entry \
			 cannot be waited for while its own thread drives the backend"
		),
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

pub(crate) fn stopped() -> io::Error {
	io::Error::new(io::ErrorKind::BrokenPipe, "the reactor has been stopped")
}

fn not_embedded() -> io::Error {
	io::Error::new(
		io::ErrorKind::Unsupported,
		"this is only for a reactor built with Reactor::embedded",
	)
}

fn is_not_submitted(error: &io::Error) -> bool {
	error
		.get_ref()
		.is_some_and(|inner| inner.is::<NotSubmitted>())
}
