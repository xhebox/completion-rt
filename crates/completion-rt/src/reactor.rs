//! Core completion engine.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use crate::desc::{AsDescriptor, RawDescriptor};

use crate::backend::{Backend, BackendEnum, Config, Park, Response, create};
use crate::error::{Error, ErrorKind};

mod handle;
mod op;
mod space;
mod timer;
mod token;

pub use handle::Handle;
pub use op::Op;
pub use space::{Lease, Space, Span};
pub use timer::TimerKey;
pub use token::{InFlight, InFlightState, Token};

use handle::Release;
use op::Request;
use space::lease_closed;
use timer::TimerWheel;
use token::GiveUp;

struct Entry {
	request: Option<Request>,
	response: Option<Result<Response, Error>>,
	waker: Option<Waker>,
	state: InFlightState,
	/// The holder went without the result; the entry goes when the backend lets go.
	detached: bool,
	give_up: GiveUp,
	names_memory: bool,
	kind: &'static str,
	submitted: Instant,
	/// The descriptor the operation names, held until the entry goes: the
	/// backend works on the number, which must stay live until then.
	descriptor: Option<Descriptor>,
	/// The scope whose borrowed memory this operation names; see [`Lease`].
	lease: Option<u64>,
	/// The memory the operation names, held until the operation lands: the
	/// backend works on resolved addresses, which must stay valid until then.
	space: Option<Space>,
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
			entry.space = None;
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
			entry.response = Some(Err(Error::from(error)));
			entry.state = InFlightState::Done;
			entry.space = None;
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
							entry.response = Some(Err(Error::cancelled()));
							entry.state = InFlightState::Done;
							entry.space = None;
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
				Request::Read { fd, fdoff, buffers } => self.backend.read(id, fd, fdoff, buffers),
				Request::Write { fd, fdoff, buffers } => self.backend.write(id, fd, fdoff, buffers),
				Request::Fsync { fd } => self.backend.fsync(id, fd),
				Request::Flush { fd } => self.backend.flush(id, fd),
				Request::Allocate { fd, offset, len } => self.backend.allocate(id, fd, offset, len),
				Request::Punch { fd, offset, len } => self.backend.punch(id, fd, offset, len),
				Request::Recv { fd, buffers } => self.backend.recv(id, fd, buffers),
				Request::RecvWithFds { fd, buffers } => self.backend.recv_with_fds(id, fd, buffers),
				Request::Send { fd, buffers } => self.backend.send(id, fd, buffers),
				Request::SendWithFds { fd, buffers, fds } => {
					self.backend.send_with_fds(id, fd, buffers, fds)
				}
				Request::Accept { fd } => self.backend.accept(id, fd),
				Request::Readable { fd } => self.backend.readable(id, fd),
				Request::Writable { fd } => self.backend.writable(id, fd),
				Request::Hangup { fd } => self.backend.hangup(id, fd),
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
	park: Arc<Park>,
	/// Set once: refusing new submissions and timers.
	stopped: Cell<bool>,
	/// Set once teardown has run, so [`Reactor::shutdown`] and the last drop run it once.
	torn_down: Cell<bool>,
	wheel: TimerWheel,
	core: RefCell<Core>,
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
	pub fn new(config: Config) -> Result<Reactor, Error> {
		Reactor::build(config)
	}

	fn build(config: Config) -> Result<Reactor, Error> {
		let backend = create(&config)?;
		let park = Arc::new(Park::new(backend.doorbell()));
		let wheel = TimerWheel::new(Arc::clone(&park));
		Ok(Reactor {
			shared: Rc::new(Shared {
				next: Cell::new(0),
				events: RefCell::new(Events::new()),
				park,
				stopped: Cell::new(false),
				torn_down: Cell::new(false),
				wheel,
				core: RefCell::new(Core::new(backend)),
			}),
		})
	}

	/// A [`Waker`] that wakes a blocked [`Reactor::poll`] from any thread.
	pub fn waker(&self) -> Waker {
		Waker::from(Arc::clone(&self.shared.park))
	}

	/// Drives the backend.
	pub fn poll(&self, timeout: Option<Duration>) -> Result<(), Error> {
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
			return Ok(core.reap(shared)?);
		}
		core.drain(shared);
		Ok(shared.park.park(|| core.wait(shared, timeout))?)
	}

	/// Arms a timer that fires at `deadline`. It fires from this reactor's
	/// polls, and fails once the reactor stops.
	pub fn insert_timer(&self, deadline: Instant) -> Result<TimerKey, Error> {
		if self.shared.stopped.get() {
			return Err(Error::stopped());
		}
		Ok(self.shared.wheel.insert(deadline))
	}

	/// Polls the timer `key`: `Ready` once it has fired, otherwise `Pending`
	/// with `cx`'s waker recorded for the reactor to wake.
	pub fn poll_timer(&self, key: TimerKey, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
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
				let response = if asked {
					Error::cancelled()
				} else {
					Error::stopped()
				};
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
				Some(Err(error)) => error.kind() == ErrorKind::Cancelled,
				Some(Ok(_)) | None => false,
			};
			if cut {
				land(entry, Err(Error::stopped()), &mut wakers);
			}
		}
	}
	for waker in wakers {
		waker.wake();
	}
}

fn land(entry: &mut Entry, response: Result<Response, Error>, wakers: &mut Vec<Waker>) {
	entry.response = Some(response);
	entry.state = InFlightState::Done;
	entry.space = None;
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

impl Reactor {
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

	/// Submits `op` on `fd`, holding `fd`'s resource until the operation's entry goes.
	///
	/// A transfer's extents are resolved here: one outside its space fails the
	/// submission with [`InvalidInput`](io::ErrorKind::InvalidInput), as does a
	/// space bound or lent on another reactor.
	pub fn submit<S: AsDescriptor + Send + Sync + 'static>(
		&self,
		fd: &Handle<S>,
		op: Op,
	) -> Result<Token, Error> {
		let space = op.space().cloned();
		if let Some(space) = &space {
			self.admit(space)?;
		}
		let request = op.into_request(fd.raw())?;
		self.enqueue(request, Some(Descriptor::new(fd)), space)
	}

	fn enqueue(
		&self,
		request: Request,
		descriptor: Option<Descriptor>,
		space: Option<Space>,
	) -> Result<Token, Error> {
		let names_memory = request.names_memory();
		let kind = request.kind();
		let id = {
			let mut events = self.shared.events.borrow_mut();
			if self.shared.stopped.get() {
				return Err(Error::stopped());
			}
			// Checked under the events borrow: `settle` closes the lease under it too,
			// so no entry can slip in after the barrier has counted.
			if space.as_ref().is_some_and(Space::closed) {
				return Err(lease_closed());
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
					submitted: Instant::now(),
					descriptor,
					lease: space.as_ref().and_then(Space::lease),
					space,
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
		let (state, detached, names_memory, kind, give_up) = (
			entry.state,
			entry.detached,
			entry.names_memory,
			entry.kind,
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
		(kind, give_up)
	};
	let (kind, give_up) = abandoned;
	let started = Instant::now();
	wait_released(shared, id);
	shared.events.borrow_mut().remove(id);
	let elapsed = started.elapsed();
	if give_up.asked() {
		log::debug!(
			id,
			op = kind,
			elapsed:? = elapsed;
			"a completion naming caller memory was dropped: the op waited out its end"
		);
		return;
	}
	log::error!(
		id,
		op = kind,
		elapsed:? = elapsed;
		"a completion naming caller memory was dropped: the op waited out its end"
	);
	// Panic only after releasing kernel access and locks; never double-panic during unwinding.
	#[cfg(debug_assertions)]
	if !thread::panicking() {
		panic!(
			"completion {id} ({kind}) was dropped with its operation in \
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
