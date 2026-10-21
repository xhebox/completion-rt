use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak as SyncWeak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use slotmap::{DefaultKey, SlotMap};
use smallvec::SmallVec;

use completion_backend::{AsDescriptor, Backend, Config, Error, Op, Response, Result};

use crate::memory::Memory;

const TICK_BUDGET: usize = 64;

// ---------------------------------------------------------------------------
// Thread-local state
// ---------------------------------------------------------------------------

thread_local! {
	static RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

/// The combined I/O driver and task scheduler for one thread.
pub(crate) struct Runtime {
	backend: Backend<Arc<Entry>>,
	shared: Arc<Shared>,
	tasks: SlotMap<DefaultKey, Option<Box<dyn Runnable>>>,
	ready: Arc<Ready>,
}

impl Runtime {
	fn new(config: Config) -> Result<Runtime> {
		let backend = Backend::new(&config)?;
		let driver_waker = backend.waker();
		let shared = Arc::new(Shared {
			inner: Mutex::new(SharedInner {
				stopped: false,
				cancellations: Vec::new(),
				timers: BTreeMap::new(),
				next_timer: 0,
				waker: driver_waker.clone(),
			}),
		});
		let ready = Arc::new(Ready {
			runnable: Mutex::new(VecDeque::new()),
			driver_waker,
			parked: AtomicBool::new(false),
			root_woken: AtomicBool::new(true),
			running: AtomicBool::new(false),
		});
		Ok(Runtime {
			backend,
			shared,
			tasks: SlotMap::new(),
			ready,
		})
	}

	fn drive(&mut self, timeout: Option<Duration>) -> Result<()> {
		for id in self.shared.take_cancellations() {
			self.backend.cancel(id);
		}
		let within = self.shared.within(timeout);
		let polled = self.backend.poll(within, |_id, entry, response| {
			entry.complete(response);
		});
		self.shared.expire_timers();
		polled
	}

	pub(crate) fn submit(
		&mut self,
		fd: Arc<dyn AsDescriptor>,
		op: Op,
		memory: Option<Arc<dyn Memory>>,
	) -> Result<Slot> {
		if self.shared.is_stopped() {
			return Err(Error::stopped());
		}
		let entry = Arc::new(Entry::pending(Arc::clone(&fd), memory));
		let id = self
			.backend
			.submit(fd.as_descriptor(), op, Arc::clone(&entry))?;
		Ok(Slot {
			shared: Arc::clone(&self.shared),
			entry,
			id: Some(id),
		})
	}
}

impl Drop for Runtime {
	fn drop(&mut self) {
		for job in self.tasks.values_mut().filter_map(Option::as_mut) {
			job.abandon();
		}
		if let Some(timers) = self.shared.stop() {
			for waker in timers.into_values() {
				waker.wake();
			}
			self.backend.cancel_all();
			while !self.backend.is_idle() {
				if self.drive(None).is_err() {
					break;
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Short-lived borrows into the thread-local Runtime
// ---------------------------------------------------------------------------

/// Borrows `RT` briefly to run `f`. Panics if no runtime is installed.
fn with_rt<R>(f: impl FnOnce(&mut Runtime) -> R) -> R {
	RT.with(|cell| {
		let mut borrow = cell.borrow_mut();
		let rt = borrow
			.as_mut()
			.expect("no runtime is installed on this thread");
		f(rt)
	})
}

/// Ensures a runtime exists on this thread (lazy default), then borrows it.
pub(crate) fn with_rt_or_default<R>(f: impl FnOnce(&mut Runtime) -> R) -> R {
	RT.with(|cell| {
		if cell.borrow().is_none() {
			let rt = Runtime::new(Config::default())
				.expect("the thread's default runtime could not be created");
			*cell.borrow_mut() = Some(rt);
		}
		f(cell.borrow_mut().as_mut().unwrap())
	})
}

// ---------------------------------------------------------------------------
// JoinHandle / Join
// ---------------------------------------------------------------------------

/// Handle to a spawned thread-local task.
pub struct JoinHandle<T> {
	join: Rc<RefCell<Join<T>>>,
}

impl<T> Future for JoinHandle<T> {
	type Output = T;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
		let mut join = self.join.borrow_mut();
		match &mut *join {
			Join::Waiting(waker) => {
				if waker
					.as_ref()
					.is_none_or(|waker| !waker.will_wake(cx.waker()))
				{
					*waker = Some(cx.waker().clone());
				}
				Poll::Pending
			}
			Join::Done(_) => {
				let Join::Done(output) = std::mem::replace(&mut *join, Join::Taken) else {
					unreachable!("the arm just matched Done")
				};
				Poll::Ready(output)
			}
			Join::Gone => panic!("the runtime was dropped before the task finished"),
			Join::Taken | Join::Detached => {
				panic!("the task result is no longer available")
			}
		}
	}
}

impl<T> Drop for JoinHandle<T> {
	fn drop(&mut self) {
		drop(self.join.replace(Join::Detached));
	}
}

enum Join<T> {
	Detached,
	Waiting(Option<Waker>),
	Done(T),
	Taken,
	Gone,
}

// ---------------------------------------------------------------------------
// Task (Job) internals
// ---------------------------------------------------------------------------

trait Runnable {
	fn poll(&mut self) -> Poll<()>;
	fn abandon(&mut self);
}

struct Job<F: Future> {
	future: Pin<Box<F>>,
	join: Rc<RefCell<Join<F::Output>>>,
	scheduled: Arc<AtomicBool>,
	waker: Waker,
}

impl<F: Future> Runnable for Job<F> {
	fn poll(&mut self) -> Poll<()> {
		self.scheduled.swap(false, Ordering::AcqRel);
		let mut cx = Context::from_waker(&self.waker);
		let output = match self.future.as_mut().poll(&mut cx) {
			Poll::Pending => return Poll::Pending,
			Poll::Ready(output) => output,
		};
		let previous = self.join.replace(Join::Taken);
		match previous {
			Join::Waiting(waker) => {
				*self.join.borrow_mut() = Join::Done(output);
				if let Some(waker) = waker {
					waker.wake();
				}
			}
			Join::Detached => drop(output),
			_ => unreachable!("the future finished while the slot held its result"),
		}
		Poll::Ready(())
	}

	fn abandon(&mut self) {
		let previous = self.join.replace(Join::Gone);
		if let Join::Waiting(Some(waker)) = previous {
			waker.wake();
		}
	}
}

// ---------------------------------------------------------------------------
// Ready queue + wakers
// ---------------------------------------------------------------------------

struct Ready {
	runnable: Mutex<VecDeque<DefaultKey>>,
	driver_waker: Waker,
	parked: AtomicBool,
	root_woken: AtomicBool,
	/// Set while a `block_on` drives this runtime.
	running: AtomicBool,
}

impl Ready {
	fn enqueue(&self, key: DefaultKey) {
		self.runnable
			.lock()
			.expect("the ready queue was poisoned")
			.push_back(key);
		self.notify();
	}

	/// Flags the root as woken and unparks the driver if it is waiting.
	fn notify(&self) {
		self.root_woken.store(true, Ordering::SeqCst);
		if self.parked.swap(false, Ordering::SeqCst) {
			self.driver_waker.wake_by_ref();
		}
	}

	fn dequeue(&self) -> Option<DefaultKey> {
		self.runnable
			.lock()
			.expect("the ready queue was poisoned")
			.pop_front()
	}

	fn has_ready(&self) -> bool {
		!self
			.runnable
			.lock()
			.expect("the ready queue was poisoned")
			.is_empty()
	}
}

impl Wake for Ready {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		self.notify();
	}
}

struct TaskWake {
	key: DefaultKey,
	scheduled: Arc<AtomicBool>,
	ready: SyncWeak<Ready>,
}

impl Wake for TaskWake {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		if self.scheduled.swap(true, Ordering::AcqRel) {
			return;
		}
		if let Some(ready) = self.ready.upgrade() {
			ready.enqueue(self.key);
		}
	}
}

// ---------------------------------------------------------------------------
// Spawning and polling tasks
// ---------------------------------------------------------------------------

/// Spawns a thread-local task.
pub fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
	let join = with_rt_or_default(|rt| {
		let join = Rc::new(RefCell::new(Join::Waiting(None)));
		// Queued right below, so it starts out scheduled.
		let scheduled = Arc::new(AtomicBool::new(true));
		let ready = Arc::downgrade(&rt.ready);
		let key = rt.tasks.insert_with_key(|key| {
			Some(Box::new(Job {
				future: Box::pin(future),
				join: Rc::clone(&join),
				scheduled: Arc::clone(&scheduled),
				waker: Waker::from(Arc::new(TaskWake {
					key,
					scheduled,
					ready,
				})),
			}) as Box<dyn Runnable>)
		});
		rt.ready.enqueue(key);
		join
	});
	JoinHandle { join }
}

/// Polls up to `budget` ready tasks. Each task is taken out of the runtime
/// before polling so the `RT` borrow is not held across the poll — futures
/// polled inside the task can call `submit`/`spawn` freely.
fn tick(budget: usize) -> bool {
	for _ in 0..budget {
		let Some((key, job)) = with_rt(|rt| {
			let key = rt.ready.dequeue()?;
			Some((key, rt.tasks.get_mut(key).and_then(Option::take)))
		}) else {
			break;
		};
		let Some(mut job) = job else {
			continue;
		};
		// No RT borrow held here — the future can submit/spawn freely.
		let result = catch_unwind(AssertUnwindSafe(|| job.poll()));
		match result {
			Ok(Poll::Pending) => {
				with_rt(|rt| {
					let slot = rt
						.tasks
						.get_mut(key)
						.expect("the task's slot went away while it ran");
					*slot = Some(job);
				});
			}
			Ok(Poll::Ready(())) => {
				with_rt(|rt| rt.tasks.remove(key));
			}
			Err(panic) => {
				job.abandon();
				with_rt(|rt| rt.tasks.remove(key));
				resume_unwind(panic);
			}
		}
	}
	with_rt(|rt| rt.ready.has_ready())
}

/// Installs a runtime built from `config` on the current thread.
///
/// Without this, the first [`block_on`], [`spawn`], or I/O operation on a
/// thread installs one with [`Config::default`]. Like `log::set_logger`, it
/// can only succeed once per thread: it fails with
/// [`io::ErrorKind::AlreadyExists`] if a runtime is already installed.
#[fastrace::trace]
pub fn init(config: Config) -> Result<()> {
	if RT.with(|cell| cell.borrow().is_some()) {
		return Err(io::Error::new(
			io::ErrorKind::AlreadyExists,
			"a runtime is already installed on this thread",
		)
		.into());
	}
	let rt = Runtime::new(config)?;
	RT.with(|cell| *cell.borrow_mut() = Some(rt));
	Ok(())
}

// ---------------------------------------------------------------------------
// The run loop (block_on)
// ---------------------------------------------------------------------------

/// Runs `future` to completion on the current thread.
pub fn block_on<F: Future>(future: F) -> Result<F::Output> {
	let ready = with_rt_or_default(|rt| Arc::clone(&rt.ready));
	assert!(
		!ready.running.swap(true, Ordering::SeqCst),
		"block_on cannot be nested on the same thread"
	);
	scopeguard::defer! {
		ready.running.store(false, Ordering::SeqCst);
	}
	let root_waker = Waker::from(Arc::clone(&ready));
	let mut fut = std::pin::pin!(future);
	let mut cx = Context::from_waker(&root_waker);
	loop {
		ready.root_woken.store(false, Ordering::SeqCst);
		if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
			tick(TICK_BUDGET);
			return Ok(out);
		}
		let timeout = if tick(TICK_BUDGET) {
			// Tasks are left over: reap completions without waiting.
			Some(Duration::ZERO)
		} else {
			// Park first, then look: a wake that lands after the look sees
			// `parked` and interrupts the wait, so none is lost.
			ready.parked.store(true, Ordering::SeqCst);
			if ready.root_woken.load(Ordering::SeqCst) {
				Some(Duration::ZERO)
			} else {
				None
			}
		};
		let result = with_rt(|rt| rt.drive(timeout));
		ready.parked.store(false, Ordering::SeqCst);
		result?;
	}
}

// ---------------------------------------------------------------------------
// Completion tracking (Entry, Shared, Slot, Timer)
// ---------------------------------------------------------------------------

type TimerKey = (Instant, u64);

struct Entry {
	inner: Mutex<EntryInner>,
}

struct EntryInner {
	response: Option<Result<Response>>,
	wakers: SmallVec<[Waker; 2]>,
	cancelled: bool,
	done: bool,
	descriptor: Option<Arc<dyn AsDescriptor>>,
	memory: Option<Arc<dyn Memory>>,
}

impl Entry {
	fn pending(descriptor: Arc<dyn AsDescriptor>, memory: Option<Arc<dyn Memory>>) -> Entry {
		Entry {
			inner: Mutex::new(EntryInner {
				response: None,
				wakers: SmallVec::new(),
				cancelled: false,
				done: false,
				descriptor: Some(descriptor),
				memory,
			}),
		}
	}

	fn lock(&self) -> MutexGuard<'_, EntryInner> {
		self.inner.lock().unwrap_or_else(|p| p.into_inner())
	}

	fn complete(&self, response: Result<Response>) {
		let mut inner = self.lock();
		inner.memory = None;
		inner.descriptor = None;
		inner.response = Some(response);
		inner.done = true;
		let wakers = std::mem::take(&mut inner.wakers);
		drop(inner);
		for waker in wakers {
			waker.wake();
		}
	}

	fn mark_cancelled(&self) -> bool {
		let mut inner = self.lock();
		!std::mem::replace(&mut inner.cancelled, true) && !inner.done
	}

	fn take_or_register(&self, waker: &Waker) -> Option<Result<Response>> {
		let mut inner = self.lock();
		if let Some(response) = inner.response.take() {
			return Some(response);
		}
		if !inner.wakers.iter().any(|w| w.will_wake(waker)) {
			inner.wakers.push(waker.clone());
		}
		None
	}

	fn take_response(&self) -> Option<Result<Response>> {
		self.lock().response.take()
	}
}

struct Shared {
	inner: Mutex<SharedInner>,
}

struct SharedInner {
	stopped: bool,
	cancellations: Vec<u64>,
	timers: BTreeMap<TimerKey, Waker>,
	next_timer: u64,
	waker: Waker,
}

impl Shared {
	fn lock(&self) -> MutexGuard<'_, SharedInner> {
		self.inner.lock().unwrap_or_else(|p| p.into_inner())
	}

	fn queue_cancel(&self, id: u64) {
		let mut inner = self.lock();
		inner.cancellations.push(id);
		inner.waker.wake_by_ref();
	}

	fn release(&self, id: u64, entry: &Entry) {
		if entry.mark_cancelled() {
			self.queue_cancel(id);
		}
	}

	fn take_cancellations(&self) -> Vec<u64> {
		std::mem::take(&mut self.lock().cancellations)
	}

	fn is_stopped(&self) -> bool {
		self.lock().stopped
	}

	fn stop(&self) -> Option<BTreeMap<TimerKey, Waker>> {
		let mut inner = self.lock();
		if std::mem::replace(&mut inner.stopped, true) {
			return None;
		}
		Some(std::mem::take(&mut inner.timers))
	}

	fn add_timer(&self, deadline: Instant, waker: &Waker) -> Option<u64> {
		let mut inner = self.lock();
		if inner.stopped {
			return None;
		}
		let seq = inner.next_timer;
		inner.next_timer += 1;
		inner.timers.insert((deadline, seq), waker.clone());
		Some(seq)
	}

	/// Refreshes the waker of a registered timer; `false` if the runtime
	/// dropped it on stopping.
	fn rearm_timer(&self, key: TimerKey, waker: &Waker) -> bool {
		match self.lock().timers.get_mut(&key) {
			Some(registered) => {
				registered.clone_from(waker);
				true
			}
			None => false,
		}
	}

	fn remove_timer(&self, key: TimerKey) {
		self.lock().timers.remove(&key);
	}

	fn expire_timers(&self) {
		let now = Instant::now();
		let mut expired = SmallVec::<[Waker; 4]>::new();
		let mut inner = self.lock();
		while let Some(entry) = inner.timers.first_entry() {
			if entry.key().0 > now {
				break;
			}
			expired.push(entry.remove());
		}
		drop(inner);
		for waker in expired {
			waker.wake();
		}
	}

	fn within(&self, timeout: Option<Duration>) -> Option<Duration> {
		let inner = self.lock();
		let nearest = inner
			.timers
			.first_key_value()
			.map(|(&(deadline, _), _)| deadline.saturating_duration_since(Instant::now()));
		match (timeout, nearest) {
			(Some(timeout), Some(nearest)) => Some(timeout.min(nearest)),
			(timeout, nearest) => timeout.or(nearest),
		}
	}
}

pub(crate) struct Slot {
	shared: Arc<Shared>,
	entry: Arc<Entry>,
	/// Cleared once the response is taken, so dropping no longer cancels.
	id: Option<u64>,
}

impl Drop for Slot {
	fn drop(&mut self) {
		if let Some(id) = self.id.take() {
			self.shared.release(id, &self.entry);
		}
		let response = self.entry.take_response();
		drop(response);
	}
}

impl Future for Slot {
	type Output = Result<Response>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Response>> {
		let this = self.get_mut();
		match this.entry.take_or_register(cx.waker()) {
			Some(response) => {
				this.id = None;
				Poll::Ready(response)
			}
			None => Poll::Pending,
		}
	}
}

/// Waits until `duration` has elapsed.
///
/// The deadline (`Instant::now() + duration`) is captured when `timeout` is
/// called; the timer is registered on first poll.
pub fn timeout(duration: Duration) -> impl Future<Output = Result<()>> {
	Timer {
		deadline: Instant::now() + duration,
		registered: None,
	}
}

/// A deadline on the runtime's timer list, registered on first poll.
///
/// Unlike a [`Slot`], nothing outside the runtime holds a timer: dropping it
/// just takes it off the list.
struct Timer {
	deadline: Instant,
	registered: Option<(Arc<Shared>, u64)>,
}

impl Drop for Timer {
	fn drop(&mut self) {
		if let Some((shared, seq)) = &self.registered {
			shared.remove_timer((self.deadline, *seq));
		}
	}
}

impl Future for Timer {
	type Output = Result<()>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
		let this = self.get_mut();
		if Instant::now() >= this.deadline {
			return Poll::Ready(Ok(()));
		}
		match &this.registered {
			Some((shared, seq)) => {
				if !shared.rearm_timer((this.deadline, *seq), cx.waker()) {
					return Poll::Ready(Err(Error::cancelled()));
				}
			}
			None => {
				let registered = with_rt_or_default(|rt| {
					let seq = rt.shared.add_timer(this.deadline, cx.waker())?;
					Some((Arc::clone(&rt.shared), seq))
				});
				match registered {
					Some(registered) => this.registered = Some(registered),
					None => return Poll::Ready(Err(Error::stopped())),
				}
			}
		}
		Poll::Pending
	}
}
