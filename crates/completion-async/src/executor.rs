use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak as SyncWeak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use slotmap::{DefaultKey, SlotMap};
use smallvec::SmallVec;

use completion_backend::{AsDescriptor, Backend, Buffer, Config, Error, Op, Response, Result};

const TICK_BUDGET: usize = 64;

// ---------------------------------------------------------------------------
// Thread-local state
// ---------------------------------------------------------------------------

thread_local! {
	static RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

/// The combined I/O driver and task scheduler for one thread.
struct Runtime {
	backend: Backend<Arc<Entry>>,
	shared: Arc<Shared>,
	tasks: SlotMap<DefaultKey, Option<Box<dyn Runnable>>>,
	ready: Arc<Ready>,
	running: bool,
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
		});
		Ok(Runtime {
			backend,
			shared,
			tasks: SlotMap::new(),
			ready,
			running: false,
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

	fn submit(&mut self, fd: Arc<dyn AsDescriptor>, op: Op) -> Slot {
		let buffer = op
			.span()
			.filter(|span| span.len() > 0)
			.map(|span| span.buffer().clone());
		let entry = Arc::new(Entry::pending(Some(Arc::clone(&fd)), buffer));
		if self.shared.is_stopped() {
			return Slot::refused(Error::stopped());
		}
		match self
			.backend
			.submit(fd.as_descriptor(), op, Arc::clone(&entry))
		{
			Ok(id) => Slot::new(Arc::clone(&self.shared), Key::Op(id), entry),
			Err(error) => Slot::refused(error),
		}
	}

	fn submit_timer(&self, deadline: Instant) -> Slot {
		match self.shared.add_timer(deadline) {
			Some((key, entry)) => Slot::new(Arc::clone(&self.shared), key, entry),
			None => Slot::refused(Error::stopped()),
		}
	}
}

impl Drop for Runtime {
	fn drop(&mut self) {
		for job in self.tasks.values_mut().filter_map(Option::as_mut) {
			job.abandon();
		}
		if let Some(timers) = self.shared.stop() {
			for timer in timers.into_values() {
				timer.complete(Err(Error::cancelled()));
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
fn with_rt_or_default<R>(f: impl FnOnce(&mut Runtime) -> R) -> R {
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
	pub(crate) join: Rc<RefCell<Join<T>>>,
}

impl<T> Future for JoinHandle<T> {
	type Output = T;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
		poll_join(&self.join, cx)
	}
}

pub(crate) fn poll_join<T>(join: &RefCell<Join<T>>, cx: &mut Context<'_>) -> Poll<T> {
	let mut join = join.borrow_mut();
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
		Join::Gone => panic!("the executor was dropped before the task finished"),
		Join::Taken | Join::Detached => {
			panic!("the task result is no longer available")
		}
	}
}

impl<T> Drop for JoinHandle<T> {
	fn drop(&mut self) {
		let held = matches!(*self.join.borrow(), Join::Waiting(_) | Join::Done(_));
		if held {
			let previous = self.join.replace(Join::Detached);
			drop(previous);
		}
	}
}

pub(crate) enum Join<T> {
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
	fn waker(&self) -> &Waker;
	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()>;
	fn abandon(&mut self);
}

struct Job<F: Future> {
	future: Pin<Box<F>>,
	join: Rc<RefCell<Join<F::Output>>>,
	scheduled: Arc<AtomicBool>,
	waker: Waker,
}

impl<F: Future> Runnable for Job<F> {
	fn waker(&self) -> &Waker {
		&self.waker
	}

	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
		self.scheduled.swap(false, Ordering::AcqRel);
		let output = match self.future.as_mut().poll(cx) {
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
}

impl Ready {
	fn enqueue(&self, key: DefaultKey) {
		self.runnable
			.lock()
			.expect("the ready queue was poisoned")
			.push_back(key);
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
		self.root_woken.store(true, Ordering::SeqCst);
		if self.parked.swap(false, Ordering::SeqCst) {
			self.driver_waker.wake_by_ref();
		}
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
// Core operations on Runtime (called via with_rt short borrows)
// ---------------------------------------------------------------------------

fn do_spawn<T: 'static>(
	rt: &mut Runtime,
	future: impl Future<Output = T> + 'static,
) -> Rc<RefCell<Join<T>>> {
	let join = Rc::new(RefCell::new(Join::Waiting(None)));
	let scheduled = Arc::new(AtomicBool::new(false));
	let ready = Arc::downgrade(&rt.ready);
	let mut queued = None;
	rt.tasks.insert_with_key(|key| {
		let waker = Waker::from(Arc::new(TaskWake {
			key,
			scheduled: Arc::clone(&scheduled),
			ready,
		}));
		queued = Some(waker.clone());
		Some(Box::new(Job {
			future: Box::pin(future),
			join: Rc::clone(&join),
			scheduled,
			waker: waker.clone(),
		}) as Box<dyn Runnable>)
	});
	let waker = queued.expect("the spawn inserted the task");
	waker.wake_by_ref();
	join
}

/// Polls up to `budget` ready tasks. Each task is taken out of the runtime
/// before polling so the `RT` borrow is not held across the poll — futures
/// polled inside the task can call `submit`/`spawn` freely.
fn tick(budget: usize) -> bool {
	for _ in 0..budget {
		let Some(key) = with_rt(|rt| rt.ready.dequeue()) else {
			break;
		};
		let Some(mut job) = with_rt(|rt| rt.tasks.get_mut(key).and_then(Option::take)) else {
			continue;
		};
		// No RT borrow held here — the future can submit/spawn freely.
		let waker = job.waker().clone();
		let result = catch_unwind(AssertUnwindSafe(|| {
			job.poll(&mut Context::from_waker(&waker))
		}));
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

// ---------------------------------------------------------------------------
// Executor (public configuration + lifecycle handle)
// ---------------------------------------------------------------------------

/// Single-threaded async executor and I/O reactor.
///
/// Create one to override the default backend [`Config`], or use the free
/// function [`block_on`] which lazily creates a default one.
pub struct Executor;

impl Executor {
	#[fastrace::trace]
	pub fn new(config: Config) -> Result<Executor> {
		let rt = Runtime::new(config)?;
		let previous = RT.with(|cell| cell.borrow_mut().replace(rt));
		drop(previous);
		Ok(Executor)
	}

	pub fn block_on<F: Future>(&mut self, fut: F) -> Result<F::Output> {
		run(fut)
	}

	pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
		JoinHandle {
			join: with_rt(|rt| do_spawn(rt, future)),
		}
	}
}

// ---------------------------------------------------------------------------
// The run loop (shared by Executor::block_on and free block_on)
// ---------------------------------------------------------------------------

struct RunGuard;

impl Drop for RunGuard {
	fn drop(&mut self) {
		let _ = RT.try_with(|cell| {
			if let Ok(mut borrow) = cell.try_borrow_mut()
				&& let Some(rt) = borrow.as_mut()
			{
				rt.running = false;
			}
		});
	}
}

fn run<F: Future>(fut: F) -> Result<F::Output> {
	let root_waker = with_rt_or_default(|rt| {
		assert!(!rt.running, "block_on cannot be nested on the same thread");
		rt.running = true;
		Waker::from(Arc::clone(&rt.ready))
	});
	let _guard = RunGuard;
	let mut fut = std::pin::pin!(fut);
	let mut cx = Context::from_waker(&root_waker);
	loop {
		with_rt(|rt| rt.ready.root_woken.swap(false, Ordering::SeqCst));
		if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
			tick(TICK_BUDGET);
			return Ok(out);
		}
		let more = tick(TICK_BUDGET);
		if more || with_rt(|rt| rt.ready.root_woken.swap(false, Ordering::SeqCst)) {
			with_rt(|rt| rt.drive(Some(Duration::ZERO)))?;
			continue;
		}
		with_rt(|rt| rt.ready.parked.store(true, Ordering::SeqCst));
		if with_rt(|rt| rt.ready.has_ready() || rt.ready.root_woken.swap(false, Ordering::SeqCst)) {
			with_rt(|rt| rt.ready.parked.store(false, Ordering::SeqCst));
			with_rt(|rt| rt.drive(Some(Duration::ZERO)))?;
			continue;
		}
		let result = with_rt(|rt| rt.drive(None));
		with_rt(|rt| rt.ready.parked.store(false, Ordering::SeqCst));
		result?;
	}
}

// ---------------------------------------------------------------------------
// Free-function public API
// ---------------------------------------------------------------------------

/// Runs `future` to completion on the current thread.
pub fn block_on<F: Future>(future: F) -> Result<F::Output> {
	run(future)
}

/// Spawns a thread-local task.
pub fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
	JoinHandle {
		join: with_rt_or_default(|rt| do_spawn(rt, future)),
	}
}

pub(crate) fn submit(fd: Arc<dyn AsDescriptor>, op: Op) -> Slot {
	with_rt_or_default(|rt| rt.submit(fd, op))
}

pub(crate) fn submit_timer(deadline: Instant) -> Slot {
	with_rt_or_default(|rt| rt.submit_timer(deadline))
}

// ---------------------------------------------------------------------------
// I/O completion tracking (Entry, Shared, Slot)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Key {
	Op(u64),
	Timer(TimerKey),
}

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
	buffer: Option<Buffer>,
}

impl Entry {
	fn pending(descriptor: Option<Arc<dyn AsDescriptor>>, buffer: Option<Buffer>) -> Entry {
		Entry {
			inner: Mutex::new(EntryInner {
				response: None,
				wakers: SmallVec::new(),
				cancelled: false,
				done: false,
				descriptor,
				buffer,
			}),
		}
	}

	fn lock(&self) -> MutexGuard<'_, EntryInner> {
		self.inner.lock().unwrap_or_else(|p| p.into_inner())
	}

	fn complete(&self, response: Result<Response>) {
		let mut inner = self.lock();
		inner.buffer = None;
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
	timers: BTreeMap<TimerKey, Arc<Entry>>,
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

	fn release(&self, key: Key, entry: &Entry) {
		match key {
			Key::Op(id) => {
				if !entry.mark_cancelled() {
					return;
				}
				self.queue_cancel(id);
			}
			Key::Timer(key) => {
				self.lock().timers.remove(&key);
			}
		}
	}

	fn take_cancellations(&self) -> Vec<u64> {
		std::mem::take(&mut self.lock().cancellations)
	}

	fn is_stopped(&self) -> bool {
		self.lock().stopped
	}

	fn stop(&self) -> Option<BTreeMap<TimerKey, Arc<Entry>>> {
		let mut inner = self.lock();
		if std::mem::replace(&mut inner.stopped, true) {
			return None;
		}
		Some(std::mem::take(&mut inner.timers))
	}

	fn add_timer(&self, deadline: Instant) -> Option<(Key, Arc<Entry>)> {
		let mut inner = self.lock();
		if inner.stopped {
			return None;
		}
		let entry = Arc::new(Entry::pending(None, None));
		let key = (deadline, inner.next_timer);
		inner.next_timer += 1;
		inner.timers.insert(key, Arc::clone(&entry));
		Some((Key::Timer(key), entry))
	}

	fn expire_timers(&self) {
		let mut inner = self.lock();
		let now = Instant::now();
		while let Some((&(deadline, _), _)) = inner.timers.first_key_value() {
			if deadline > now {
				break;
			}
			let (_, timer) = inner
				.timers
				.pop_first()
				.expect("the first timer is present");
			timer.complete(Ok(Response::Done));
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
	inner: SlotInner,
}

enum SlotInner {
	Live {
		shared: Arc<Shared>,
		entry: Arc<Entry>,
		key: Option<Key>,
		owner: bool,
	},
	Refused(Option<Result<Response>>),
}

impl Clone for Slot {
	fn clone(&self) -> Self {
		match &self.inner {
			SlotInner::Live {
				shared, entry, key, ..
			} => Slot {
				inner: SlotInner::Live {
					shared: Arc::clone(shared),
					entry: Arc::clone(entry),
					key: *key,
					owner: false,
				},
			},
			SlotInner::Refused(_) => Slot {
				inner: SlotInner::Refused(None),
			},
		}
	}
}

impl Drop for Slot {
	fn drop(&mut self) {
		let SlotInner::Live {
			shared,
			entry,
			key,
			owner: true,
		} = &mut self.inner
		else {
			return;
		};
		if let Some(key) = key.take() {
			shared.release(key, entry);
		}
		let response = entry.take_response();
		drop(response);
	}
}

impl Slot {
	fn new(shared: Arc<Shared>, key: Key, entry: Arc<Entry>) -> Slot {
		Slot {
			inner: SlotInner::Live {
				shared,
				entry,
				key: Some(key),
				owner: true,
			},
		}
	}

	fn refused(error: Error) -> Slot {
		Slot {
			inner: SlotInner::Refused(Some(Err(error))),
		}
	}
}

impl Future for Slot {
	type Output = Result<Response>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Response>> {
		match &mut self.get_mut().inner {
			SlotInner::Live { entry, key, .. } => {
				if let Some(response) = entry.take_or_register(cx.waker()) {
					*key = None;
					return Poll::Ready(response);
				}
				Poll::Pending
			}
			SlotInner::Refused(response) => {
				Poll::Ready(response.take().expect("a refused slot is polled only once"))
			}
		}
	}
}
