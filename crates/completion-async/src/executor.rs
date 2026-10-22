use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak as SyncWeak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use async_task::{Runnable, Task};
use slotmap::{DefaultKey, SlotMap};
use smallvec::SmallVec;

use completion_backend::{AsDescriptor, Backend, Config, Error, Op, Response, Result};

const TICK_BUDGET: usize = 64;

// ---------------------------------------------------------------------------
// Thread-local state
// ---------------------------------------------------------------------------

thread_local! {
	static RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

/// The combined I/O driver and task scheduler for one thread.
pub(crate) struct Runtime {
	backend: Backend<Arc<dyn EntryOps>>,
	shared: Arc<Shared>,
	/// Set while a `block_on` drives this runtime; only touched on this thread.
	running: bool,
}

impl Runtime {
	fn new(config: Config) -> Result<Runtime> {
		let backend = Backend::new(&config)?;
		let shared = Arc::new(Shared {
			inner: Mutex::new(SharedInner {
				stopped: false,
				root_runnable: None,
				runnable: VecDeque::new(),
				active_tasks: SlotMap::new(),
				cancellations: Vec::new(),
				timers: BTreeMap::new(),
				next_timer: 0,
			}),
			driver_waker: backend.waker(),
			parked: AtomicBool::new(false),
		});
		Ok(Runtime {
			backend,
			shared,
			running: false,
		})
	}

	fn drive(&mut self, timeout: Option<Duration>) -> Result<()> {
		for id in self.shared.take_cancellations() {
			self.backend.cancel(id);
		}
		let within = self.shared.reap_timers(timeout);
		let polled = self.backend.poll(within, |_id, entry, response| {
			entry.complete(response);
		});
		// Timers that came due during the wait wake their callers now.
		self.shared.reap_timers(None);
		polled
	}

	fn submit<D: AsDescriptor + 'static, M: Send + 'static>(
		&mut self,
		fd: Arc<D>,
		op: Op,
		memory: M,
	) -> Result<Slot<D, M>> {
		if self.shared.is_stopped() {
			return Err(Error::stopped());
		}
		let entry = Arc::new(Entry::pending(Arc::clone(&fd), memory));
		let id = self.backend.submit(
			fd.as_descriptor(),
			op,
			Arc::clone(&entry) as Arc<dyn EntryOps>,
		)?;
		Ok(Slot {
			shared: Arc::clone(&self.shared),
			entry,
			id: Some(id),
		})
	}
}

impl Drop for Runtime {
	fn drop(&mut self) {
		let (wakers, timers) = self.shared.stop();
		for (_, waker) in wakers {
			waker.wake();
		}
		for waker in timers.into_values() {
			waker.wake();
		}
		while let Some(runnable) = self.shared.dequeue() {
			drop(runnable);
		}
		self.backend.cancel_all();
		while !self.backend.is_idle() {
			if self.drive(None).is_err() {
				break;
			}
		}
		while let Some(runnable) = self.shared.dequeue() {
			drop(runnable);
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
		let mut borrow = cell.borrow_mut();
		let rt = borrow.get_or_insert_with(|| {
			Runtime::new(Config::default())
				.expect("the thread's default runtime could not be created")
		});
		f(rt)
	})
}

/// Submits `op` on `fd` to the current thread's runtime.
pub(crate) fn submit<D: AsDescriptor + 'static, M: Send + 'static>(
	fd: Arc<D>,
	op: Op,
	memory: M,
) -> Result<Slot<D, M>> {
	with_rt_or_default(|rt| rt.submit(fd, op, memory))
}

// ---------------------------------------------------------------------------
// JoinHandle
// ---------------------------------------------------------------------------

/// Handle to a spawned task.
pub struct JoinHandle<T> {
	task: Option<Task<T>>,
}

impl<T> Future for JoinHandle<T> {
	type Output = T;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
		Pin::new(
			self.task
				.as_mut()
				.expect("the task result is no longer available"),
		)
		.poll(cx)
	}
}

impl<T> Drop for JoinHandle<T> {
	fn drop(&mut self) {
		if let Some(task) = self.task.take() {
			task.detach();
		}
	}
}

// ---------------------------------------------------------------------------
// Spawning tasks
// ---------------------------------------------------------------------------

/// Spawns a task onto the current thread's runtime.
///
/// The task runs on the thread that spawned it, driven by that thread's
/// [`block_on`]: it needs neither `Send` nor a thread of its own.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
	F: Future + 'static,
	F::Output: 'static,
{
	with_rt_or_default(|rt| {
		let key = rt.shared.lock().active_tasks.insert(Waker::noop().clone());
		let cleanup: SyncWeak<Shared> = Arc::downgrade(&rt.shared);
		let wrapped = async move {
			scopeguard::defer! {
				if let Some(shared) = cleanup.upgrade() {
					shared.lock().active_tasks.remove(key);
				}
			}
			future.await
		};
		let schedule_weak: SyncWeak<Shared> = Arc::downgrade(&rt.shared);
		let (runnable, task) = async_task::spawn_local(wrapped, move |runnable: Runnable| {
			if let Some(shared) = schedule_weak.upgrade() {
				shared.enqueue(runnable);
			}
		});
		if let Some(slot) = rt.shared.lock().active_tasks.get_mut(key) {
			*slot = runnable.waker();
		}
		runnable.schedule();
		JoinHandle { task: Some(task) }
	})
}

// ---------------------------------------------------------------------------
// The run loop (block_on)
// ---------------------------------------------------------------------------

/// Installs a runtime built from `config` on the current thread.
///
/// Without this, the first [`block_on`], [`spawn`], or I/O operation on a
/// thread installs one with [`Config::default`]. Like `log::set_logger`, it
/// can only succeed once per thread: it fails with
/// [`io::ErrorKind::AlreadyExists`] if a runtime is already installed.
#[fastrace::trace]
pub fn init(config: Config) -> Result<()> {
	RT.with(|cell| {
		let mut borrow = cell.borrow_mut();
		if borrow.is_some() {
			return Err(io::Error::new(
				io::ErrorKind::AlreadyExists,
				"a runtime is already installed on this thread",
			)
			.into());
		}
		*borrow = Some(Runtime::new(config)?);
		Ok(())
	})
}

/// Runs `future` to completion on the current thread.
pub fn block_on<F: Future>(future: F) -> Result<F::Output> {
	let shared = with_rt_or_default(|rt| {
		assert!(!rt.running, "block_on cannot be nested on the same thread");
		rt.running = true;
		Arc::clone(&rt.shared)
	});
	let output = Rc::new(RefCell::new(None));
	let slot = Rc::clone(&output);
	let weak = Arc::downgrade(&shared);
	// SAFETY: the root future runs only on this thread (via `root_runnable`),
	// and the guard below cancels `root_task` and drops any queued root
	// `Runnable` before `block_on` returns or unwinds.
	let (runnable, root_task) = unsafe {
		async_task::spawn_unchecked(
			async move {
				*slot.borrow_mut() = Some(future.await);
			},
			move |runnable: Runnable| {
				if let Some(shared) = weak.upgrade() {
					shared.enqueue_root(runnable);
				}
			},
		)
	};
	shared.lock().root_runnable = Some(runnable);
	scopeguard::defer! {
		drop(root_task);
		let root = shared.lock().root_runnable.take();
		drop(root);
		with_rt(|rt| rt.running = false);
	}
	loop {
		for _ in 0..TICK_BUDGET {
			let Some(runnable) = shared.dequeue() else {
				break;
			};
			runnable.run();
		}
		if let Some(output) = output.borrow_mut().take() {
			return Ok(output);
		}
		// Park first, then look: a wake that lands after the look sees
		// `parked` and interrupts the wait, so none is lost.
		shared.parked.store(true, Ordering::SeqCst);
		let timeout = if shared.has_work() {
			// Work is left over: reap completions without waiting.
			Some(Duration::ZERO)
		} else {
			None
		};
		let result = with_rt(|rt| rt.drive(timeout));
		shared.parked.store(false, Ordering::SeqCst);
		result?;
	}
}

// ---------------------------------------------------------------------------
// Completion tracking (Entry, Shared, Slot, Timer)
// ---------------------------------------------------------------------------

type TimerKey = (Instant, u64);

/// What [`Shared::stop`] hands back: the wakers of the live tasks and of the
/// registered timers, so both end rather than wait for a runtime that is gone.
type Stopped = (SlotMap<DefaultKey, Waker>, BTreeMap<TimerKey, Waker>);

/// What the backend drives on an entry once it no longer names its types.
///
/// The entry itself stays generic over its descriptor and memory types; both
/// live in the same allocation, and the type-erased handle the backend keeps is
/// an `Arc` over that allocation.
trait EntryOps: Send + Sync {
	fn complete(&self, response: Result<Response>);
}

struct Entry<D, M> {
	state: Mutex<EntryState<D, M>>,
}

/// Where an operation stands.
///
/// The memory and descriptor stay in the entry until the operation settles, so
/// a future dropped in flight leaves them to go with the entry when the kernel
/// finishes. Completing replaces the pending variant, releasing the descriptor;
/// the memory then waits in `Done` for the caller.
enum EntryState<D, M> {
	Pending {
		waker: Option<Waker>,
		descriptor: Arc<D>,
		memory: M,
	},
	Done(Result<Response>, M),
	/// The completion and memory went back to the caller.
	Taken,
}

impl<D, M> Entry<D, M> {
	fn pending(descriptor: Arc<D>, memory: M) -> Entry<D, M> {
		Entry {
			state: Mutex::new(EntryState::Pending {
				waker: None,
				descriptor,
				memory,
			}),
		}
	}

	fn lock(&self) -> MutexGuard<'_, EntryState<D, M>> {
		self.state.lock().unwrap_or_else(|p| p.into_inner())
	}

	/// Cancels the operation unless it has already settled.
	fn cancel(&self, shared: &Shared, id: u64) {
		let state = self.lock();
		if matches!(&*state, EntryState::Pending { .. }) {
			drop(state);
			shared.queue_cancel(id);
		}
	}

	fn take_or_register(&self, waker: &Waker) -> Option<(Result<Response>, M)> {
		let mut state = self.lock();
		if let EntryState::Pending {
			waker: registered, ..
		} = &mut *state
		{
			if registered.as_ref().is_none_or(|w| !w.will_wake(waker)) {
				*registered = Some(waker.clone());
			}
			return None;
		}
		match std::mem::replace(&mut *state, EntryState::Taken) {
			EntryState::Done(response, memory) => Some((response, memory)),
			_ => None,
		}
	}
}

impl<D: AsDescriptor + 'static, M: Send + 'static> EntryOps for Entry<D, M> {
	fn complete(&self, response: Result<Response>) {
		let mut state = self.lock();
		let waker = match std::mem::replace(&mut *state, EntryState::Taken) {
			EntryState::Pending {
				waker,
				descriptor,
				memory,
			} => {
				drop(descriptor);
				*state = EntryState::Done(response, memory);
				waker
			}
			// The backend reports each operation once.
			EntryState::Done(..) | EntryState::Taken => {
				unreachable!("the backend reports each operation once")
			}
		};
		drop(state);
		if let Some(waker) = waker {
			waker.wake();
		}
	}
}

struct Shared {
	inner: Mutex<SharedInner>,
	driver_waker: Waker,
	/// Set while the driver is waiting in `Backend::poll`.
	parked: AtomicBool,
}

struct SharedInner {
	stopped: bool,
	/// The `block_on` root, queued by its waker; only the owning thread runs it.
	root_runnable: Option<Runnable>,
	/// Ready `spawn` tasks; only the owning thread runs them.
	runnable: VecDeque<Runnable>,
	/// Wakers of live `spawn` tasks, so dropping the runtime wakes them.
	active_tasks: SlotMap<DefaultKey, Waker>,
	cancellations: Vec<u64>,
	timers: BTreeMap<TimerKey, Waker>,
	next_timer: u64,
}

impl Shared {
	fn lock(&self) -> MutexGuard<'_, SharedInner> {
		self.inner.lock().unwrap_or_else(|p| p.into_inner())
	}

	fn enqueue_root(&self, runnable: Runnable) {
		self.lock().root_runnable = Some(runnable);
		self.notify();
	}

	fn enqueue(&self, runnable: Runnable) {
		self.lock().runnable.push_back(runnable);
		self.notify();
	}

	/// Unparks the driver if it is waiting.
	fn notify(&self) {
		if self.parked.swap(false, Ordering::SeqCst) {
			self.driver_waker.wake_by_ref();
		}
	}

	fn dequeue(&self) -> Option<Runnable> {
		let mut inner = self.lock();
		inner
			.root_runnable
			.take()
			.or_else(|| inner.runnable.pop_front())
	}

	/// Whether the loop has something to do besides wait: a queued task or a
	/// cancellation still to hand to the backend.
	fn has_work(&self) -> bool {
		let inner = self.lock();
		inner.root_runnable.is_some()
			|| !inner.runnable.is_empty()
			|| !inner.cancellations.is_empty()
	}

	fn queue_cancel(&self, id: u64) {
		self.lock().cancellations.push(id);
		self.notify();
	}

	fn take_cancellations(&self) -> Vec<u64> {
		std::mem::take(&mut self.lock().cancellations)
	}

	fn is_stopped(&self) -> bool {
		self.lock().stopped
	}

	/// Marks the runtime stopped and hands back what is still waiting on it.
	fn stop(&self) -> Stopped {
		let mut inner = self.lock();
		inner.stopped = true;
		(
			std::mem::take(&mut inner.active_tasks),
			std::mem::take(&mut inner.timers),
		)
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

	/// Reaps timers that have come due and returns how long the backend may
	/// wait: `timeout` bounded by the nearest remaining deadline.
	///
	/// An expiry wakes its waiter and cuts the wait to zero, so work it makes
	/// ready is not left behind a wait for a later deadline.
	fn reap_timers(&self, timeout: Option<Duration>) -> Option<Duration> {
		let now = Instant::now();
		let mut expired = SmallVec::<[Waker; 4]>::new();
		let mut inner = self.lock();
		while let Some(entry) = inner.timers.first_entry() {
			if entry.key().0 > now {
				break;
			}
			expired.push(entry.remove());
		}
		let within = if expired.is_empty() {
			let nearest = inner
				.timers
				.first_key_value()
				.map(|(&(deadline, _), _)| deadline.saturating_duration_since(now));
			match (timeout, nearest) {
				(Some(timeout), Some(nearest)) => Some(timeout.min(nearest)),
				(timeout, nearest) => timeout.or(nearest),
			}
		} else {
			Some(Duration::ZERO)
		};
		drop(inner);
		for waker in expired {
			waker.wake();
		}
		within
	}
}

pub(crate) struct Slot<D, M> {
	shared: Arc<Shared>,
	entry: Arc<Entry<D, M>>,
	/// Cleared once the response is taken, so dropping no longer cancels.
	id: Option<u64>,
}

impl<D, M> Drop for Slot<D, M> {
	fn drop(&mut self) {
		if let Some(id) = self.id.take() {
			self.entry.cancel(&self.shared, id);
		}
	}
}

impl<D, M> Future for Slot<D, M> {
	/// The completion and the memory the operation was filed with.
	type Output = (Result<Response>, M);

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		match this.entry.take_or_register(cx.waker()) {
			Some((response, memory)) => {
				this.id = None;
				Poll::Ready((response, memory))
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
