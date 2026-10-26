use std::cell::{Cell, RefCell, UnsafeCell};
use std::future::Future;
use std::io;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use completion_backend::{
	AsDescriptor, Backend, Config, Op, Response, Result, Waker as BackendWaker,
};
use crossbeam_queue::SegQueue;
use slotmap::{DefaultKey, SlotMap};

const TICK_BUDGET: usize = 64;

thread_local! {
	static RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };
	static BUDGET: Cell<usize> = const { Cell::new(usize::MAX) };
}

fn poll_proceed(cx: &Context<'_>) -> bool {
	let budget = BUDGET.get();
	if budget > 0 {
		BUDGET.set(budget - 1);
		true
	} else {
		cx.waker().wake_by_ref();
		false
	}
}

trait RunnableTask: Send + Sync {
	fn run(self: Arc<Self>);
	fn abort(&self);
}

enum Stage<F: Future> {
	Running { future: F, waker: Option<Waker> },
	Finished(F::Output),
	Panicked,
	Consumed,
}

struct TaskCell<F: Future> {
	scheduler: Arc<Scheduler>,
	stage: UnsafeCell<Stage<F>>,
}

// SAFETY: `stage` is only ever accessed or dropped on the owning runtime
// thread; cross-thread `Waker` calls only push `Arc<TaskCell<F>>` to `scheduler`.
unsafe impl<F: Future> Send for TaskCell<F> {}
unsafe impl<F: Future> Sync for TaskCell<F> {}

impl<F: Future + 'static> Wake for TaskCell<F> {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		self.scheduler
			.push(Arc::clone(self) as Arc<dyn RunnableTask>);
	}
}

impl<F: Future + 'static> RunnableTask for TaskCell<F> {
	fn run(self: Arc<Self>) {
		// Extract raw pointer to `future` so no reference to `Stage` is held
		// across `poll` while `PanicGuard` is active.
		let future_ptr: *mut F = unsafe {
			match &mut *self.stage.get() {
				Stage::Running { future, .. } => future as *mut F,
				_ => return,
			}
		};
		let waker = Waker::from(Arc::clone(&self));
		let mut cx = Context::from_waker(&waker);

		struct PanicGuard<'a, F: Future>(&'a TaskCell<F>);
		impl<F: Future> Drop for PanicGuard<'_, F> {
			fn drop(&mut self) {
				BUDGET.set(usize::MAX);
				// SAFETY: only the owning thread runs `run`.
				let prev = unsafe { std::mem::replace(&mut *self.0.stage.get(), Stage::Panicked) };
				if let Stage::Running {
					waker: Some(waker), ..
				} = prev
				{
					waker.wake();
				}
			}
		}

		let guard = PanicGuard(&self);
		BUDGET.set(TICK_BUDGET);
		// SAFETY: `future_ptr` is pinned inside `self` on the heap and only
		// polled on the owning thread.
		let polled = unsafe { Pin::new_unchecked(&mut *future_ptr).poll(&mut cx) };
		BUDGET.set(usize::MAX);
		std::mem::forget(guard);

		if let Poll::Ready(output) = polled {
			let prev =
				unsafe { std::mem::replace(&mut *self.stage.get(), Stage::Finished(output)) };
			if let Stage::Running {
				waker: Some(waker), ..
			} = prev
			{
				waker.wake();
			}
		}
	}

	fn abort(&self) {
		// SAFETY: only called on the owning thread.
		unsafe { *self.stage.get() = Stage::Consumed };
	}
}

/// Lock-free runnable task queue for one thread's executor.
struct Scheduler {
	runnable: SegQueue<Arc<dyn RunnableTask>>,
	root_woken: AtomicBool,
	parked: AtomicBool,
	waker: BackendWaker,
}

impl Scheduler {
	fn push(&self, task: Arc<dyn RunnableTask>) {
		self.runnable.push(task);
		if self.parked.load(Ordering::SeqCst) {
			self.waker.wake();
		}
	}
}

impl Wake for Scheduler {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		self.root_woken.store(true, Ordering::SeqCst);
		if self.parked.load(Ordering::SeqCst) {
			self.waker.wake();
		}
	}
}

struct LocalOp {
	waker: Option<Waker>,
	result: *mut Option<Result<Response>>,
	keepalive: Option<Box<dyn Send>>,
}

/// Single-threaded task scheduler, timer wheel, and direct [`Backend`] driver.
struct Runtime {
	backend: Backend<LocalOp>,
	timers: SlotMap<DefaultKey, (Instant, Waker)>,
	scheduler: Arc<Scheduler>,
	/// Set while a `block_on` drives this runtime; only touched on this thread.
	running: bool,
}

impl Default for Runtime {
	fn default() -> Self {
		Runtime::new(Config::default()).expect("the thread's default runtime could not be created")
	}
}

impl Runtime {
	fn new(config: Config) -> Result<Runtime> {
		let backend = Backend::new(&config)?;
		let scheduler = Arc::new(Scheduler {
			runnable: SegQueue::new(),
			root_woken: AtomicBool::new(false),
			parked: AtomicBool::new(false),
			waker: backend.waker(),
		});
		Ok(Runtime {
			backend,
			timers: SlotMap::new(),
			scheduler,
			running: false,
		})
	}

	fn drive(&mut self, timeout: Option<Duration>) -> Result<()> {
		let within = self.reap_timers(timeout);
		let polled = self.backend.poll(within, |_id, op, response| {
			if !op.result.is_null() {
				// SAFETY: `op.result` points to the pinned `LocalSlot`, which
				// clears `op.result` to null in `Drop` if dropped before completion.
				unsafe { *op.result = Some(response) };
			}
			drop(op.keepalive);
			if let Some(waker) = op.waker {
				waker.wake();
			}
		});
		self.reap_timers(None);
		polled
	}

	fn reap_timers(&mut self, timeout: Option<Duration>) -> Option<Duration> {
		let now = Instant::now();
		let mut expired = false;
		let mut nearest: Option<Duration> = None;
		self.timers.retain(|_, (deadline, waker)| {
			if *deadline <= now {
				expired = true;
				waker.wake_by_ref();
				false
			} else {
				let rem = deadline.saturating_duration_since(now);
				nearest = Some(nearest.map_or(rem, |n| n.min(rem)));
				true
			}
		});
		if expired {
			Some(Duration::ZERO)
		} else {
			match (timeout, nearest) {
				(Some(timeout), Some(nearest)) => Some(timeout.min(nearest)),
				(timeout, nearest) => timeout.or(nearest),
			}
		}
	}

	fn submit<D: AsDescriptor + 'static, M: Send + 'static>(
		&mut self,
		fd: D,
		op: Op,
		memory: M,
	) -> Result<LocalSlot<D, M>> {
		let id = self.backend.submit(
			fd.as_descriptor(),
			op,
			LocalOp {
				waker: None,
				result: std::ptr::null_mut(),
				keepalive: None,
			},
		)?;
		Ok(LocalSlot {
			id,
			fd: Some(fd),
			memory: Some(memory),
			result: UnsafeCell::new(None),
			_pin: PhantomPinned,
		})
	}
}

impl Drop for Runtime {
	fn drop(&mut self) {
		self.timers.clear();
		while let Some(task) = self.scheduler.runnable.pop() {
			task.abort();
		}
		self.backend.cancel_all();
		while !self.backend.is_idle() && self.drive(None).is_ok() {}
		while let Some(task) = self.scheduler.runnable.pop() {
			task.abort();
		}
	}
}

pub(crate) fn submit<D: AsDescriptor + 'static, M: Send + 'static>(
	fd: D,
	op: Op,
	memory: M,
) -> Result<LocalSlot<D, M>> {
	RT.with_borrow_mut(|rt| rt.get_or_insert_default().submit(fd, op, memory))
}

pub(crate) struct LocalSlot<D: Send + 'static, M: Send + 'static> {
	id: u64,
	fd: Option<D>,
	memory: Option<M>,
	result: UnsafeCell<Option<Result<Response>>>,
	_pin: PhantomPinned,
}

// SAFETY: `LocalSlot` is only polled after pinning on the local runtime thread.
unsafe impl<D: Send + 'static, M: Send + 'static> Send for LocalSlot<D, M> {}

impl<D: Send + 'static, M: Send + 'static> Future for LocalSlot<D, M> {
	type Output = (Result<Response>, D, M);

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		// SAFETY: we do not move any pinned fields out of `this`.
		let this = unsafe { self.get_unchecked_mut() };
		if unsafe { (*this.result.get()).is_some() } {
			if !poll_proceed(cx) {
				return Poll::Pending;
			}
			return Poll::Ready((
				unsafe { (*this.result.get()).take().unwrap() },
				this.fd.take().unwrap(),
				this.memory.take().unwrap(),
			));
		}
		let result_ptr = this.result.get();
		let id = this.id;
		RT.with_borrow_mut(|rt| {
			let op = rt
				.as_mut()
				.unwrap()
				.backend
				.get_mut(id)
				.expect("in-flight operation missing");
			op.result = result_ptr;
			if op.waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
				op.waker = Some(cx.waker().clone());
			}
		});
		Poll::Pending
	}
}

impl<D: Send + 'static, M: Send + 'static> Drop for LocalSlot<D, M> {
	fn drop(&mut self) {
		if let (Some(fd), Some(memory)) = (self.fd.take(), self.memory.take()) {
			if self.result.get_mut().is_some() {
				return;
			}
			let id = self.id;
			let _ = RT.try_with(|cell| {
				if let Some(rt) = cell.borrow_mut().as_mut()
					&& let Some(op) = rt.backend.get_mut(id)
				{
					op.result = std::ptr::null_mut();
					op.keepalive = Some(Box::new((fd, memory)));
					rt.backend.cancel(id);
				}
			});
		}
	}
}

/// Waits until `duration` has elapsed on the current thread's runtime.
///
/// The deadline (`Instant::now() + duration`) is captured when `timeout` is
/// called; the timer is registered on first poll.
pub fn timeout(duration: Duration) -> impl Future<Output = Result<()>> {
	Timer {
		deadline: Instant::now() + duration,
		key: DefaultKey::default(),
	}
}

struct Timer {
	deadline: Instant,
	key: DefaultKey,
}

impl Drop for Timer {
	fn drop(&mut self) {
		let _ = RT.try_with(|cell| {
			if let Some(rt) = cell.borrow_mut().as_mut() {
				rt.timers.remove(self.key);
			}
		});
	}
}

impl Future for Timer {
	type Output = Result<()>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
		let this = self.get_mut();
		if Instant::now() >= this.deadline {
			return if poll_proceed(cx) {
				Poll::Ready(Ok(()))
			} else {
				Poll::Pending
			};
		}
		let waker = cx.waker().clone();
		let deadline = this.deadline;
		RT.with_borrow_mut(|rt| {
			let rt = rt.get_or_insert_default();
			match rt.timers.get_mut(this.key) {
				Some((_, registered)) => {
					if !registered.will_wake(&waker) {
						*registered = waker;
					}
				}
				None => {
					this.key = rt.timers.insert((deadline, waker));
				}
			}
		});
		Poll::Pending
	}
}

/// Spawns a task onto the current thread's runtime.
///
/// The task runs on the thread that spawned it, driven by that thread's
/// [`block_on`]: it needs neither `Send` nor a thread of its own.
#[fastrace::trace]
pub fn spawn<F>(future: F) -> impl Future<Output = F::Output>
where
	F: Future + 'static,
	F::Output: 'static,
{
	let task = RT.with_borrow_mut(|rt| {
		let rt = rt.get_or_insert_default();
		let task = Arc::new(TaskCell {
			scheduler: Arc::clone(&rt.scheduler),
			stage: UnsafeCell::new(Stage::Running {
				future,
				waker: None,
			}),
		});
		rt.scheduler
			.push(Arc::clone(&task) as Arc<dyn RunnableTask>);
		task
	});
	let not_send = std::marker::PhantomData::<*const ()>;
	std::future::poll_fn(move |cx| {
		let _ = not_send;
		// SAFETY: `not_send` makes this future `!Send`, so it is only polled
		// on the owning thread.
		unsafe {
			match &mut *task.stage.get() {
				Stage::Running { waker, .. } => {
					if waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
						*waker = Some(cx.waker().clone());
					}
					Poll::Pending
				}
				Stage::Finished(_) => {
					if poll_proceed(cx) {
						let Stage::Finished(output) =
							std::mem::replace(&mut *task.stage.get(), Stage::Consumed)
						else {
							unreachable!()
						};
						Poll::Ready(output)
					} else {
						Poll::Pending
					}
				}
				Stage::Panicked => panic!("the spawned task panicked"),
				Stage::Consumed => panic!("the task result was already taken"),
			}
		}
	})
}

/// Yields execution back to the executor.
#[fastrace::trace]
pub fn yield_now() -> impl Future<Output = ()> {
	let mut yielded = false;
	std::future::poll_fn(move |cx| {
		if std::mem::replace(&mut yielded, true) {
			Poll::Ready(())
		} else {
			cx.waker().wake_by_ref();
			Poll::Pending
		}
	})
}

/// Runs `future` to completion on the current thread.
#[fastrace::trace]
pub fn block_on<F: Future>(future: F) -> Result<F::Output> {
	let scheduler = RT.with_borrow_mut(|rt| {
		let rt = rt.get_or_insert_default();
		assert!(!rt.running, "block_on cannot be nested on the same thread");
		rt.running = true;
		Arc::clone(&rt.scheduler)
	});
	scopeguard::defer! {
		BUDGET.set(usize::MAX);
		RT.with_borrow_mut(|rt| rt.as_mut().unwrap().running = false);
	}
	scheduler.root_woken.store(true, Ordering::SeqCst);
	let waker = Waker::from(Arc::clone(&scheduler));
	let mut cx = Context::from_waker(&waker);
	let mut future = std::pin::pin!(future);
	let mut output = None;

	loop {
		if output.is_none() && scheduler.root_woken.swap(false, Ordering::SeqCst) {
			BUDGET.set(TICK_BUDGET);
			if let Poll::Ready(val) = future.as_mut().poll(&mut cx) {
				output = Some(val);
			}
			BUDGET.set(usize::MAX);
		}
		for _ in 0..TICK_BUDGET {
			let Some(task) = scheduler.runnable.pop() else {
				break;
			};
			task.run();
		}
		if let Some(output) = output {
			return Ok(output);
		}
		if scheduler.root_woken.load(Ordering::SeqCst) {
			continue;
		}
		RT.with_borrow_mut(|rt| {
			let rt = rt.as_mut().unwrap();
			rt.scheduler.parked.store(true, Ordering::SeqCst);
			let timeout = if rt.scheduler.runnable.is_empty()
				&& !rt.scheduler.root_woken.load(Ordering::SeqCst)
			{
				None
			} else {
				Some(Duration::ZERO)
			};
			let res = rt.drive(timeout);
			rt.scheduler.parked.store(false, Ordering::SeqCst);
			res
		})?;
	}
}

/// Installs a runtime built from `config` on the current thread.
///
/// Without this, the first [`block_on`], [`spawn`], or I/O operation on a
/// thread installs one with [`Config::default`]. Fails with
/// [`io::ErrorKind::AlreadyExists`] if a runtime is already installed.
#[fastrace::trace]
pub fn init(config: Config) -> Result<()> {
	RT.with_borrow_mut(|rt| {
		if rt.is_some() {
			return Err(io::Error::new(
				io::ErrorKind::AlreadyExists,
				"a runtime is already installed on this thread",
			)
			.into());
		}
		*rt = Some(Runtime::new(config)?);
		Ok(())
	})
}
