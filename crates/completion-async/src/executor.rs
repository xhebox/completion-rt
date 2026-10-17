//! The executor a completion reactor's tasks run on.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;

use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak as SyncWeak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use completion_rt::Config;

use crate::Error;
use crate::reactor::Reactor;

const TICK_BUDGET: usize = 64;

/// A handle for spawning thread-local tasks while its executor exists.
#[derive(Clone)]
pub struct Spawner {
	local: Weak<Local>,
}

impl Spawner {
	/// Spawns a task, or returns an [`Error`](crate::Error) of kind
	/// [`ExecutorClosed`](crate::ErrorKind::ExecutorClosed) if the executor has been dropped.
	pub fn try_spawn<T: 'static>(
		&self,
		future: impl Future<Output = T> + 'static,
	) -> Result<Task<T>, Error> {
		let Some(local) = self.local.upgrade() else {
			return Err(Error::executor_closed());
		};
		let (join, waker) = local.spawn(future);
		Ok(Task { join, waker })
	}

	/// Panics if the executor has been dropped.
	pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Task<T> {
		self.try_spawn(future)
			.expect("the executor was dropped before spawning a task")
	}
}

/// A thread-local task result. Dropping it requests cancellation on the next executor pass.
#[must_use = "await the task or detach it to keep it running"]
pub struct Task<T> {
	join: Rc<RefCell<Join<T>>>,
	waker: Waker,
}

impl<T> Task<T> {
	/// Lets the task finish without keeping its result.
	pub fn detach(self) {
		let previous = self.join.replace(Join::Detached);
		drop(previous);
	}
}

impl<T> Future for Task<T> {
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
		Join::Taken | Join::Detached | Join::Cancelled => {
			panic!("the task result is no longer available")
		}
	}
}

impl<T> Drop for Task<T> {
	fn drop(&mut self) {
		let waiting = matches!(*self.join.borrow(), Join::Waiting(_));
		if waiting {
			let previous = self.join.replace(Join::Cancelled);
			drop(previous);
			self.waker.wake_by_ref();
		}
	}
}

pub(crate) enum Join<T> {
	Detached,
	Waiting(Option<Waker>),
	Done(T),
	Taken,
	Gone,
	Cancelled,
}

trait Runnable {
	fn waker(&self) -> &Waker;
	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()>;
	fn abandon(&mut self);
}

struct Job<F: Future> {
	future: Option<Pin<Box<F>>>,
	join: Rc<RefCell<Join<F::Output>>>,
	scheduled: Arc<AtomicBool>,
	waker: Waker,
}

impl<F: Future> Runnable for Job<F> {
	fn waker(&self) -> &Waker {
		&self.waker
	}

	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
		// This task is off the queue, so it is no longer scheduled: a wake from
		// here on must queue it again. `swap` rather than a plain store: a wake
		// that saw the flag set and returned without queueing anything has to have
		// its writes published to this poll, and reading that `true` back is what
		// publishes them.
		self.scheduled.swap(false, Ordering::AcqRel);
		if matches!(*self.join.borrow(), Join::Cancelled) {
			self.future = None;
			return Poll::Ready(());
		}
		let output = match self
			.future
			.as_mut()
			.expect("a task is polled only while it still has its future")
			.as_mut()
			.poll(cx)
		{
			Poll::Pending => return Poll::Pending,
			Poll::Ready(output) => output,
		};
		if matches!(*self.join.borrow(), Join::Cancelled) {
			self.future = None;
			drop(output);
			return Poll::Ready(());
		}
		drop(self.future.take());
		let previous = self.join.replace(Join::Taken);
		match previous {
			Join::Waiting(waker) => {
				*self.join.borrow_mut() = Join::Done(output);
				if let Some(waker) = waker {
					waker.wake();
				}
			}
			Join::Detached => drop(output),
			Join::Cancelled => drop(output),
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

/// A task set, and the queue of ids a wake has made runnable.
pub(crate) struct Local {
	tasks: RefCell<HashMap<u64, Box<dyn Runnable>>>,
	ready: Arc<Ready>,
	next_id: Cell<u64>,
}

impl Local {
	pub(crate) fn new() -> Local {
		Local {
			tasks: RefCell::new(HashMap::new()),
			ready: Arc::new(Ready {
				runnable: Mutex::new(VecDeque::new()),
				driver: Mutex::new(None),
			}),
			next_id: Cell::new(0),
		}
	}

	pub(crate) fn spawn<T: 'static>(
		&self,
		future: impl Future<Output = T> + 'static,
	) -> (Rc<RefCell<Join<T>>>, Waker) {
		let id = self.next_id.get();
		self.next_id
			.set(id.checked_add(1).expect("task id overflow"));
		let join = Rc::new(RefCell::new(Join::Waiting(None)));
		let scheduled = Arc::new(AtomicBool::new(false));
		let waker = Waker::from(Arc::new(TaskWake {
			id,
			scheduled: Arc::clone(&scheduled),
			ready: Arc::downgrade(&self.ready),
		}));
		self.tasks.borrow_mut().insert(
			id,
			Box::new(Job {
				future: Some(Box::pin(future)),
				join: Rc::clone(&join),
				scheduled,
				waker: waker.clone(),
			}),
		);
		waker.wake_by_ref();
		(join, waker)
	}

	/// Sets the waker a task's wake raises: that of whoever polls this set.
	pub(crate) fn drive(&self, driver: &Waker) {
		let mut slot = self
			.ready
			.driver
			.lock()
			.expect("the driver waker slot was poisoned");
		if slot
			.as_ref()
			.is_none_or(|current| !current.will_wake(driver))
		{
			*slot = Some(driver.clone());
		}
	}

	pub(crate) fn tick(&self, budget: usize) -> bool {
		for _ in 0..budget {
			let Some(id) = self.ready.dequeue() else {
				break;
			};
			let Some(mut job) = self.tasks.borrow_mut().remove(&id) else {
				continue;
			};
			let waker = job.waker().clone();
			let result = catch_unwind(AssertUnwindSafe(|| {
				job.poll(&mut Context::from_waker(&waker))
			}));
			match result {
				Ok(Poll::Pending) => {
					self.tasks.borrow_mut().insert(id, job);
				}
				Ok(Poll::Ready(())) => {}
				Err(panic) => {
					job.abandon();
					resume_unwind(panic);
				}
			}
		}
		self.ready.has_ready()
	}
}

impl Drop for Local {
	fn drop(&mut self) {
		let mut tasks = self.tasks.take();
		for job in tasks.values_mut() {
			job.abandon();
		}
		drop(tasks);
	}
}

struct Ready {
	runnable: Mutex<VecDeque<u64>>,
	driver: Mutex<Option<Waker>>,
}

impl Ready {
	// A poisoned lock is a bug: it panics, and says which lock it was.
	fn enqueue(&self, id: u64) {
		let mut runnable = self.runnable.lock().expect("the ready queue was poisoned");
		runnable.push_back(id);
		drop(runnable);
		let driver = self
			.driver
			.lock()
			.expect("the driver waker slot was poisoned")
			.clone();
		// Raised with the lock released: a waker that re-enters here would
		// otherwise deadlock against it.
		if let Some(driver) = driver {
			driver.wake();
		}
	}

	fn dequeue(&self) -> Option<u64> {
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

struct TaskWake {
	id: u64,
	scheduled: Arc<AtomicBool>,
	ready: SyncWeak<Ready>,
}

impl Wake for TaskWake {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		// Already queued, or being polled right now: whichever poll is in flight
		// has not yet stored `false`, so an id for this task is on its way to the
		// queue or is on it. Queueing a second one would only waste a poll.
		if self.scheduled.swap(true, Ordering::AcqRel) {
			return;
		}
		if let Some(ready) = self.ready.upgrade() {
			ready.enqueue(self.id);
		}
	}
}

/// A reactor and the task set it drives.
///
/// It parks the thread calling [`Executor::block_on`] in its own reactor.
pub struct Executor {
	reactor: Reactor,
	local: Rc<Local>,
	driver: Waker,
}

impl Executor {
	#[fastrace::trace]
	pub fn new(config: Config) -> Result<Executor, Error> {
		let reactor = Reactor::new(config)?;
		let driver = reactor.waker();
		let local = Rc::new(Local::new());
		local.drive(&driver);
		Ok(Executor {
			reactor,
			local,
			driver,
		})
	}

	/// Runs the root future, with bounded passes over ready tasks so I/O and timers keep advancing.
	pub fn block_on<F: Future>(&mut self, fut: F) -> Result<F::Output, Error> {
		Executor::run(&self.reactor, &self.local, &self.driver, fut)
	}

	fn run<F: Future>(
		reactor: &Reactor,
		local: &Local,
		driver: &Waker,
		fut: F,
	) -> Result<F::Output, Error> {
		let mut fut = std::pin::pin!(fut);
		let mut cx = Context::from_waker(driver);
		loop {
			// Poll the root first: a backlog of spawned tasks must not delay the
			// future the caller handed in.
			if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
				// The poll above is the one that returned `Ready`, so a task the
				// root future detached on the way to returning is queued and still
				// unpolled. Give it one bounded pass so it reaches its first await,
				// not a full drain: any I/O it submits is driven by the reactor on
				// the next `block_on`, never inside this one.
				local.tick(TICK_BUDGET);
				return Ok(out);
			}
			let more = local.tick(TICK_BUDGET);
			reactor.drive(if more { Some(Duration::ZERO) } else { None })?;
		}
	}

	/// A handle for spawning tasks on this executor.
	pub fn spawner(&self) -> Spawner {
		Spawner {
			local: Rc::downgrade(&self.local),
		}
	}

	/// Spawns a task, or returns an [`Error`] of kind
	/// [`ExecutorClosed`](crate::ErrorKind::ExecutorClosed) if the executor has
	/// been dropped.
	///
	/// A method on the executor rather than on its [`Spawner`]: the executor is
	/// alive for the call, so only the reactor's shutdown can close it.
	pub fn try_spawn<T: 'static>(
		&self,
		future: impl Future<Output = T> + 'static,
	) -> Result<Task<T>, Error> {
		self.spawner().try_spawn(future)
	}

	/// Spawns a task, panicking if the executor has been dropped.
	pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Task<T> {
		self.spawner().spawn(future)
	}

	pub(crate) fn reactor(&self) -> &Reactor {
		&self.reactor
	}
}

impl Drop for Executor {
	fn drop(&mut self) {
		// Shut the reactor down first so in-flight I/O settles before task futures release their Completions.
		let _ = self.reactor.shutdown(true);
	}
}
