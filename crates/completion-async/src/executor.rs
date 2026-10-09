//! The executors a completion reactor's tasks run on.
//!
//! The two shapes differ in who owns the poll loop: [`BlockingExecutor`] parks
//! the thread that called [`Executor::block_on`] in the reactor, while
//! [`EmbeddedExecutor`] hands its caller [`EmbeddedExecutor::poll_fd`] and
//! [`EmbeddedExecutor::turn`]. [`ExecutorEnum`] names either of them, so code
//! that needs nothing beyond the shared surface can be written once against
//! [`Executor`].

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak as SyncWeak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use completion_rt::{BorrowedDescriptor, Reactor, Submitter as LowSubmitter};
use enum_dispatch::enum_dispatch;

use crate::Submitter;

const TICK_BUDGET: usize = 64;

/// Configure the executor's reactor.
#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub entries: u32,
	/// Completions appear only while polling (`io_uring` IOPOLL).
	pub iopoll: bool,
	/// A kernel thread consumes submissions (`io_uring` SQPOLL).
	pub sqpoll: bool,
}

impl Default for Config {
	fn default() -> Self {
		Config {
			entries: 256,
			iopoll: false,
			sqpoll: false,
		}
	}
}

impl Config {
	fn into_rt(self) -> completion_rt::Config {
		completion_rt::Config {
			entries: self.entries,
			iopoll: self.iopoll,
			sqpoll: self.sqpoll,
		}
	}
}

/// Returned by [`Spawner::try_spawn`] once the executor it came from has dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorClosed;

impl fmt::Display for ExecutorClosed {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("the executor was dropped before spawning a task")
	}
}

impl std::error::Error for ExecutorClosed {}

/// A handle for spawning thread-local tasks while its executor exists.
#[derive(Clone)]
pub struct Spawner {
	local: Weak<Local>,
}

impl Spawner {
	/// Spawns a task, or returns [`ExecutorClosed`] if the executor has been dropped.
	pub fn try_spawn<T: 'static>(
		&self,
		future: impl Future<Output = T> + 'static,
	) -> Result<Task<T>, ExecutorClosed> {
		let local = self.local.upgrade().ok_or(ExecutorClosed)?;
		let id = local.next_id.get();
		local
			.next_id
			.set(id.checked_add(1).expect("task id overflow"));
		let join = Rc::new(RefCell::new(Join::Waiting(None)));
		let scheduled = Arc::new(AtomicBool::new(false));
		let waker = Waker::from(Arc::new(TaskWake {
			id,
			scheduled: Arc::clone(&scheduled),
			ready: Arc::downgrade(&local.ready),
		}));
		local.tasks.borrow_mut().insert(
			id,
			Box::new(Job {
				future: Some(Box::pin(future)),
				join: Rc::clone(&join),
				scheduled: Arc::clone(&scheduled),
				waker: waker.clone(),
			}),
		);
		waker.wake_by_ref();
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
					unreachable!()
				};
				Poll::Ready(output)
			}
			Join::Gone => panic!("the executor was dropped before the task finished"),
			Join::Taken | Join::Detached | Join::Cancelled => {
				panic!("the task result is no longer available")
			}
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

enum Join<T> {
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
			completion_rt::cancel_task_future(self.future.take());
			return Poll::Ready(());
		}
		let output = match self.future.as_mut().unwrap().as_mut().poll(cx) {
			Poll::Pending => return Poll::Pending,
			Poll::Ready(output) => output,
		};
		if matches!(*self.join.borrow(), Join::Cancelled) {
			completion_rt::cancel_task_future((self.future.take(), output));
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
			Join::Cancelled => completion_rt::cancel_task_future(output),
			_ => unreachable!(),
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

struct Local {
	tasks: RefCell<HashMap<u64, Box<dyn Runnable>>>,
	ready: Arc<Ready>,
	next_id: Cell<u64>,
}

impl Local {
	fn tick(&self, budget: usize) -> bool {
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
	driver: Waker,
}

impl Ready {
	// A waker can run while another task unwinds a panic, so a poisoned lock must
	// not turn that wake into a second panic: take the inner queue back from the
	// poison instead of propagating it.
	fn enqueue(&self, id: u64) {
		let mut runnable = self.runnable.lock().unwrap_or_else(PoisonError::into_inner);
		runnable.push_back(id);
		drop(runnable);
		self.driver.wake_by_ref();
	}

	fn dequeue(&self) -> Option<u64> {
		self.runnable
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.pop_front()
	}

	fn has_ready(&self) -> bool {
		!self
			.runnable
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
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

/// What either executor shape is made of: the reactor, its submitter, and the
/// task set they share.
struct Core {
	// Drop the reactor first so in-flight I/O settles before task futures release their Completions.
	reactor: Reactor,
	submitter: LowSubmitter,
	local: Rc<Local>,
}

impl Core {
	fn assemble(reactor: Reactor, submitter: LowSubmitter) -> Core {
		let driver = Waker::from(Arc::new(reactor.waker()));
		let local = Rc::new(Local {
			tasks: RefCell::new(HashMap::new()),
			ready: Arc::new(Ready {
				runnable: Mutex::new(VecDeque::new()),
				driver,
			}),
			next_id: Cell::new(0),
		});
		Core {
			reactor,
			submitter,
			local,
		}
	}

	/// Runs the root future, with bounded passes over ready tasks so I/O and timers keep advancing.
	fn block_on<F: Future>(&mut self, fut: F) -> io::Result<F::Output> {
		let reactor = &mut self.reactor;
		reactor.enter_reactor();
		let mut fut = std::pin::pin!(fut);
		let mut cx = Context::from_waker(&self.local.ready.driver);
		loop {
			// Poll the root first: a backlog of spawned tasks must not delay the
			// future the caller handed in.
			if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
				// The poll above is the one that returned `Ready`, so a task the
				// root future detached on the way to returning is queued and still
				// unpolled. Give it one bounded pass so it reaches its first await,
				// not a full drain: any I/O it submits is driven by the reactor on
				// the next `block_on`, never inside this one.
				self.local.tick(TICK_BUDGET);
				return Ok(out);
			}
			let more = self.local.tick(TICK_BUDGET);
			reactor.poll(if more { Some(Duration::ZERO) } else { None })?;
		}
	}

	fn spawner(&self) -> Spawner {
		Spawner {
			local: Rc::downgrade(&self.local),
		}
	}

	fn submitter(&self) -> Submitter {
		Submitter(self.submitter.clone())
	}
}

/// An executor that parks the thread calling [`Executor::block_on`] in its
/// own reactor.
pub struct BlockingExecutor(Core);

impl BlockingExecutor {
	fn new(config: Config) -> io::Result<BlockingExecutor> {
		let (reactor, submitter) = Reactor::new(config.into_rt())?;
		Ok(BlockingExecutor(Core::assemble(reactor, submitter)))
	}
}

impl Executor for BlockingExecutor {
	fn block_on<F: Future>(&mut self, fut: F) -> io::Result<F::Output> {
		self.0.block_on(fut)
	}

	fn spawner(&self) -> Spawner {
		self.0.spawner()
	}

	fn submitter(&self) -> Submitter {
		self.0.submitter()
	}
}

/// An executor for a caller that owns the poll loop: it hands out
/// [`EmbeddedExecutor::poll_fd`] to wait on and [`EmbeddedExecutor::turn`] to
/// drive the reactor once per wake.
pub struct EmbeddedExecutor(Core);

impl EmbeddedExecutor {
	fn new(config: Config) -> io::Result<EmbeddedExecutor> {
		let (reactor, submitter) = Reactor::embedded(config.into_rt())?;
		Ok(EmbeddedExecutor(Core::assemble(reactor, submitter)))
	}

	/// The descriptor to wait on between passes.
	pub fn poll_fd(&self) -> io::Result<BorrowedDescriptor<'_>> {
		self.0.reactor.poll_fd()
	}

	/// One pass of the reactor, for a caller that waits on
	/// [`EmbeddedExecutor::poll_fd`] with its own poller: it drains the
	/// descriptor the poller woke on, runs the tasks spawned through
	/// [`Executor::spawner`], then hands what they submitted to the reactor and
	/// reaps.
	///
	/// Returns how long the caller may wait on [`EmbeddedExecutor::poll_fd`]
	/// before calling this again: [`Duration::ZERO`] while tasks are still
	/// ready, so the caller comes straight back; otherwise the time to the next
	/// timer deadline, or `None` if there is none, meaning wait on the
	/// descriptor alone.
	pub fn turn(&mut self) -> io::Result<Option<Duration>> {
		let reactor = &mut self.0.reactor;
		reactor.enter_reactor();
		// Drain first: a completion or a wake raised after this re-raises the
		// descriptor, so the caller can wait on it again rather than spin.
		reactor.drain_notification()?;
		self.0.local.tick(TICK_BUDGET);
		let deadline = reactor.turn()?;
		// The pass may have readied tasks — a completion or a due timer — and
		// they must run before the caller waits again.
		if self.0.local.ready.has_ready() {
			Ok(Some(Duration::ZERO))
		} else {
			Ok(deadline)
		}
	}
}

impl Executor for EmbeddedExecutor {
	fn block_on<F: Future>(&mut self, fut: F) -> io::Result<F::Output> {
		self.0.block_on(fut)
	}

	fn spawner(&self) -> Spawner {
		self.0.spawner()
	}

	fn submitter(&self) -> Submitter {
		self.0.submitter()
	}
}

/// Implements [`Executor`].
///
/// [`BlockingExecutor`] parks the calling thread in its own reactor.
///
/// [`EmbeddedExecutor`] owns no poll loop: it hands the caller
/// [`EmbeddedExecutor::poll_fd`] and [`EmbeddedExecutor::turn`] instead.
#[enum_dispatch(Executor)]
pub enum ExecutorEnum {
	Blocking(BlockingExecutor),
	Embedded(EmbeddedExecutor),
}

impl ExecutorEnum {
	pub fn new(config: Config) -> io::Result<Self> {
		Ok(BlockingExecutor::new(config)?.into())
	}

	pub fn embedded(config: Config) -> io::Result<Self> {
		Ok(EmbeddedExecutor::new(config)?.into())
	}
}

#[enum_dispatch]
pub trait Executor {
	/// Runs the root future.
	fn block_on<F: Future>(&mut self, fut: F) -> io::Result<F::Output>;

	/// A handle for spawning tasks on this executor.
	fn spawner(&self) -> Spawner;

	/// A handle for submitting operations.
	fn submitter(&self) -> Submitter;
}
