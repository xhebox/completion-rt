//! The executor a completion reactor's tasks run on.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex, PoisonError, Weak as SyncWeak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use completion_rt::{BorrowedDescriptor, Reactor, Submitter as LowSubmitter};

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

/// A handle for spawning thread-local tasks while its executor exists.
#[derive(Clone)]
pub struct Spawner {
	local: Weak<Local>,
}

impl Spawner {
	/// Panics if the executor has been dropped.
	pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Task<T> {
		let local = self
			.local
			.upgrade()
			.filter(|local| !local.ready.state.lock().unwrap().closed)
			.expect("the executor was dropped before spawning a task");
		let id = local.next_id.get();
		local
			.next_id
			.set(id.checked_add(1).expect("task id overflow"));
		let join = Rc::new(RefCell::new(Join::Waiting(None)));
		let waker = Waker::from(Arc::new(TaskWake {
			id,
			ready: Arc::downgrade(&local.ready),
		}));
		local.tasks.borrow_mut().insert(
			id,
			Box::new(Job {
				future: Some(Box::pin(future)),
				join: Rc::clone(&join),
				waker: waker.clone(),
			}),
		);
		waker.wake_by_ref();
		Task { join, waker }
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
	waker: Waker,
}

impl<F: Future> Runnable for Job<F> {
	fn waker(&self) -> &Waker {
		&self.waker
	}

	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
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
				Ok(Poll::Ready(())) => {
					self.ready.retire(id);
				}
				Err(panic) => {
					self.ready.retire(id);
					job.abandon();
					resume_unwind(panic);
				}
			}
		}
		!self.ready.state.lock().unwrap().queued.is_empty()
	}

	/// Whether a task is runnable but has not been polled this pass.
	fn has_ready(&self) -> bool {
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
	state: Mutex<Queue>,
	driver: Waker,
}

struct Queue {
	runnable: VecDeque<u64>,
	queued: HashSet<u64>,
	closed: bool,
}

impl Ready {
	// A waker can run while another task unwinds a panic, so a poisoned lock must
	// not turn that wake into a second panic: take the inner queue back from the
	// poison instead of propagating it.
	fn enqueue(&self, id: u64) {
		let mut queue = self.state.lock().unwrap_or_else(PoisonError::into_inner);
		if queue.closed || !queue.queued.insert(id) {
			return;
		}
		queue.runnable.push_back(id);
		drop(queue);
		self.driver.wake_by_ref();
	}

	fn dequeue(&self) -> Option<u64> {
		let mut queue = self.state.lock().unwrap_or_else(PoisonError::into_inner);
		while let Some(id) = queue.runnable.pop_front() {
			if queue.queued.remove(&id) {
				return Some(id);
			}
		}
		None
	}

	fn has_ready(&self) -> bool {
		!self
			.state
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.queued
			.is_empty()
	}

	fn retire(&self, id: u64) {
		// A task can wake itself and then finish; queued is authoritative over the remaining deque entry.
		self.state
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.queued
			.remove(&id);
	}

	fn close(&self) {
		let mut queue = self.state.lock().unwrap_or_else(PoisonError::into_inner);
		queue.closed = true;
		queue.queued.clear();
		queue.runnable.clear();
	}
}

struct TaskWake {
	id: u64,
	ready: SyncWeak<Ready>,
}

impl Wake for TaskWake {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		if let Some(ready) = self.ready.upgrade() {
			ready.enqueue(self.id);
		}
	}
}

/// Drives a completion reactor and its tasks on the thread that created it.
pub struct Executor {
	// Drop the reactor first so in-flight I/O settles before task futures release their Completions.
	reactor: Reactor,
	submitter: LowSubmitter,
	local: Rc<Local>,
}

impl Executor {
	pub fn new(config: Config) -> io::Result<Executor> {
		let (reactor, submitter) = Reactor::new(config.into_rt())?;
		Ok(Executor::assemble(reactor, submitter))
	}

	/// An executor for a caller that owns the poll loop: it hands out
	/// [`Executor::poll_fd`] to wait on and [`Executor::turn`] to drive the
	/// reactor once per wake.
	pub fn embedded(config: Config) -> io::Result<Executor> {
		let (reactor, submitter) = Reactor::embedded(config.into_rt())?;
		Ok(Executor::assemble(reactor, submitter))
	}

	fn assemble(reactor: Reactor, submitter: LowSubmitter) -> Executor {
		let driver = Waker::from(Arc::new(reactor.waker()));
		let local = Rc::new(Local {
			tasks: RefCell::new(HashMap::new()),
			ready: Arc::new(Ready {
				state: Mutex::new(Queue {
					runnable: VecDeque::new(),
					queued: HashSet::new(),
					closed: false,
				}),
				driver,
			}),
			next_id: Cell::new(0),
		});
		Executor {
			reactor,
			submitter,
			local,
		}
	}

	pub fn submitter(&self) -> Submitter {
		Submitter(self.submitter.clone())
	}

	pub fn spawner(&self) -> Spawner {
		Spawner {
			local: Rc::downgrade(&self.local),
		}
	}

	/// The descriptor to wait on between passes, for an executor built with
	/// [`Executor::embedded`].
	pub fn poll_fd(&self) -> io::Result<BorrowedDescriptor<'_>> {
		self.reactor.poll_fd()
	}

	/// One pass of the reactor, for a caller that waits on
	/// [`Executor::poll_fd`] with its own poller: it drains the descriptor the
	/// poller woke on, runs the tasks spawned through [`Executor::spawner`],
	/// then hands what they submitted to the reactor and reaps.
	///
	/// Returns how long the caller may wait on [`Executor::poll_fd`] before
	/// calling this again: [`Duration::ZERO`] while tasks are still ready, so
	/// the caller comes straight back; otherwise the time to the next timer
	/// deadline, or `None` if there is none, meaning wait on the descriptor
	/// alone.
	pub fn turn(&mut self) -> io::Result<Option<Duration>> {
		let reactor = &mut self.reactor;
		reactor.enter_reactor();
		// Drain first: a completion or a wake raised after this re-raises the
		// descriptor, so the caller can wait on it again rather than spin.
		reactor.drain_notification()?;
		self.local.tick(TICK_BUDGET);
		let deadline = reactor.turn()?;
		// The pass may have readied tasks — a completion or a due timer — and
		// they must run before the caller waits again.
		if self.local.has_ready() {
			Ok(Some(Duration::ZERO))
		} else {
			Ok(deadline)
		}
	}

	/// Runs the root future, with bounded passes over ready tasks so I/O and timers keep advancing.
	pub fn block_on<F: Future>(&mut self, fut: F) -> io::Result<F::Output> {
		let reactor = &mut self.reactor;
		reactor.enter_reactor();
		let mut fut = std::pin::pin!(fut);
		let mut cx = Context::from_waker(&self.local.ready.driver);
		loop {
			let more = self.local.tick(TICK_BUDGET);
			match fut.as_mut().poll(&mut cx) {
				Poll::Ready(out) => return Ok(out),
				Poll::Pending => reactor.poll(if more { Some(Duration::ZERO) } else { None })?,
			}
		}
	}
}

impl Drop for Executor {
	fn drop(&mut self) {
		self.local.ready.close();
	}
}
