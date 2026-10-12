//! The executor a completion reactor's tasks run on, and the scopes whose
//! tasks borrow from their caller.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak as SyncWeak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use completion_rt::{BorrowedDescriptor, Lease, Memory, Reactor, Submitter as LowSubmitter};

use crate::io::Lent;
use crate::{Error, Submitter, Timers};

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
	local: Weak<Local<'static>>,
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

/// A task set, and the queue of ids a wake has made runnable.
///
/// The executor's set is `'static` and lives as long as the executor; a
/// [`Scope`]'s is bounded by the scope, which cancels what is left in it.
pub(crate) struct Local<'a> {
	tasks: RefCell<HashMap<u64, Box<dyn Runnable + 'a>>>,
	ready: Arc<Ready>,
	next_id: Cell<u64>,
}

impl<'a> Local<'a> {
	pub(crate) fn new() -> Local<'a> {
		Local {
			tasks: RefCell::new(HashMap::new()),
			ready: Arc::new(Ready {
				runnable: Mutex::new(VecDeque::new()),
				driver: Mutex::new(None),
			}),
			next_id: Cell::new(0),
		}
	}

	pub(crate) fn spawn<T: 'a>(
		&self,
		future: impl Future<Output = T> + 'a,
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
			.unwrap_or_else(PoisonError::into_inner);
		if slot
			.as_ref()
			.is_none_or(|current| !current.will_wake(driver))
		{
			*slot = Some(driver.clone());
		}
	}

	/// Drops the tasks still in the set as a cancellation: their operations are
	/// given up, not abandoned.
	pub(crate) fn cancel_all(&self) {
		let tasks = std::mem::take(&mut *self.tasks.borrow_mut());
		completion_rt::cancel_task_future(tasks);
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

impl Drop for Local<'_> {
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
	// A waker can run while another task unwinds a panic, so a poisoned lock must
	// not turn that wake into a second panic: take the inner queue back from the
	// poison instead of propagating it.
	fn enqueue(&self, id: u64) {
		let mut runnable = self.runnable.lock().unwrap_or_else(PoisonError::into_inner);
		runnable.push_back(id);
		drop(runnable);
		let driver = self
			.driver
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
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

/// A reactor, its submitter, and the task set they drive.
///
/// Built by [`Executor::new`], it parks the thread calling
/// [`Executor::block_on`] in its own reactor. Built by [`Executor::embedded`],
/// it also serves a caller that owns the poll loop, through
/// [`Executor::poll_fd`] and [`Executor::turn`].
pub struct Executor {
	// Drop the reactor first so in-flight I/O settles before task futures release their Completions.
	reactor: Reactor,
	submitter: LowSubmitter,
	timers: Timers,
	local: Rc<Local<'static>>,
	driver: Waker,
}

impl Executor {
	pub fn new(config: Config) -> io::Result<Executor> {
		let (reactor, submitter) = Reactor::new(config.into_rt())?;
		Ok(Executor::assemble(reactor, submitter))
	}

	/// An executor whose reactor never parks, for a caller that waits on
	/// [`Executor::poll_fd`] and calls [`Executor::turn`] itself.
	pub fn embedded(config: Config) -> io::Result<Executor> {
		let (reactor, submitter) = Reactor::embedded(config.into_rt())?;
		Ok(Executor::assemble(reactor, submitter))
	}

	fn assemble(reactor: Reactor, submitter: LowSubmitter) -> Executor {
		let driver = Waker::from(Arc::new(reactor.waker()));
		let local = Rc::new(Local::new());
		local.drive(&driver);
		let timers = Timers(reactor.timers());
		Executor {
			reactor,
			submitter,
			timers,
			local,
			driver,
		}
	}

	/// Runs the root future, with bounded passes over ready tasks so I/O and timers keep advancing.
	pub fn block_on<F: Future>(&mut self, fut: F) -> io::Result<F::Output> {
		Executor::run(&mut self.reactor, &self.local, &self.driver, fut)
	}

	fn run<F: Future>(
		reactor: &mut Reactor,
		local: &Local<'static>,
		driver: &Waker,
		fut: F,
	) -> io::Result<F::Output> {
		reactor.enter_reactor();
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
			reactor.poll(if more { Some(Duration::ZERO) } else { None })?;
		}
	}

	/// A handle for spawning tasks on this executor.
	pub fn spawner(&self) -> Spawner {
		Spawner {
			local: Rc::downgrade(&self.local),
		}
	}

	/// Runs `body` with a [`Scope`] whose tasks may borrow from the caller.
	///
	/// The tasks are driven by the scope future, so they advance while the body
	/// awaits. Whatever is still in flight when the body returns is dropped
	/// before this does, which is what lets a task hold a borrow rather than a
	/// shared owner of the memory it transfers through.
	pub fn scope<'s, F, Fut, R>(&mut self, body: F) -> io::Result<R>
	where
		F: FnOnce(Scope<'s>) -> Fut,
		Fut: Future<Output = R> + 's,
	{
		/// The barrier: on the way out, normal or unwinding, waits until no
		/// operation under the lease is left in the kernel.
		struct Settle<'c> {
			reactor: &'c mut Reactor,
			submitter: &'c LowSubmitter,
			lease: Arc<Lease>,
		}

		impl Drop for Settle<'_> {
			fn drop(&mut self) {
				while self.submitter.settle(&self.lease) {
					if let Err(error) = self.reactor.poll(None) {
						// Returning would end the borrow while the kernel may still
						// write through it.
						log::error!(error:%; "the reactor failed while a scope waited out its lent memory");
						std::process::abort();
					}
				}
			}
		}

		let lease = Arc::new(self.submitter.lease());
		let local = Rc::new(Local::new());
		let body = body(Scope {
			local: Rc::clone(&local),
			lease: Arc::clone(&lease),
		});
		let future = ScopeFuture {
			local,
			body: Some(Box::pin(body)),
		};
		let settle = Settle {
			reactor: &mut self.reactor,
			submitter: &self.submitter,
			lease,
		};
		let result = Executor::run(settle.reactor, &self.local, &self.driver, future);
		drop(settle);
		result
	}

	/// A handle for submitting operations.
	pub fn submitter(&self) -> Submitter {
		Submitter(self.submitter.clone())
	}

	/// The timer wheel of this executor's reactor, for [`Timeout::with_timers`](crate::Timeout::with_timers).
	pub fn timers(&self) -> Timers {
		self.timers.clone()
	}

	/// The descriptor to wait on between passes of an [`Executor::embedded`]
	/// executor. Any other fails with [`io::ErrorKind::Unsupported`].
	pub fn poll_fd(&self) -> io::Result<BorrowedDescriptor<'_>> {
		self.reactor.poll_fd()
	}

	/// One pass of an [`Executor::embedded`] executor's reactor, for a caller
	/// that waits on [`Executor::poll_fd`] with its own poller: it drains the
	/// descriptor the poller woke on, runs the tasks spawned through
	/// [`Executor::spawner`], then hands what they submitted to the reactor and
	/// reaps. Any other executor fails with [`io::ErrorKind::Unsupported`].
	///
	/// Returns how long the caller may wait on [`Executor::poll_fd`] before
	/// calling this again: [`Duration::ZERO`] while tasks are still ready, so the
	/// caller comes straight back; otherwise the time to the next timer
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
		if self.local.ready.has_ready() {
			Ok(Some(Duration::ZERO))
		} else {
			Ok(deadline)
		}
	}
}

/// Tasks borrowing the scope that spawned them.
///
/// [`spawn`](crate::spawn) cannot take a future that holds a reference: the
/// executor's task set outlives the call that filled it, so its futures are
/// `'static`. A scope is the other half of that trade — the tasks it hands out
/// are bounded by the scope, which drives them itself and drops whatever is
/// still in flight before it returns. That is what lets a task hold a borrow of
/// the caller's memory instead of a shared owner of it, which is the shape a
/// hypervisor needs: the guest's pages are mapped by someone else and cannot be
/// moved into an `Arc`.
///
/// Tasks are polled by the scope future, so they advance while the body waits
/// on them; a body that computes without awaiting holds them up, exactly as it
/// holds up everything else on the thread.
///
/// Handed to the body [`Executor::scope`] runs, and gone when that body
/// returns: a handle taken from here cannot outlive the borrow it was made
/// with.
///
/// Passed by value, not by reference: an `async` body has to move the handle in
/// to keep it across an `await`, and a reference would tie the body to the
/// closure's own frame rather than the scope's.
#[derive(Clone)]
pub struct Scope<'a> {
	local: Rc<Local<'a>>,
	lease: Arc<Lease>,
}

impl<'a> Scope<'a> {
	/// Lends `memory` to operations started inside this scope.
	///
	/// The operations are tied to the scope: one submitted after it ends is
	/// refused, and the scope does not return while one is still in the kernel —
	/// even one whose future was forgotten.
	pub fn lend(&self, memory: impl Memory + 'a) -> Lent<'a> {
		let memory: Arc<dyn Memory + 'a> = Arc::new(memory);
		// SAFETY: only the lifetime bound changes. The erased `Arc` reaches the
		// reactor under this scope's lease, and `Executor::scope` — a plain call,
		// which cannot be forgotten — settles the lease before returning, so every
		// operation naming the memory has left the kernel and dropped its clone
		// while `'a` still holds. Every other clone is typed `Lent<'a>` or sits in
		// a `Transfer<'a, _>`, both bounded by `'a`.
		let memory: Arc<dyn Memory> = unsafe { std::mem::transmute(memory) };
		Lent::lent(memory, Arc::clone(&self.lease))
	}

	/// Spawns `future` on this scope.
	///
	/// The task runs whether or not its handle is awaited. Dropping the handle
	/// only gives up the result; the task keeps running until the scope ends,
	/// and is dropped with it.
	pub fn spawn<T: 'a>(&self, future: impl Future<Output = T> + 'a) -> ScopeTask<'a, T> {
		let (join, _) = self.local.spawn(future);
		ScopeTask {
			join,
			scope: PhantomData,
		}
	}
}

/// A task on a [`Scope`], awaited for its result.
///
/// Unlike [`Task`], dropping this does not cancel the task: a scope has an end
/// to hang cancellation on, so the handle does not have to be one.
pub struct ScopeTask<'a, T> {
	join: Rc<RefCell<Join<T>>>,
	/// Ties the handle to the scope, which is what bounds the borrow.
	scope: PhantomData<&'a ()>,
}

impl<T> Future for ScopeTask<'_, T> {
	type Output = T;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
		poll_join(&self.join, cx)
	}
}

impl<T> Drop for ScopeTask<'_, T> {
	fn drop(&mut self) {
		let waiting = matches!(*self.join.borrow(), Join::Waiting(_));
		if waiting {
			let previous = self.join.replace(Join::Detached);
			drop(previous);
		}
	}
}

/// The future a [`Scope`] body is wrapped in: it polls the tasks, then the body.
struct ScopeFuture<'a, Fut: Future> {
	local: Rc<Local<'a>>,
	body: Option<Pin<Box<Fut>>>,
}

impl<Fut: Future> Future for ScopeFuture<'_, Fut> {
	type Output = Fut::Output;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Fut::Output> {
		let this = self.get_mut();
		let local = &this.local;
		// The tasks wake this future, not the executor's task set: the waker the
		// body was polled with is the one they have to raise.
		local.drive(cx.waker());
		let more = local.tick(TICK_BUDGET);
		let body = this
			.body
			.as_mut()
			.expect("a scope is polled after its body returned");
		match body.as_mut().poll(cx) {
			Poll::Ready(output) => {
				this.body = None;
				// Still in flight, so cancelled here: the borrow they hold dies with
				// the scope, and an operation they started is given up and waited
				// out by the scope's barrier. Also what breaks the cycle a task
				// holding a `Scope` clone makes with the set.
				local.cancel_all();
				Poll::Ready(output)
			}
			Poll::Pending => {
				// A task left over from a spent budget has not run yet. One the
				// body spawned on this poll raised the waker itself.
				if more {
					cx.waker().wake_by_ref();
				}
				Poll::Pending
			}
		}
	}
}

impl<Fut: Future> Drop for ScopeFuture<'_, Fut> {
	fn drop(&mut self) {
		// The body first: it may hold handles into the tasks.
		self.body = None;
		self.local.cancel_all();
	}
}
