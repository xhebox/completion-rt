//! Single-threaded task executor driven by a completion reactor.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;

use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak as SyncWeak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use slotmap::{DefaultKey, SlotMap};

use completion_backend::Config;

use crate::Error;
use crate::reactor::Reactor;

const TICK_BUDGET: usize = 64;

/// Handle for spawning thread-local tasks onto an [`Executor`].
#[derive(Clone)]
pub struct Spawner {
	local: Weak<Local>,
}

impl Spawner {
	/// Spawns a thread-local task, or returns [`ErrorKind::ExecutorClosed`](crate::ErrorKind::ExecutorClosed)
	/// if the [`Executor`] has been dropped.
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

	/// Spawns a thread-local task, panicking if the [`Executor`] has been dropped.
	pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Task<T> {
		self.try_spawn(future)
			.expect("the executor was dropped before spawning a task")
	}
}

/// Handle to a spawned thread-local task.
///
/// Awaiting a `Task` yields its output. Dropping a `Task` without calling
/// [`Task::detach`] cancels the task on the next executor tick.
#[must_use = "await the task or call `detach()` to keep it running in the background"]
pub struct Task<T> {
	join: Rc<RefCell<Join<T>>>,
	waker: Waker,
}

impl<T> Task<T> {
	/// Detaches the task to let it keep running in the background, discarding its result.
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

/// A task set, and the queue of keys a wake has made runnable.
pub(crate) struct Local {
	/// A slot per task; a poll takes its task out of the slot and puts it back,
	/// so a running future never leaves the table borrowed and keeps its key.
	tasks: RefCell<SlotMap<DefaultKey, Option<Box<dyn Runnable>>>>,
	ready: Arc<Ready>,
	root: Waker,
}

impl Local {
	pub(crate) fn new(driver: Waker) -> Local {
		let ready = Arc::new(Ready {
			runnable: Mutex::new(VecDeque::new()),
			driver,
			parked: AtomicBool::new(false),
			root_woken: AtomicBool::new(true),
		});
		Local {
			tasks: RefCell::new(SlotMap::new()),
			root: Waker::from(Arc::clone(&ready)),
			ready,
		}
	}

	/// The waker the root future is polled with: a wake marks the future for
	/// another poll, and reaches the reactor only while this thread is parked.
	pub(crate) fn root(&self) -> &Waker {
		&self.root
	}

	pub(crate) fn spawn<T: 'static>(
		&self,
		future: impl Future<Output = T> + 'static,
	) -> (Rc<RefCell<Join<T>>>, Waker) {
		let join = Rc::new(RefCell::new(Join::Waiting(None)));
		let scheduled = Arc::new(AtomicBool::new(false));
		let ready = Arc::downgrade(&self.ready);
		let mut queued = None;
		self.tasks.borrow_mut().insert_with_key(|key| {
			let waker = Waker::from(Arc::new(TaskWake {
				key,
				scheduled: Arc::clone(&scheduled),
				ready,
			}));
			queued = Some(waker.clone());
			Some(Box::new(Job {
				future: Some(Box::pin(future)),
				join: Rc::clone(&join),
				scheduled,
				waker: waker.clone(),
			}) as Box<dyn Runnable>)
		});
		let waker = queued.expect("the spawn inserted the task");
		waker.wake_by_ref();
		(join, waker)
	}

	pub(crate) fn tick(&self, budget: usize) -> bool {
		for _ in 0..budget {
			let Some(key) = self.ready.dequeue() else {
				break;
			};
			// The task leaves its slot for the poll, so the table is not borrowed
			// while the future runs, and goes back into the same slot on `Pending`.
			let Some(mut job) = self.tasks.borrow_mut().get_mut(key).and_then(Option::take) else {
				continue;
			};
			let waker = job.waker().clone();
			let result = catch_unwind(AssertUnwindSafe(|| {
				job.poll(&mut Context::from_waker(&waker))
			}));
			match result {
				Ok(Poll::Pending) => {
					let mut tasks = self.tasks.borrow_mut();
					let slot = tasks
						.get_mut(key)
						.expect("the task's slot went away while it ran");
					*slot = Some(job);
				}
				Ok(Poll::Ready(())) => {
					self.tasks.borrow_mut().remove(key);
				}
				Err(panic) => {
					job.abandon();
					self.tasks.borrow_mut().remove(key);
					resume_unwind(panic);
				}
			}
		}
		self.ready.has_ready()
	}

	/// Parks the loop in the reactor: from here on a wake rings the reactor
	/// instead of only raising the flag.
	pub(crate) fn park(&self) {
		self.ready.parked.store(true, Ordering::SeqCst);
	}

	pub(crate) fn unpark(&self) {
		self.ready.parked.store(false, Ordering::SeqCst);
	}

	/// Takes the root future's wake, returning whether one had been raised since
	/// the last take.
	pub(crate) fn take_root_woken(&self) -> bool {
		self.ready.root_woken.swap(false, Ordering::SeqCst)
	}

	pub(crate) fn has_ready(&self) -> bool {
		self.ready.has_ready()
	}
}

impl Drop for Local {
	fn drop(&mut self) {
		let mut tasks = self.tasks.take();
		for job in tasks.values_mut().filter_map(Option::as_mut) {
			job.abandon();
		}
		drop(tasks);
	}
}

/// The executor's own wake state: the queue a spawned task's wake fills, the
/// root future's wake flag, and whether the executor thread is parked in the
/// reactor.
struct Ready {
	runnable: Mutex<VecDeque<DefaultKey>>,
	/// The reactor's waker, rung only while the thread is parked: a wake raised
	/// on the executor thread has the loop itself to answer it.
	driver: Waker,
	/// Set while the loop is about to block in the reactor.
	parked: AtomicBool,
	/// Set by a wake of the root future, or by a task wake: either one is a
	/// reason to poll the root again before parking.
	root_woken: AtomicBool,
}

impl Ready {
	// A poisoned lock is a bug: it panics, and says which lock it was.
	fn enqueue(&self, key: DefaultKey) {
		self.runnable
			.lock()
			.expect("the ready queue was poisoned")
			.push_back(key);
		// A task woken on the executor thread has the loop itself to run it; the
		// checks before parking see this flag. Only a wake that crosses the park
		// transition reaches the reactor.
		self.root_woken.store(true, Ordering::SeqCst);
		if self.parked.swap(false, Ordering::SeqCst) {
			self.driver.wake_by_ref();
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
			self.driver.wake_by_ref();
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
		// Already queued, or being polled right now: whichever poll is in flight
		// has not yet stored `false`, so a key for this task is on its way to the
		// queue or is on it. Queueing a second one would only waste a poll.
		if self.scheduled.swap(true, Ordering::AcqRel) {
			return;
		}
		if let Some(ready) = self.ready.upgrade() {
			ready.enqueue(self.key);
		}
	}
}

/// Single-threaded async executor and I/O reactor.
pub struct Executor {
	reactor: Reactor,
	local: Rc<Local>,
}

impl Executor {
	/// Creates a new executor and initializes its underlying I/O backend with `config`.
	#[fastrace::trace]
	pub fn new(config: Config) -> Result<Executor, Error> {
		let reactor = Reactor::new(config)?;
		let local = Rc::new(Local::new(reactor.waker()));
		Ok(Executor { reactor, local })
	}

	/// Drives `fut` and any spawned tasks on the current thread until `fut` completes.
	pub fn block_on<F: Future>(&mut self, fut: F) -> Result<F::Output, Error> {
		Executor::run(&self.reactor, &self.local, fut)
	}

	fn run<F: Future>(reactor: &Reactor, local: &Local, fut: F) -> Result<F::Output, Error> {
		let mut fut = std::pin::pin!(fut);
		let mut cx = Context::from_waker(local.root());
		loop {
			// Taken before the poll: a wake raised while the poll runs is what the
			// checks below see, while one already answered here is not.
			local.take_root_woken();
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
			if more || local.take_root_woken() {
				// Work is in hand: reap what the reactor already has instead of
				// sleeping on it.
				reactor.drive(Some(Duration::ZERO))?;
				continue;
			}
			// Park, then check again: a wake that swapped `parked` before this
			// store was seen by neither the check above nor the wake itself, and
			// must not be slept through.
			local.park();
			if local.has_ready() || local.take_root_woken() {
				local.unpark();
				reactor.drive(Some(Duration::ZERO))?;
				continue;
			}
			let result = reactor.drive(None);
			local.unpark();
			result?;
		}
	}

	/// Returns a cloneable [`Spawner`] for this executor.
	pub fn spawner(&self) -> Spawner {
		Spawner {
			local: Rc::downgrade(&self.local),
		}
	}

	/// Spawns a thread-local task on this executor, or returns
	/// [`ErrorKind::ExecutorClosed`](crate::ErrorKind::ExecutorClosed) if the
	/// executor has been shut down.
	pub fn try_spawn<T: 'static>(
		&self,
		future: impl Future<Output = T> + 'static,
	) -> Result<Task<T>, Error> {
		self.spawner().try_spawn(future)
	}

	/// Spawns a thread-local task on this executor, panicking if the executor has been shut down.
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

#[cfg(test)]
mod tests {
	use super::{Local, TICK_BUDGET};

	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::task::{Wake, Waker};

	/// A stand-in for the reactor's waker that counts the times a wake rings it.
	struct Ring(AtomicUsize);

	impl Wake for Ring {
		fn wake(self: Arc<Self>) {
			self.wake_by_ref();
		}

		fn wake_by_ref(self: &Arc<Self>) {
			self.0.fetch_add(1, Ordering::AcqRel);
		}
	}

	fn local() -> (Arc<Ring>, Local) {
		let ring = Arc::new(Ring(AtomicUsize::new(0)));
		let local = Local::new(Waker::from(Arc::clone(&ring)));
		(ring, local)
	}

	/// A wake raised on the executor thread has the loop itself to answer it:
	/// it must not ring the reactor, whose notify is a syscall on the portable
	/// and IOCP backends.
	#[test]
	fn a_spawn_does_not_ring_the_reactor() {
		let (ring, local) = local();
		let (join, _) = local.spawn(std::future::pending::<()>());
		local.tick(TICK_BUDGET);
		assert_eq!(
			ring.0.load(Ordering::Acquire),
			0,
			"a spawn on the executor thread rang the reactor"
		);
		drop(join);
	}

	/// A wake that crosses the park transition is the one that has to reach
	/// the reactor: nothing else would bring the loop back.
	#[test]
	fn a_root_wake_across_the_park_rings_the_reactor() {
		let (ring, local) = local();
		local.park();
		local.root().wake_by_ref();
		assert_eq!(
			ring.0.load(Ordering::Acquire),
			1,
			"a root wake while parked did not ring the reactor"
		);
		assert!(local.take_root_woken(), "the root wake was not recorded");
	}

	/// The same for a task's own wake: the parked loop has no other way to
	/// learn that the queue grew.
	#[test]
	fn a_task_wake_across_the_park_rings_the_reactor() {
		let (ring, local) = local();
		let (join, waker) = local.spawn(std::future::pending::<()>());
		local.tick(TICK_BUDGET);
		local.park();
		waker.wake_by_ref();
		assert_eq!(
			ring.0.load(Ordering::Acquire),
			1,
			"a task wake while parked did not ring the reactor"
		);
		assert!(local.has_ready(), "the task wake was not queued");
		drop(join);
	}
}
