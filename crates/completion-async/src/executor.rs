use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;

use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak as SyncWeak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use slotmap::{DefaultKey, SlotMap};

use completion_backend::Config;

use crate::Result;
use crate::cancel::Scope;
use crate::global;
use crate::reactor::{self, Driver};

const TICK_BUDGET: usize = 64;

/// Handle to a spawned thread-local task.
///
/// Awaiting a `JoinHandle` yields the task's output. Dropping a `JoinHandle`
/// detaches the task, letting it keep running in the background and discarding
/// its output.
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

trait Runnable {
	fn waker(&self) -> &Waker;
	fn scope(&self) -> &Scope;
	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()>;
	fn abandon(&mut self);
}

struct Job<F: Future> {
	future: Pin<Box<F>>,
	join: Rc<RefCell<Join<F::Output>>>,
	scheduled: Arc<AtomicBool>,
	waker: Waker,
	scope: Scope,
}

impl<F: Future> Runnable for Job<F> {
	fn waker(&self) -> &Waker {
		&self.waker
	}

	fn scope(&self) -> &Scope {
		&self.scope
	}

	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
		// `AcqRel` swap rather than a store so any wake that observed `scheduled == true`
		// and returned without re-queueing synchronizes-with this poll.
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

pub(crate) struct Local {
	tasks: RefCell<SlotMap<DefaultKey, Option<Box<dyn Runnable>>>>,
	ready: Arc<Ready>,
	root: Waker,
	scope: RefCell<Scope>,
}

impl Local {
	pub(crate) fn new(driver: Waker, root: Scope) -> Local {
		let ready = Arc::new(Ready {
			runnable: Mutex::new(VecDeque::new()),
			driver,
			parked: AtomicBool::new(false),
			root_woken: AtomicBool::new(true),
		});
		Local {
			tasks: RefCell::new(SlotMap::new()),
			root: Waker::from(Arc::clone(&ready)),
			scope: RefCell::new(root),
			ready,
		}
	}

	pub(crate) fn current_scope(&self) -> Scope {
		self.scope.borrow().clone()
	}

	pub(crate) fn spawn<T: 'static>(
		&self,
		scope: Scope,
		future: impl Future<Output = T> + 'static,
	) -> Rc<RefCell<Join<T>>> {
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
				future: Box::pin(future),
				join: Rc::clone(&join),
				scheduled,
				waker: waker.clone(),
				scope,
			}) as Box<dyn Runnable>)
		});
		let waker = queued.expect("the spawn inserted the task");
		waker.wake_by_ref();
		join
	}

	pub(crate) fn tick(&self, budget: usize) -> bool {
		for _ in 0..budget {
			let Some(key) = self.ready.dequeue() else {
				break;
			};
			// Take the task out of its slot while polling so `self.tasks` is not
			// borrowed if the future spawns or wakes tasks reentrantly.
			let Some(mut job) = self.tasks.borrow_mut().get_mut(key).and_then(Option::take) else {
				continue;
			};
			let waker = job.waker().clone();
			let previous = self.scope.replace(job.scope().clone());
			let result = catch_unwind(AssertUnwindSafe(|| {
				job.poll(&mut Context::from_waker(&waker))
			}));
			*self.scope.borrow_mut() = previous;
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

	pub(crate) fn park(&self) {
		self.ready.parked.store(true, Ordering::SeqCst);
	}

	pub(crate) fn unpark(&self) {
		self.ready.parked.store(false, Ordering::SeqCst);
	}

	pub(crate) fn take_root_woken(&self) -> bool {
		self.ready.root_woken.swap(false, Ordering::SeqCst)
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

struct Ready {
	runnable: Mutex<VecDeque<DefaultKey>>,
	driver: Waker,
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
	pub(crate) reactor: RefCell<Option<Driver>>,
	pub(crate) local: Rc<Local>,
	pub(crate) root: Scope,
}

struct Entered<'a> {
	reactor: &'a RefCell<Option<Driver>>,
	previous: Option<Driver>,
}

impl<'a> Entered<'a> {
	fn new(reactor: &'a RefCell<Option<Driver>>, driver: Driver) -> Entered<'a> {
		Entered {
			reactor,
			previous: reactor::enter(Some(driver), |_| ()).0,
		}
	}
}

impl Drop for Entered<'_> {
	fn drop(&mut self) {
		let driver = reactor::REACTOR.with(|slot| slot.borrow_mut().take());
		*self.reactor.borrow_mut() = driver;
		if let Some(previous) = self.previous.take() {
			reactor::enter(Some(previous), |_| ());
		}
	}
}

impl Executor {
	/// Creates a new executor and initializes its underlying I/O backend with `config`.
	#[fastrace::trace]
	pub fn new(config: Config) -> Result<Executor> {
		let reactor = Driver::new(config)?;
		let root = Scope::new();
		let local = Rc::new(Local::new(reactor.waker(), root.clone()));
		Ok(Executor {
			reactor: RefCell::new(Some(reactor)),
			local,
			root,
		})
	}

	/// Drives `fut` and any spawned tasks on the current thread until `fut` completes.
	pub fn block_on<F: Future>(&mut self, fut: F) -> Result<F::Output> {
		self.run(fut)
	}

	pub(crate) fn run<F: Future>(&self, fut: F) -> Result<F::Output> {
		let driver = self
			.reactor
			.borrow_mut()
			.take()
			.expect("the executor's reactor is already running");
		let local = &self.local;
		let _reactor = Entered::new(&self.reactor, driver);
		let _local = global::enter_local(local);
		let mut fut = std::pin::pin!(fut);
		let mut cx = Context::from_waker(&local.root);
		loop {
			local.take_root_woken();
			if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
				// Run one bounded tick so tasks spawned by `fut` just before
				// returning reach their first suspension point.
				local.tick(TICK_BUDGET);
				return Ok(out);
			}
			let more = local.tick(TICK_BUDGET);
			if more || local.take_root_woken() {
				reactor::REACTOR.with(|slot| {
					slot.borrow_mut()
						.as_mut()
						.expect("the executor's reactor is entered")
						.drive(Some(Duration::ZERO))
				})?;
				continue;
			}
			// Store `parked = true` before rechecking `ready` and `root_woken`
			// under `SeqCst` so a concurrent wake either sees `parked` or is seen
			// by the recheck.
			local.park();
			if local.ready.has_ready() || local.take_root_woken() {
				local.unpark();
				reactor::REACTOR.with(|slot| {
					slot.borrow_mut()
						.as_mut()
						.expect("the executor's reactor is entered")
						.drive(Some(Duration::ZERO))
				})?;
				continue;
			}
			let result = reactor::REACTOR.with(|slot| {
				slot.borrow_mut()
					.as_mut()
					.expect("the executor's reactor is entered")
					.drive(None)
			});
			local.unpark();
			result?;
		}
	}

	/// Creates a new child [`Scope`] derived from this executor's root scope.
	pub fn scope(&self) -> Scope {
		self.root.child()
	}

	/// Spawns a thread-local task on this executor, in the scope active where it
	/// is spawned.
	pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
		let join = self.local.spawn(self.local.current_scope(), future);
		JoinHandle { join }
	}
}

impl Drop for Executor {
	fn drop(&mut self) {
		if let Some(driver) = self.reactor.get_mut().take() {
			let _ = driver.shutdown(true);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{Local, TICK_BUDGET};

	use crate::cancel::Scope;

	use std::cell::RefCell;
	use std::rc::Rc;
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::task::{Poll, Wake, Waker};

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
		let local = Local::new(Waker::from(Arc::clone(&ring)), Scope::new());
		(ring, local)
	}

	/// A wake raised on the executor thread has the loop itself to answer it:
	/// it must not ring the reactor, whose notify is a syscall on the portable
	/// and IOCP backends.
	#[test]
	fn a_spawn_does_not_ring_the_reactor() {
		let (ring, local) = local();
		let join = local.spawn(Scope::new(), std::future::pending::<()>());
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
		local.root.wake_by_ref();
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
		let filed = Rc::new(RefCell::new(None::<Waker>));
		let slot = Rc::clone(&filed);
		let join = local.spawn(
			Scope::new(),
			std::future::poll_fn(move |cx| -> Poll<()> {
				*slot.borrow_mut() = Some(cx.waker().clone());
				Poll::Pending
			}),
		);
		local.tick(TICK_BUDGET);
		local.park();
		filed
			.borrow()
			.as_ref()
			.expect("the task filed its waker")
			.wake_by_ref();
		assert_eq!(
			ring.0.load(Ordering::Acquire),
			1,
			"a task wake while parked did not ring the reactor"
		);
		assert!(local.ready.has_ready(), "the task wake was not queued");
		drop(join);
	}
}
