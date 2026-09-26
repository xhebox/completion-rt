//! The executor a completion reactor's tasks run on.

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use async_executor::{LocalExecutor, Task};

use crate::core::ReactorWaker;
use crate::{Config, Event, Reactor, Submitter};

thread_local! {
	static EXECUTOR: LocalExecutor<'static> = LocalExecutor::new();
}

/// The calling thread's tasks, as a handle a task or a device can hold.
///
/// The tasks belong to the thread they run on: a `LocalExecutor` is `!Send`,
/// so it is made where it runs and this is a handle to it, created on first
/// use and dropped when the thread exits — taking the tasks still in flight
/// with it. The `'static` is fabricated: a task spawned there is `'static` and
/// has to name the executor it runs on, but the slot grants no lifetime that
/// outlives the closure. It holds because the reference cannot leave the
/// thread (`LocalExecutor` is `!Sync`, so `&LocalExecutor` is not `Send`) and
/// because `with` refuses a slot whose destructor is already running, where a
/// task's own `Drop` would otherwise reach a half-destroyed executor.
///
/// An [`Executor`] is the only thing that hands one out, and a holder clones
/// it: it is what a device spawns its own tasks on, and what a task carries to
/// spawn the next. A device that has to wait for its tasks keeps them through
/// [`crate::Tasks`], which is built from one of these.
#[derive(Clone)]
pub struct Spawner {
	executor: &'static LocalExecutor<'static>,
	/// The reactor wake-up of the thread those tasks run on.
	event: Event,
	/// What a task spawned here polls its future with, behind the task's own
	/// waker.
	waker: Arc<ReactorWaker>,
}

impl Spawner {
	/// Spawns a task on the thread.
	///
	/// The task polls its future with the reactor's wake-up behind its own: a
	/// wake from another thread, or from a primitive that is not the reactor,
	/// queues the task and has to reach the driver parked in `Reactor::poll`
	/// as well, or the task waits for whatever wakes the driver next.
	pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> Task<T> {
		let waker = Arc::clone(&self.waker);
		self.executor.spawn(async move {
			let mut future = std::pin::pin!(future);
			// The task's own waker and the one wrapping it, made on the first
			// poll and remade only if the task's changes: async-executor's is
			// the same waker for the life of the task.
			let mut task: Option<(Waker, Waker)> = None;
			std::future::poll_fn(move |cx| {
				if task
					.as_ref()
					.is_none_or(|(own, _)| !own.will_wake(cx.waker()))
				{
					task = Some((cx.waker().clone(), waker.around(cx.waker())));
				}
				let (_, combined) = task.as_ref().unwrap();
				future.as_mut().poll(&mut Context::from_waker(combined))
			})
			.await
		})
	}

	/// The thread's reactor wake-up; what a task set has to raise to be run.
	pub(super) fn event(&self) -> Event {
		self.event.clone()
	}
}

/// A thread's executor: the reactor its operations are submitted to, and the
/// tasks they are awaited on.
///
/// The tasks are the thread's ([`Spawner`]), because a `LocalExecutor` is
/// `!Send`: it is made where it runs, and this type only names it. The reactor
/// is `Send`, so the side that spawns a thread builds the executor and hands it
/// over.
pub struct Executor {
	reactor: Reactor,
	submitter: Submitter,
	/// Made once, here: a task's waker wraps a clone of it, and `block_on`
	/// polls its future with one, so neither path allocates per poll.
	waker: Arc<ReactorWaker>,
}

impl Executor {
	/// Creates the reactor.
	pub fn new(config: Config) -> io::Result<Executor> {
		let (reactor, submitter) = Reactor::new(config)?;
		let waker = Arc::new(reactor.waker());
		Ok(Executor {
			reactor,
			submitter,
			waker,
		})
	}

	/// The submit side, for whatever the tasks are built from.
	pub fn submitter(&self) -> Submitter {
		self.submitter.clone()
	}

	/// This reactor's fallback drops; see [`Reactor::fallback_drops`].
	pub fn fallback_drops(&self) -> u64 {
		self.reactor.fallback_drops()
	}

	/// The thread's tasks, for a task or a device that outlives this call. It is
	/// the only way to get one.
	pub fn spawner(&self) -> Spawner {
		Spawner {
			executor: EXECUTOR
				.with(|executor| unsafe { &*(executor as *const LocalExecutor<'static>) }),
			event: self.submitter.event(),
			waker: Arc::clone(&self.waker),
		}
	}

	/// Runs `fut` to completion: the ready tasks, then `fut`, then a wait on the
	/// reactor.
	///
	/// Every ready task runs before the future is polled: a tick readies
	/// further tasks, so `try_tick` loops until none are left. The wait is the
	/// reactor's own: a landing completion and anything the tasks raise (a
	/// channel send, a [`crate::Event`]) both reach it, which is what a future
	/// waiting on either is woken by. The future's waker is the reactor's own
	/// wake-up, so a wake from off this thread also ends it.
	pub fn block_on<F: Future>(&mut self, fut: F) -> io::Result<F::Output> {
		// This thread is the reactor's driver from here on: a completion
		// dropped on it has nobody else to reap for it.
		self.reactor.enter_reactor();
		let mut fut = std::pin::pin!(fut);
		let waker = Waker::from(Arc::clone(&self.waker));
		let mut cx = Context::from_waker(&waker);
		let spawner = self.spawner();
		loop {
			while spawner.executor.try_tick() {}
			match fut.as_mut().poll(&mut cx) {
				Poll::Ready(out) => return Ok(out),
				Poll::Pending => self.reactor.poll(None)?,
			}
		}
	}
}
