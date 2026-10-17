//! Thread-local default [`Executor`].
//!
//! Each thread lazily initializes its own [`Executor`] with [`Config::default`]
//! on the first call to [`block_on`], [`spawn`], [`try_spawn`], [`Facade::new`](crate::Facade::new),
//! or [`Timeout::new`](crate::Timeout::new), and keeps it until the thread exits.
//!
//! [`block_on`] cannot be nested on the same thread; tasks running inside
//! [`block_on`] can spawn additional tasks with [`spawn`] or [`try_spawn`].
//! Do not call these functions from thread-local destructors during thread teardown.

use std::cell::{OnceCell, RefCell};
use std::future::Future;

use crate::reactor::Reactor;

use crate::{Config, Error, Executor, Spawner, Task};

thread_local! {
	static EXECUTOR: RefCell<Option<Executor>> = const { RefCell::new(None) };
	static HANDLES: OnceCell<(Spawner, Reactor)> = const { OnceCell::new() };
}

/// Creates the executor and its handles on the first call on this thread, and
/// returns the handles.
///
/// The handles are set only together with the executor, so once `HANDLES` is
/// set, `EXECUTOR` is too, and `EXECUTOR` is not borrowed by this path.
fn handles<R>(f: impl FnOnce(&Spawner, &Reactor) -> R) -> R {
	HANDLES.with(|handles| {
		let (spawner, reactor) = handles.get_or_init(|| {
			let executor = Executor::new(Config::default())
				.expect("the thread's global executor could not be created");
			let handles = (executor.spawner(), executor.reactor().clone());
			EXECUTOR.with(|slot| *slot.borrow_mut() = Some(executor));
			handles
		});
		f(spawner, reactor)
	})
}

/// Runs `future` to completion on the current thread's default [`Executor`].
///
/// # Panics
/// Panics if called from inside another [`block_on`] on the same thread, or if
/// the default executor could not be created.
pub fn block_on<F: Future>(future: F) -> Result<F::Output, Error> {
	handles(|_, _| ());
	EXECUTOR.with(|slot| {
		let mut slot = slot
			.try_borrow_mut()
			.expect("the thread's global executor is already running; block_on cannot be nested");
		slot.as_mut().expect("initialized above").block_on(future)
	})
}

/// Spawns a thread-local task on the current thread's default [`Executor`].
///
/// Can be called before or during [`block_on`]. Panics if the thread's executor
/// has already been destroyed.
pub fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> Task<T> {
	handles(|spawner, _| spawner.spawn(future))
}

/// Spawns a thread-local task on the current thread's default [`Executor`], or
/// returns [`ErrorKind::ExecutorClosed`](crate::ErrorKind::ExecutorClosed) if
/// the thread's executor has been destroyed.
pub fn try_spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> Result<Task<T>, Error> {
	handles(|spawner, _| spawner.try_spawn(future))
}

/// Returns the [`Reactor`] of the current thread's default [`Executor`].
pub(crate) fn reactor() -> Reactor {
	handles(|_, reactor| reactor.clone())
}
