//! The thread's global executor.
//!
//! Each thread has its own executor. It is created on the first call to
//! [`block_on`], [`scope`], [`spawn`], [`try_spawn`] or [`submitter`], with
//! `Config::default()`. It is never replaced, and it lives until the thread
//! exits.
//!
//! [`block_on`] cannot be nested: a call made while the executor is already
//! running, such as from inside a `block_on`, panics. A task running under the
//! executor can call [`spawn`] or [`try_spawn`].
//!
//! None of them may be used from a destructor that runs during the thread's
//! teardown. The thread-local is already destroyed by then, and the call panics.

use std::cell::{OnceCell, RefCell};
use std::future::Future;
use std::io;

use crate::{Config, Error, Executor, Scope, Spawner, Submitter, Task};

thread_local! {
	static EXECUTOR: RefCell<Option<Executor>> = const { RefCell::new(None) };
	static HANDLES: OnceCell<(Spawner, Submitter)> = const { OnceCell::new() };
}

/// Creates the executor and its handles on the first call on this thread, and
/// returns the handles.
///
/// The handles are set only together with the executor, so once `HANDLES` is
/// set, `EXECUTOR` is too, and `EXECUTOR` is not borrowed by this path.
fn handles<R>(f: impl FnOnce(&Spawner, &Submitter) -> R) -> R {
	HANDLES.with(|handles| {
		let (spawner, submitter) = handles.get_or_init(|| {
			let executor = Executor::new(Config::default())
				.expect("the thread's global executor could not be created");
			let handles = (executor.spawner(), executor.submitter());
			EXECUTOR.with(|slot| *slot.borrow_mut() = Some(executor));
			handles
		});
		f(spawner, submitter)
	})
}

fn initialize() {
	handles(|_, _| ());
}
/// Runs `future` to completion on this thread's global executor.
///
/// Panics if the executor is already running, as it is when called from inside
/// another `block_on`.
pub fn block_on<F: Future>(future: F) -> io::Result<F::Output> {
	initialize();
	EXECUTOR.with(|slot| {
		let Ok(mut slot) = slot.try_borrow_mut() else {
			panic!("the thread's global executor is already running; block_on cannot be nested");
		};
		slot.as_mut().expect("initialized above").block_on(future)
	})
}

/// Spawns `future` on this thread's global executor.
///
/// Safe to call from a task while a `block_on` is running. Panics if the executor
/// has been dropped.
pub fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> Task<T> {
	let spawner = handles(|spawner, _| spawner.clone());
	spawner.spawn(future)
}

/// Spawns `future` on this thread's global executor, or returns an
/// [`Error`](crate::Error) of kind [`ExecutorClosed`](crate::ErrorKind::ExecutorClosed)
/// if the executor has been dropped.
///
/// Safe to call from a task while a `block_on` is running.
pub fn try_spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> Result<Task<T>, Error> {
	let spawner = handles(|spawner, _| spawner.clone());
	spawner.try_spawn(future)
}

/// The submitter of this thread's global executor, which
/// [`Facade::new`](crate::Facade::new), [`Facade::bind`](crate::Facade::bind)
/// and [`Timeout::new`](crate::Timeout::new) submit through.
///
/// Safe to call from a task while a `block_on` is running. An operation
/// submitted after the executor has been dropped fails.
pub fn submitter() -> Submitter {
	handles(|_, submitter| submitter.clone())
}

/// Runs `body` with a scope on this thread's global executor, so the tasks it
/// spawns may borrow from the caller.
///
/// The scope returns once `body` does, dropping whatever is still in flight.
/// Panics if the executor is already running, as it is when called from inside
/// another [`block_on`] or `scope`.
pub fn scope<'a, F, Fut, R>(body: F) -> io::Result<R>
where
	F: FnOnce(Scope<'a>) -> Fut,
	Fut: Future<Output = R> + 'a,
{
	initialize();
	EXECUTOR.with(|slot| {
		let Ok(mut slot) = slot.try_borrow_mut() else {
			panic!("the thread's global executor is already running; scope cannot be nested");
		};
		slot.as_mut().expect("initialized above").scope(body)
	})
}
