use std::cell::{OnceCell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use completion_backend::{AsDescriptor, Error, Op};

use crate::executor::Local;
use crate::reactor::{self, Slot};
use crate::{Config, Executor, JoinHandle, Result, Scope};

thread_local! {
	static DEFAULT: OnceCell<Executor> = const { OnceCell::new() };
	static LOCAL: RefCell<Option<Rc<Local>>> = const { RefCell::new(None) };
}

fn default<R>(f: impl FnOnce(&Executor) -> R) -> R {
	DEFAULT.with(|cell| {
		let executor = cell.get_or_init(|| {
			Executor::new(Config::default())
				.expect("the thread's global executor could not be created")
		});
		f(executor)
	})
}

/// Runs `future` to completion on the current thread's default [`Executor`],
/// in its root [`Scope`].
///
/// # Panics
/// Panics if called from inside another [`block_on`] on the same thread, or if
/// the default executor could not be created.
pub fn block_on<F: Future>(future: F) -> Result<F::Output> {
	assert!(
		LOCAL.with(|slot| slot.borrow().is_none()),
		"the thread's global executor is already running; block_on cannot be nested"
	);
	default(|executor| executor.run(future))
}

/// Creates a child [`Scope`] of the scope active on this thread, or of a new
/// root scope when no executor is running.
pub fn scope() -> Scope {
	current_scope().unwrap_or_else(Scope::new).child()
}

/// Spawns a thread-local task in the scope active on this thread, on the
/// current thread's active [`Executor`] or else on the thread's default one.
///
/// Can be called before or during [`block_on`].
pub fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
	let local = local();
	let join = local.spawn(local.current_scope(), future);
	JoinHandle { join }
}

pub(crate) fn submit(fd: Arc<dyn AsDescriptor>, op: Op) -> Slot {
	submitted(|| reactor::submit(fd, op))
}

pub(crate) fn submit_timer(deadline: Instant) -> Slot {
	submitted(|| reactor::submit_timer(deadline))
}

fn submitted(make: impl FnOnce() -> Slot) -> Slot {
	let Some(scope) = current_scope() else {
		return make();
	};
	if scope.is_cancelled() {
		return Slot::refused(Error::cancelled());
	}
	let slot = make();
	scope.track(slot.clone());
	slot
}

fn current_scope() -> Option<Scope> {
	LOCAL.with(|slot| slot.borrow().as_ref().map(|local| local.current_scope()))
}

pub(crate) fn enter_local(local: &Rc<Local>) -> EnteredLocal {
	EnteredLocal {
		previous: LOCAL.with(|slot| slot.replace(Some(Rc::clone(local)))),
	}
}

pub(crate) struct EnteredLocal {
	previous: Option<Rc<Local>>,
}

impl Drop for EnteredLocal {
	fn drop(&mut self) {
		let previous = self.previous.take();
		LOCAL.with(|slot| *slot.borrow_mut() = previous);
	}
}

pub(crate) fn local() -> Rc<Local> {
	if let Some(local) = LOCAL.with(|slot| slot.borrow().clone()) {
		return local;
	}
	default(|executor| Rc::clone(&executor.local))
}
