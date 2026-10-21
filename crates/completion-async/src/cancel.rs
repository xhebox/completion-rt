use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::reactor::Slot;
use crate::{JoinHandle, global};

const PRUNE_FLOOR: usize = 64;

/// A thread-safe, cloneable handle to a cancellation scope.
///
/// Cloning a `Scope` shares the same underlying scope. [`Scope::child`]
/// creates a derived scope that is cancelled when its parent is cancelled
/// without affecting the parent.
#[derive(Clone)]
pub struct Scope {
	inner: Arc<ScopeInner>,
}

struct ScopeInner {
	cancelled: AtomicBool,
	handles: Mutex<Tracked>,
	children: Mutex<Vec<Weak<ScopeInner>>>,
}

struct Tracked {
	handles: Vec<Slot>,
	prune_at: usize,
}

impl ScopeInner {
	fn new() -> ScopeInner {
		Self {
			cancelled: AtomicBool::new(false),
			handles: Mutex::new(Tracked {
				handles: Vec::new(),
				prune_at: PRUNE_FLOOR,
			}),
			children: Mutex::new(Vec::new()),
		}
	}

	fn raise(&self, out: &mut Vec<Slot>) {
		self.cancelled.store(true, Ordering::Release);
		let handles = self
			.handles
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.handles
			.clone();
		for handle in &handles {
			handle.cancel();
		}
		out.extend(handles);
		let children: Vec<Arc<ScopeInner>> = {
			let mut children = self
				.children
				.lock()
				.unwrap_or_else(|poisoned| poisoned.into_inner());
			children.retain(|child| child.strong_count() > 0);
			children.iter().filter_map(Weak::upgrade).collect()
		};
		for child in &children {
			child.raise(out);
		}
	}
}

impl Scope {
	pub(crate) fn new() -> Scope {
		Self {
			inner: Arc::new(ScopeInner::new()),
		}
	}

	/// Cancels this scope, its descendants, and all operations and timers
	/// submitted within them.
	///
	/// Cancellation takes effect immediately, before the returned future is
	/// polled. Awaiting the returned future waits until every cancelled
	/// operation and timer has resolved and the kernel has released its
	/// buffers; dropping it leaves the scope cancelled without waiting.
	pub fn cancel(&self) -> impl Future<Output = ()> + Send + 'static {
		let mut handles = Vec::new();
		self.inner.raise(&mut handles);
		async move {
			for handle in &handles {
				handle.wait().await;
			}
		}
	}

	pub(crate) fn fire(&self) {
		self.inner.raise(&mut Vec::new());
	}

	/// Returns `true` if [`Scope::cancel`] has been called on this scope or an
	/// ancestor.
	pub fn is_cancelled(&self) -> bool {
		self.inner.cancelled.load(Ordering::Acquire)
	}

	/// Creates a child scope: cancelling this scope cancels the child, while
	/// cancelling the child leaves this one unaffected.
	pub fn child(&self) -> Scope {
		let child = Scope::new();
		if self.is_cancelled() {
			child.fire();
			return child;
		}
		let mut children = self
			.inner
			.children
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		// Recheck under the lock in case `raise` ran before we locked `children`.
		if self.is_cancelled() {
			drop(children);
			child.fire();
			return child;
		}
		children.retain(|child| child.strong_count() > 0);
		children.push(Arc::downgrade(&child.inner));
		child
	}

	/// Spawns `future` as a task in this scope, on the executor running on this
	/// thread or else on the thread's default one.
	///
	/// Operations, timers, and tasks spawned via [`spawn`](crate::spawn) or
	/// [`Executor::spawn`](crate::Executor::spawn) while `future` is polled
	/// inherit this scope.
	pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
		JoinHandle {
			join: global::local().spawn(self.clone(), future),
		}
	}

	pub(crate) fn track(&self, handle: Slot) {
		let mut tracked = self
			.inner
			.handles
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		// `raise` stores `cancelled` before locking `handles`, so checking under
		// the lock guarantees `handle` is either seen by `raise` or cancelled here.
		if self.is_cancelled() {
			drop(tracked);
			handle.cancel();
			return;
		}
		tracked.handles.push(handle);
		if tracked.handles.len() >= tracked.prune_at {
			tracked.handles.retain(|handle| !handle.is_done());
			tracked.prune_at = (tracked.handles.len() * 2).max(PRUNE_FLOOR);
		}
	}
}
