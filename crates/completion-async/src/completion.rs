//! The completion futures: a [`Completion`] that resolves when the reactor
//! lands its operation, the [`Until`] that races it against a [`Cancel`], and
//! the task-cancel flag that tells a dropped completion it was given up on
//! purpose.

use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::panic::Location;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use completion_rt::{AsDescriptor, Handle, Lease, Op, Reactor, Response, Token};

/// A clonable cancellation signal. Clones share the signal; [`Cancel::child`] creates a signal also cancelled by its parent.
#[derive(Clone)]
pub struct Cancel {
	inner: Arc<CancelInner>,
}

struct CancelInner {
	cancelled: AtomicBool,
	wakers: Mutex<HashMap<u64, Waker>>,
	next: AtomicU64,
	children: Mutex<Vec<Weak<CancelInner>>>,
}

impl CancelInner {
	fn new() -> CancelInner {
		Self {
			cancelled: AtomicBool::new(false),
			wakers: Mutex::new(HashMap::new()),
			next: AtomicU64::new(0),
			children: Mutex::new(Vec::new()),
		}
	}

	fn fire(&self) {
		if self.cancelled.swap(true, Ordering::AcqRel) {
			return;
		}
		let wakers = std::mem::take(&mut *self.wakers.lock().unwrap());
		let children = std::mem::take(&mut *self.children.lock().unwrap());
		for (_, waker) in wakers {
			waker.wake();
		}
		for child in &children {
			if let Some(child) = child.upgrade() {
				child.fire();
			}
		}
	}
}

impl Cancel {
	/// A fresh cancel, unraised.
	pub fn new() -> Cancel {
		Self {
			inner: Arc::new(CancelInner::new()),
		}
	}

	/// Raises the cancel. Idempotent.
	pub fn cancel(&self) {
		self.inner.fire();
	}

	/// Whether the cancel has fired.
	pub fn is_cancelled(&self) -> bool {
		self.inner.cancelled.load(Ordering::Acquire)
	}

	/// A cancel that fires when this one does; cancelling the child leaves the
	/// parent alone. A child of an already-raised `Cancel` starts out raised.
	pub fn child(&self) -> Cancel {
		let child = Cancel::new();
		if self.is_cancelled() {
			child.cancel();
			return child;
		}
		let mut children = self.inner.children.lock().unwrap();
		// Recheck under the lock to cover cancellation racing child registration.
		if self.is_cancelled() {
			drop(children);
			child.cancel();
			return child;
		}
		children.retain(|child| child.strong_count() > 0);
		children.push(Arc::downgrade(&child.inner));
		child
	}

	/// Resolves once the cancel fires, at once if it already has. Dropping the
	/// future before then takes its waker out of the cancel.
	pub fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
		Wait::new(Arc::clone(&self.inner))
	}

	#[cfg(test)]
	fn children(&self) -> usize {
		self.inner.children.lock().unwrap().len()
	}

	#[cfg(test)]
	fn waiting(&self) -> usize {
		self.inner.wakers.lock().unwrap().len()
	}
}

impl Default for Cancel {
	fn default() -> Cancel {
		Cancel::new()
	}
}

struct Wait {
	inner: Arc<CancelInner>,
	key: u64,
	registered: bool,
}

impl Wait {
	fn new(inner: Arc<CancelInner>) -> Wait {
		let key = inner.next.fetch_add(1, Ordering::Relaxed);
		Wait {
			inner,
			key,
			registered: false,
		}
	}
}

impl Future for Wait {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		let this = self.get_mut();
		let mut wakers = this.inner.wakers.lock().unwrap();
		if this.inner.cancelled.load(Ordering::Acquire) {
			this.registered = false;
			return Poll::Ready(());
		}
		wakers.insert(this.key, cx.waker().clone());
		this.registered = true;
		Poll::Pending
	}
}

impl Drop for Wait {
	fn drop(&mut self) {
		if self.registered {
			self.inner.wakers.lock().unwrap().remove(&self.key);
		}
	}
}

thread_local! {
	static TASK_CANCEL: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn cancel_task_future<F>(future: F) {
	struct Restore(bool);

	impl Drop for Restore {
		fn drop(&mut self) {
			TASK_CANCEL.set(self.0);
		}
	}

	// A cancelled task is an explicit give-up, even when its opaque future hides a Completion.
	let _restore = Restore(TASK_CANCEL.replace(true));
	drop(future);
}

/// Submits `op` on `fd` and wraps its token in a [`Completion`].
pub(crate) fn submit<S: AsDescriptor + Send + Sync + 'static>(
	reactor: &Reactor,
	fd: &Handle<S>,
	op: Op,
	origin: &'static Location<'static>,
	lease: Option<&Lease>,
) -> io::Result<Completion> {
	let token = reactor.submit(fd, op, origin, lease)?;
	Ok(Completion {
		reactor: reactor.clone(),
		token,
	})
}

/// Resolves to the response the reactor lands for one operation.
///
/// Dropping an in-flight operation that accesses caller memory waits for the kernel to release it. An unsignalled drop logs an error and panics in debug builds unless already unwinding; a requested cancellation does not panic.
pub(crate) struct Completion {
	reactor: Reactor,
	token: Token,
}

impl Completion {
	/// Races the operation against `cancel`, giving the [`Until`] future.
	pub(crate) fn until(self, cancel: &Cancel) -> Until {
		let wait = Wait::new(Arc::clone(&cancel.inner));
		Until {
			completion: self,
			wait,
			requested: false,
		}
	}

	fn request_cancel(&self) {
		self.token.cancel();
	}

	fn was_cancelled(&self, response: &io::Result<Response>) -> bool {
		match response {
			Ok(_) => false,
			Err(error) => self.reactor.is_cancelled(error),
		}
	}
}

/// Returned by [`Until`] when the cancel wins.
pub(crate) struct Cancelled;

/// Waits for completion or cancellation, including release of kernel access to memory.
///
/// Returns `Err(Cancelled)` only if the operation was actually cancelled; an operation that finishes despite cancellation retains its result.
pub(crate) struct Until {
	completion: Completion,
	wait: Wait,
	requested: bool,
}

impl Future for Until {
	type Output = Result<io::Result<Response>, Cancelled>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		match Pin::new(&mut this.completion).poll(cx) {
			Poll::Ready(response) => {
				if this.requested && this.completion.was_cancelled(&response) {
					Poll::Ready(Err(Cancelled))
				} else {
					Poll::Ready(Ok(response))
				}
			}
			Poll::Pending => {
				if !this.requested && Pin::new(&mut this.wait).poll(cx).is_ready() {
					this.requested = true;
					this.completion.request_cancel();
				}
				Poll::Pending
			}
		}
	}
}

impl Future for Completion {
	type Output = io::Result<Response>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		self.get_mut().token.poll(cx.waker())
	}
}

impl Drop for Completion {
	fn drop(&mut self) {
		if TASK_CANCEL.get() {
			self.token.cancel();
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use completion_rt::{Config, Reactor};
	use std::time::Duration;

	fn platform() -> Reactor {
		Reactor::new(Config::default()).unwrap()
	}

	fn race_to_completion(reactor: &Reactor, cancel: &Cancel) {
		// A pipe cannot be synced, so the operation lands on an error; that it
		// lands at all is all this helper needs.
		let (reader, _writer) = std::io::pipe().unwrap();
		let completion = submit(
			reactor,
			&Handle::new(reader),
			Op::Fsync,
			Location::caller(),
			None,
		)
		.unwrap();
		let mut until = std::pin::pin!(completion.until(cancel));
		let mut cx = Context::from_waker(Waker::noop());
		for _ in 0..8 {
			if until.as_mut().poll(&mut cx).is_ready() {
				return;
			}
			reactor.poll(Some(Duration::from_secs(1))).unwrap();
		}
		panic!("the operation did not complete");
	}

	/// A long-lived cancel must not retain waiters after their operations finish.
	#[test]
	fn a_cancel_keeps_no_waiter_that_finished() {
		let reactor = platform();
		let cancel = Cancel::new();
		for _ in 0..64 {
			race_to_completion(&reactor, &cancel);
		}
		assert_eq!(cancel.waiting(), 0, "a finished waiter was left behind");
	}

	/// A long-lived parent must not retain dropped children.
	#[test]
	fn a_cancel_keeps_no_child_that_was_dropped() {
		let cancel = Cancel::new();
		for _ in 0..64 {
			drop(cancel.child());
		}
		assert!(
			cancel.children() <= 1,
			"the children outlived their holders: {}",
			cancel.children()
		);
	}
}
