//! The completion future: a [`Completion`] that resolves when the reactor lands
//! its operation, or reports the cancel that beat it to the kernel.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use completion_backend::{AsDescriptor, Error, ErrorKind, Op, Response};

use crate::reactor::{Key, Reactor};

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
		let wakers = std::mem::take(
			&mut *self
				.wakers
				.lock()
				.expect("the cancel's wakers were poisoned"),
		);
		let children = std::mem::take(
			&mut *self
				.children
				.lock()
				.expect("the cancel's wakers were poisoned"),
		);
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
	#[fastrace::trace]
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
		let mut children = self
			.inner
			.children
			.lock()
			.expect("the cancel's children were poisoned");
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
		self.inner
			.children
			.lock()
			.expect("the cancel's children were poisoned")
			.len()
	}

	#[cfg(test)]
	fn waiting(&self) -> usize {
		self.inner
			.wakers
			.lock()
			.expect("the cancel's wakers were poisoned")
			.len()
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
		let mut wakers = this
			.inner
			.wakers
			.lock()
			.expect("the cancel's wakers were poisoned");
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
			self.inner
				.wakers
				.lock()
				.expect("the cancel's wakers were poisoned")
				.remove(&self.key);
		}
	}
}

/// Submits `op` on `fd`, racing it against `cancel` when one is given.
pub(crate) fn submit(
	reactor: &Reactor,
	fd: Arc<dyn AsDescriptor>,
	op: Op,
	cancel: Option<&Cancel>,
) -> Result<Completion, Error> {
	let key = reactor.submit(fd, op)?;
	Ok(Completion::new(reactor.clone(), key, cancel))
}

/// Arms a timer for `deadline`, racing it against `cancel` when one is given.
pub(crate) fn submit_timer(
	reactor: &Reactor,
	deadline: Instant,
	cancel: Option<&Cancel>,
) -> Result<Completion, Error> {
	let key = reactor.submit_timer(deadline)?;
	Ok(Completion::new(reactor.clone(), key, cancel))
}

/// Resolves to the response the reactor lands for one operation or timer, or to
/// `None` when a cancel it was given won.
///
/// Dropping an in-flight operation is predictable and does not block: the drop
/// cancels the operation and detaches it, and the reactor keeps its
/// [`Space`](completion_backend::Space) and descriptor alive until the kernel
/// finishes with it, though the span's bytes stay in flight until then. A
/// range that is to be read or reused right away is waited for by naming a
/// [`Cancel`]: the completion then resolves to `None` only once the kernel has
/// let go of the [`Span`](completion_backend::Span).
pub(crate) struct Completion {
	reactor: Reactor,
	key: Key,
	wait: Option<Wait>,
	requested: bool,
}

impl Completion {
	fn new(reactor: Reactor, key: Key, cancel: Option<&Cancel>) -> Completion {
		Completion {
			reactor,
			key,
			wait: cancel.map(|cancel| Wait::new(Arc::clone(&cancel.inner))),
			requested: false,
		}
	}

	/// Asks the reactor to give up on this completion's operation or timer.
	#[cfg(test)]
	pub(crate) fn cancel(&self) {
		self.reactor.cancel(self.key);
	}

	pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Response, Error>>> {
		loop {
			if let Some(response) = self.reactor.poll(self.key, cx.waker()) {
				if self.requested
					&& response
						.as_ref()
						.is_err_and(|error| error.kind() == ErrorKind::Cancelled)
				{
					return Poll::Ready(None);
				}
				return Poll::Ready(Some(response));
			}
			if !self.requested
				&& let Some(wait) = &mut self.wait
				&& Pin::new(wait).poll(cx).is_ready()
			{
				self.requested = true;
				self.reactor.cancel(self.key);
				continue;
			}
			return Poll::Pending;
		}
	}
}

impl Drop for Completion {
	fn drop(&mut self) {
		self.reactor.release(self.key);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::reactor::Reactor;
	use completion_backend::Config;
	use std::time::Duration;

	fn platform() -> Reactor {
		Reactor::new(Config::default()).unwrap()
	}

	fn race_to_completion(reactor: &Reactor, cancel: &Cancel) {
		// A pipe cannot be synced, so the operation lands on an error; that it
		// lands at all is all this helper needs.
		let (reader, _writer) = std::io::pipe().unwrap();
		let mut completion = submit(reactor, Arc::new(reader), Op::Fsync, Some(cancel)).unwrap();
		let mut cx = Context::from_waker(Waker::noop());
		for _ in 0..8 {
			if completion.poll(&mut cx).is_ready() {
				return;
			}
			reactor.drive(Some(Duration::from_secs(1))).unwrap();
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
