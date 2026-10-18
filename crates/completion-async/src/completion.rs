//! Cancellation tokens ([`Cancel`]) and reactor completion futures.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use completion_backend::{AsDescriptor, ErrorKind, Op, Response, Result};
use slotmap::{DefaultKey, SlotMap};

use crate::reactor::{Key, Reactor};

/// A thread-safe, cloneable cancellation token.
///
/// Cloning a `Cancel` shares the same underlying state. Use [`Cancel::child`] to
/// create a derived token that is cancelled when its parent is cancelled without
/// affecting the parent.
#[derive(Clone)]
pub struct Cancel {
	inner: Arc<CancelInner>,
}

struct CancelInner {
	cancelled: AtomicBool,
	wakers: Mutex<SlotMap<DefaultKey, Waker>>,
	children: Mutex<Vec<Weak<CancelInner>>>,
}

impl CancelInner {
	fn new() -> CancelInner {
		Self {
			cancelled: AtomicBool::new(false),
			wakers: Mutex::new(SlotMap::new()),
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
	/// Creates a new, uncancelled token.
	#[fastrace::trace]
	pub fn new() -> Cancel {
		Self {
			inner: Arc::new(CancelInner::new()),
		}
	}

	/// Triggers cancellation and wakes all waiting operations and child tokens. Idempotent.
	pub fn cancel(&self) {
		self.inner.fire();
	}

	/// Returns `true` if [`Cancel::cancel`] has been called on this token or an ancestor.
	pub fn is_cancelled(&self) -> bool {
		self.inner.cancelled.load(Ordering::Acquire)
	}

	/// Creates a child token that is cancelled when `self` is cancelled, while
	/// cancelling the child leaves `self` unaffected. If `self` is already
	/// cancelled, the returned child starts cancelled.
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

	/// Waits until [`Cancel::cancel`] is called, or resolves immediately if
	/// already cancelled.
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
	/// The slot the waker is filed in, taken on the first poll: building a
	/// `Wait` touches no lock at all.
	key: Option<DefaultKey>,
}

impl Wait {
	fn new(inner: Arc<CancelInner>) -> Wait {
		Wait { inner, key: None }
	}
}

impl Future for Wait {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		let this = self.get_mut();
		if this.inner.cancelled.load(Ordering::Acquire) {
			return Poll::Ready(());
		}
		let mut wakers = this
			.inner
			.wakers
			.lock()
			.expect("the cancel's wakers were poisoned");
		// Rechecked under the lock: `fire` takes the map, so a waiter that
		// missed it there has to see the cancellation here instead.
		if this.inner.cancelled.load(Ordering::Acquire) {
			if let Some(key) = this.key.take() {
				wakers.remove(key);
			}
			return Poll::Ready(());
		}
		if let Some(key) = this.key
			&& let Some(existing) = wakers.get_mut(key)
		{
			// A poll with the same waker does not even clone it.
			if !existing.will_wake(cx.waker()) {
				*existing = cx.waker().clone();
			}
			return Poll::Pending;
		}
		// No slot yet, or none any more because `fire` took the map: file the
		// waker afresh.
		this.key = Some(wakers.insert(cx.waker().clone()));
		Poll::Pending
	}
}

impl Drop for Wait {
	fn drop(&mut self) {
		if let Some(key) = self.key.take() {
			self.inner
				.wakers
				.lock()
				.expect("the cancel's wakers were poisoned")
				.remove(key);
		}
	}
}

/// Submits `op` on `fd` with optional cancellation.
pub(crate) fn submit(
	reactor: &Reactor,
	fd: Arc<dyn AsDescriptor>,
	op: Op,
	cancel: Option<&Cancel>,
) -> Result<Completion> {
	let key = reactor.submit(fd, op)?;
	Ok(Completion::new(reactor.clone(), key, cancel))
}

/// Submits a timer for `deadline` with optional cancellation.
pub(crate) fn submit_timer(
	reactor: &Reactor,
	deadline: Instant,
	cancel: Option<&Cancel>,
) -> Result<Completion> {
	let key = reactor.submit_timer(deadline)?;
	Ok(Completion::new(reactor.clone(), key, cancel))
}

/// Tracks a single submitted reactor operation or timer.
///
/// Dropping a `Completion` detaches and cancels the operation without blocking
/// while keeping the [`Buffer`](completion_backend::Buffer) and descriptor alive
/// until the kernel finishes. When paired with a [`Cancel`] token, `poll` waits
/// for the kernel completion and returns `None` once the buffer is safe to reuse.
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

	pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Response>>> {
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
