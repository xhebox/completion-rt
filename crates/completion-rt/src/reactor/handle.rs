//! Resources shared by the operations that name them.

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};

#[cfg(windows)]
use crate::desc::BorrowedDescriptor;
use crate::desc::{AsDescriptor, RawHandle};

/// An owned resource shared by operations; [`Handle::take`] waits for all other holders to release it.
pub struct Handle<S> {
	pub(super) value: Arc<S>,
	/// Drop after `value` so a woken taker sees the resource released.
	pub(super) release: Release,
}

impl<S> Handle<S> {
	pub fn new(value: S) -> Handle<S> {
		Self {
			value: Arc::new(value),
			release: Release::new(),
		}
	}

	/// Returns the resource after all other holders release it, or `None` if another take is already waiting.
	pub fn take(self) -> impl Future<Output = Option<S>> {
		let value = self.value;
		Take::new(value, self.release)
	}

	/// [`Handle::take`] without waiting: `Err(self)` while another clone still
	/// names the resource.
	pub fn try_take(self) -> Result<S, Handle<S>> {
		match Arc::try_unwrap(self.value) {
			Ok(value) => Ok(value),
			Err(value) => Err(Self {
				value,
				release: self.release,
			}),
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> From<S> for Handle<S> {
	fn from(value: S) -> Handle<S> {
		Handle::new(value)
	}
}

impl<S: AsDescriptor> Handle<S> {
	pub(super) fn raw(&self) -> RawHandle {
		RawHandle::from_descriptor(self.value.as_descriptor())
	}
}

impl<S> Clone for Handle<S> {
	fn clone(&self) -> Handle<S> {
		Self {
			value: Arc::clone(&self.value),
			release: self.release.clone(),
		}
	}
}

impl<S> Deref for Handle<S> {
	type Target = S;

	fn deref(&self) -> &S {
		&self.value
	}
}

#[cfg(unix)]
impl<S: AsFd> AsFd for Handle<S> {
	fn as_fd(&self) -> BorrowedFd<'_> {
		(**self).as_fd()
	}
}

#[cfg(windows)]
impl<S: AsDescriptor> AsDescriptor for Handle<S> {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		(**self).as_descriptor()
	}
}

pub(super) struct Release {
	waker: Arc<WakerSlot>,
}

impl Release {
	fn new() -> Release {
		Self {
			waker: Arc::new(WakerSlot::new()),
		}
	}

	fn wake(&self) {
		self.waker.wake();
	}
}

impl Clone for Release {
	fn clone(&self) -> Release {
		Self {
			waker: Arc::clone(&self.waker),
		}
	}
}

impl Drop for Release {
	fn drop(&mut self) {
		self.waker.wake();
	}
}

pub(super) struct WakerSlot {
	taken: AtomicBool,
	waker: Mutex<Option<Waker>>,
}

impl WakerSlot {
	fn new() -> WakerSlot {
		Self {
			taken: AtomicBool::new(false),
			waker: Mutex::new(None),
		}
	}

	fn register(&self, waker: &Waker) {
		*self.waker.lock().unwrap() = Some(waker.clone());
	}

	/// Wake outside the lock: the waker may register here again.
	fn wake(&self) {
		let waker = self.waker.lock().unwrap().take();
		if let Some(waker) = waker {
			waker.wake();
		}
	}
}

pub(super) struct Take<S> {
	value: Option<Arc<S>>,
	/// Drop after `value` so a woken taker sees the resource released.
	release: Release,
	claimed: bool,
}

impl<S> Take<S> {
	fn new(value: Arc<S>, release: Release) -> Take<S> {
		Self {
			value: Some(value),
			release,
			claimed: false,
		}
	}
}

impl<S> Future for Take<S> {
	type Output = Option<S>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S>> {
		let this = self.get_mut();
		let value = this
			.value
			.take()
			.expect("a take future is not polled after it resolved");
		if !this.claimed {
			if this
				.release
				.waker
				.taken
				.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
				.is_err()
			{
				drop(value);
				this.release.wake();
				return Poll::Ready(None);
			}
			this.claimed = true;
		}
		// Register before checking ownership to avoid losing a concurrent release.
		this.release.waker.register(cx.waker());
		match Arc::try_unwrap(value) {
			Ok(value) => Poll::Ready(Some(value)),
			Err(value) => {
				this.value = Some(value);
				Poll::Pending
			}
		}
	}
}

impl<S> Drop for Take<S> {
	fn drop(&mut self) {
		if self.claimed {
			self.release.waker.taken.store(false, Ordering::Release);
		}
	}
}
