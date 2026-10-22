use std::future::Future;
use std::sync::Arc;

use completion_backend::{Op, OwnedDescriptor};

use crate::Result;
use crate::executor::{Handle, submit};

/// Handle for descriptors that signal readiness without transferring bytes
/// (such as `eventfd`, `pidfd`, or `signalfd`).
///
/// ```
/// # use completion_async::Readiness;
/// async fn wait(readiness: &Readiness) {
/// 	readiness.readable().await.unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct Readiness(Arc<Handle<OwnedDescriptor>>);

impl Readiness {
	/// Wraps `descriptor`.
	#[fastrace::trace]
	pub fn new(descriptor: impl Into<OwnedDescriptor>) -> Readiness {
		Readiness(Arc::new(Handle::new(descriptor.into())))
	}

	/// Unwraps the underlying [`OwnedDescriptor`] if no clones of this handle
	/// and no in-flight waits remain.
	pub fn try_take(self) -> Option<OwnedDescriptor> {
		Arc::into_inner(self.0).map(Handle::into_inner)
	}

	/// Waits once until the descriptor becomes readable.
	///
	/// This is a one-shot wait; call `readable` again to wait for subsequent
	/// readiness events.
	pub fn readable(&self) -> impl Future<Output = Result<()>> + use<> {
		let fd = Arc::clone(&self.0);
		async move { submit(fd, Op::Readable, ())?.await.0.map(|_response| ()) }
	}

	/// Waits once until the descriptor becomes writable.
	///
	/// This is a one-shot wait; call `writable` again to wait for subsequent
	/// readiness events.
	pub fn writable(&self) -> impl Future<Output = Result<()>> + use<> {
		let fd = Arc::clone(&self.0);
		async move { submit(fd, Op::Writable, ())?.await.0.map(|_response| ()) }
	}
}
