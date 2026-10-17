//! Readiness polling for notification descriptors ([`Signal`]).

use completion_backend::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor};

use crate::op::{Action, Facade, Op};

/// Wrapper for descriptors that only signal readiness without transferring bytes
/// through the reactor (such as `eventfd`, `pidfd`, `signalfd`, or custom device fds).
///
/// For files, pipes, and sockets, use [`Facade`] directly—their read/write/send/recv
/// operations handle readiness internally.
///
/// ```no_run
/// # use completion_async::{Executor, Facade, OwnedDescriptor, Signal};
/// # fn example(descriptor: OwnedDescriptor, executor: &Executor) {
/// let facade: Facade<Signal> = Facade::with_executor(Signal::new(descriptor), executor);
/// # }
/// ```
pub struct Signal(OwnedDescriptor);

impl Signal {
	/// Wraps an owned descriptor in a [`Signal`].
	#[fastrace::trace]
	pub fn new(descriptor: impl Into<OwnedDescriptor>) -> Signal {
		Signal(descriptor.into())
	}

	/// Unwraps and returns the underlying [`OwnedDescriptor`].
	pub fn into_descriptor(self) -> OwnedDescriptor {
		self.0
	}
}

impl AsDescriptor for Signal {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.as_descriptor()
	}
}

impl Facade<Signal> {
	/// Waits once until the descriptor becomes readable.
	///
	/// This is a one-shot wait; call `wait_readable` again to wait for subsequent
	/// readiness events.
	pub fn wait_readable(&self) -> Op {
		Op::new(self, Action::Readable)
	}

	/// Waits once until the descriptor becomes writable.
	///
	/// This is a one-shot wait; call `wait_writable` again to wait for subsequent
	/// readiness events.
	pub fn wait_writable(&self) -> Op {
		Op::new(self, Action::Writable)
	}
}
