//! Waiting on a descriptor for readiness, and nothing else.

use std::panic::Location;

use completion_rt::{AsDescriptor, BorrowedDescriptor, Op as LowOp, OwnedDescriptor};

use crate::Facade;
use crate::completion::submit;
use crate::kind::{self, Resource};
use crate::net::Op;

/// A descriptor whose only operation is the wait for readiness: an eventfd, a
/// pidfd, a vhost-user endpoint.
///
/// It is a kind of its own because nothing moves through one. A resource with a
/// transfer is not this: the backend arms the readiness itself when a call
/// would block, and the completion reports how much moved — so a caller who
/// reads, finds nothing, and waits for readability has written a loop the
/// engine already runs.
///
/// ```no_run
/// # use completion_async::{Executor, Facade, OwnedDescriptor, Signal};
/// # fn example(descriptor: OwnedDescriptor, executor: &Executor) {
/// let facade: Facade<Signal> = Facade::with_executor(Signal::new(descriptor), executor);
/// # }
/// ```
pub struct Signal(OwnedDescriptor);

impl Signal {
	/// Waits on `descriptor`, taking it over.
	pub fn new(descriptor: impl Into<OwnedDescriptor>) -> Signal {
		Signal(descriptor.into())
	}

	/// Gives the descriptor back.
	pub fn into_descriptor(self) -> OwnedDescriptor {
		self.0
	}
}

impl AsDescriptor for Signal {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.as_descriptor()
	}
}

impl Resource for Signal {
	type Kind = kind::Signal;
}

impl<S: Resource<Kind = kind::Signal>> Facade<S> {
	/// Resolves once the descriptor has something to read.
	///
	/// A completion resolves once, so a caller that wants readiness again
	/// submits another one — this is not a standing registration.
	#[track_caller]
	pub fn wait_readable(&self) -> Op<S> {
		Op::new(self, Location::caller(), |reactor, origin, handle| {
			submit(reactor, handle, LowOp::Readable, origin, None)
		})
	}

	/// Resolves once the descriptor can take a write.
	#[track_caller]
	pub fn wait_writable(&self) -> Op<S> {
		Op::new(self, Location::caller(), |reactor, origin, handle| {
			submit(reactor, handle, LowOp::Writable, origin, None)
		})
	}
}
