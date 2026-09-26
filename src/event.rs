//! The reactor's own wake-up.

use std::io;
use std::sync::Arc;

use crate::desc::{BorrowedDescriptor, RawHandle};

use imp::Inner;

/// A handle on a reactor's wake-up. Raising it makes a blocked
/// [`Reactor::poll`](crate::Reactor::poll) return, whether or not anything
/// completed — what a waiter was after is the completion side's business.
///
/// Cheap to clone, and safe to hold from any thread: it is how a producer
/// tells a reactor that something changed. Waiting for one is
/// [`Submitter::wait`](crate::Submitter::wait).
#[derive(Clone)]
pub struct Event(Arc<Inner>);

impl Event {
	/// A fresh wake-up, unraised.
	///
	/// Reached through [`Reactor::event`](crate::Reactor::event); one made on
	/// its own has nothing waiting on it.
	pub(super) fn new() -> io::Result<Event> {
		Ok(Event(Arc::new(imp::new()?)))
	}

	/// Raises it: a blocked `poll` returns, and a waiting completion lands.
	pub fn notify(&self) -> io::Result<()> {
		self.0.notify()
	}

	/// What the backend waits on or drains: a descriptor where the platform
	/// has one, the completion port where it does not.
	pub(super) fn handle(&self) -> RawHandle {
		self.0.handle()
	}

	/// The same wake-up as a borrow, for [`Reactor::poll_fd`](crate::Reactor::poll_fd).
	pub(super) fn descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.descriptor()
	}

	/// Whether `other` is the same wake-up, not a clone of the same kind: two
	/// handles on one reactor share their `Inner`, two reactors never do.
	pub(super) fn same(&self, other: &Event) -> bool {
		Arc::ptr_eq(&self.0, &other.0)
	}
}

#[cfg(unix)]
#[path = "event/fd.rs"]
mod imp;
#[cfg(windows)]
#[path = "event/windows.rs"]
mod imp;
