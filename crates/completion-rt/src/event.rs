//! The reactor's own wake-up.

use std::io;
use std::sync::Arc;

use crate::desc::{BorrowedDescriptor, RawHandle};

use imp::Inner;

/// A cloneable, thread-safe wake-up handle for [`Reactor::poll`](crate::Reactor::poll) and [`Submitter::wait`](crate::Submitter::wait).
#[derive(Clone)]
pub struct Event(Arc<Inner>);

impl Event {
	pub(super) fn new() -> io::Result<Event> {
		Ok(Event(Arc::new(imp::new()?)))
	}

	/// Raises it: a blocked `poll` returns, and a waiting completion lands.
	pub fn notify(&self) -> io::Result<()> {
		self.0.notify()
	}

	pub(super) fn handle(&self) -> RawHandle {
		self.0.handle()
	}

	pub(super) fn descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.descriptor()
	}
}

#[cfg(unix)]
#[path = "event/fd.rs"]
mod imp;
#[cfg(windows)]
#[path = "event/windows.rs"]
mod imp;
