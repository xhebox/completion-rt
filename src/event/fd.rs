//! A descriptor pair: the end a backend waits on, and the end `notify` writes.

use std::io;
use std::os::fd::OwnedFd;

use rustix::io::Errno;

use crate::desc::{AsDescriptor, BorrowedDescriptor, RawHandle};

pub(super) struct Inner {
	/// What a backend blocks on, and drains when it wakes.
	read: OwnedFd,
	/// What `notify` writes. An eventfd is one counter for both.
	write: OwnedFd,
}

impl Inner {
	pub(super) fn notify(&self) -> io::Result<()> {
		// The counter is what a reader drains; a full pipe already means
		// "something happened", so a refused write is not an error.
		match rustix::io::write(&self.write, &1u64.to_ne_bytes()) {
			Ok(_) | Err(Errno::AGAIN) => Ok(()),
			Err(error) => Err(error.into()),
		}
	}

	pub(super) fn handle(&self) -> RawHandle {
		RawHandle::from_descriptor(self.descriptor())
	}

	pub(super) fn descriptor(&self) -> BorrowedDescriptor<'_> {
		self.read.as_descriptor()
	}
}

/// A fresh pair, unraised.
pub(super) fn new() -> io::Result<Inner> {
	platform::new()
}

#[cfg(target_os = "linux")]
#[path = "fd/linux.rs"]
mod platform;
#[cfg(not(target_os = "linux"))]
#[path = "fd/pipe.rs"]
mod platform;
