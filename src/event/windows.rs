//! A completion port: raised by posting to it, drained by its backend.

use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};

use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
use windows_sys::Win32::System::IO::{CreateIoCompletionPort, PostQueuedCompletionStatus};

use crate::desc::{AsDescriptor, BorrowedDescriptor, RawHandle};

pub(super) struct Inner {
	port: OwnedHandle,
}

pub(super) fn new() -> io::Result<Inner> {
	// SAFETY: plain Win32 call with valid arguments.
	let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
	if port.is_null() {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: the port handle is unowned; we own it from here on.
	Ok(Inner {
		port: unsafe { OwnedHandle::from_raw_handle(port) },
	})
}

impl Inner {
	pub(super) fn notify(&self) -> io::Result<()> {
		// SAFETY: the port is open for as long as this event lives.
		unsafe {
			PostQueuedCompletionStatus(self.port.as_raw_handle(), 0, 0, std::ptr::null_mut());
		}
		Ok(())
	}

	/// The port is the handle a backend drains; there is no descriptor.
	pub(super) fn handle(&self) -> RawHandle {
		RawHandle::from_raw_handle(self.port.as_raw_handle())
	}

	pub(super) fn descriptor(&self) -> BorrowedDescriptor<'_> {
		self.port.as_descriptor()
	}
}
