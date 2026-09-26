//! Descriptors: one name per family.

#[cfg(unix)]
mod imp {
	use std::io;
	use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

	/// A descriptor as the backend names it: the value it keys an operation
	/// by, and the one a completion reports.
	#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
	pub struct RawHandle(RawFd);

	/// A descriptor this value owns and will close.
	pub type OwnedDescriptor = OwnedFd;
	/// A descriptor borrowed for a stated lifetime.
	pub type BorrowedDescriptor<'a> = BorrowedFd<'a>;
	/// A socket that owns its descriptor.
	pub type OwnedSocket = OwnedFd;

	impl RawHandle {
		/// The descriptor a completion reports for one it created: the
		/// kernel hands over a number, and the caller owns it from there.
		pub const fn from_raw(value: usize) -> Self {
			Self(value as RawFd)
		}

		/// The number behind a borrow, for the backend.
		pub fn from_descriptor(descriptor: BorrowedDescriptor<'_>) -> Self {
			Self(descriptor.as_raw_fd())
		}

		pub const fn as_raw_fd(self) -> RawFd {
			self.0
		}
	}

	/// Naming the descriptor a value holds, without giving up ownership of it.
	///
	/// A borrow, not a number: a number a caller holds is not a reason to
	/// believe the descriptor behind it is open.
	pub trait AsDescriptor {
		fn as_descriptor(&self) -> BorrowedDescriptor<'_>;
	}

	impl<T: AsFd> AsDescriptor for T {
		fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
			self.as_fd()
		}
	}

	impl<S: AsFd> AsFd for crate::core::Handle<S> {
		fn as_fd(&self) -> BorrowedFd<'_> {
			(**self).as_fd()
		}
	}

	/// Taking the socket the kernel accepted: the descriptor an accept reports
	/// is the caller's from there.
	///
	/// # Safety
	///
	/// `raw` must be a live socket nobody else owns.
	pub unsafe fn from_raw_socket(raw: RawHandle) -> OwnedSocket {
		unsafe { OwnedSocket::from_raw_fd(raw.as_raw_fd()) }
	}

	/// A second, independent descriptor on the same resource: it stays open on
	/// its own, and closing one descriptor does not close the other.
	pub fn dup(handle: BorrowedDescriptor<'_>) -> io::Result<OwnedSocket> {
		handle.try_clone_to_owned()
	}
}

#[cfg(windows)]
mod imp {
	use std::io;
	use std::os::windows::io::{
		AsHandle, AsSocket, BorrowedHandle, BorrowedSocket, OwnedHandle,
		RawHandle as PlatformHandle,
	};

	/// A borrowed descriptor: a plain descriptor, or a socket, which Windows keeps
	/// in a family of its own.
	#[derive(Clone, Copy, Debug)]
	pub enum BorrowedDescriptor<'a> {
		Handle(BorrowedHandle<'a>),
		Socket(BorrowedSocket<'a>),
	}

	/// A descriptor as the backend names it: the value it keys an operation
	/// by, and the one a completion reports.
	#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
	pub struct RawHandle(usize);

	/// A descriptor this value owns and will close.
	pub type OwnedDescriptor = OwnedHandle;
	/// A socket that owns its descriptor.
	pub type OwnedSocket = std::os::windows::io::OwnedSocket;

	impl RawHandle {
		pub fn as_platform(self) -> PlatformHandle {
			std::ptr::without_provenance_mut(self.0)
		}

		pub fn from_raw_handle(handle: PlatformHandle) -> Self {
			Self(handle as usize)
		}

		/// The number behind a borrow, for the backend.
		pub fn from_descriptor(descriptor: BorrowedDescriptor<'_>) -> Self {
			match descriptor {
				BorrowedDescriptor::Handle(handle) => {
					Self(std::os::windows::io::AsRawHandle::as_raw_handle(&handle) as usize)
				}
				BorrowedDescriptor::Socket(socket) => {
					Self(std::os::windows::io::AsRawSocket::as_raw_socket(&socket) as usize)
				}
			}
		}
	}

	/// Naming the descriptor a value holds, without giving up ownership of it.
	/// See the unix side.
	pub trait AsDescriptor {
		fn as_descriptor(&self) -> BorrowedDescriptor<'_>;
	}

	// Sockets are their own family here, so a blanket over one of the two
	// cannot cover both; the handle types are listed.
	macro_rules! as_descriptor {
		($($handle:ty),* $(,)?) => {
			$(
				impl AsDescriptor for $handle {
					fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
						BorrowedDescriptor::Handle(self.as_handle())
					}
				}
			)*
		};
	}

	as_descriptor!(
		OwnedHandle,
		BorrowedHandle<'_>,
		std::fs::File,
		std::io::Stdin,
		std::io::Stdout,
		std::io::Stderr,
		std::io::PipeReader,
		std::io::PipeWriter,
	);

	macro_rules! as_descriptor_socket {
		($($socket:ty),* $(,)?) => {
			$(
				impl AsDescriptor for $socket {
					fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
						BorrowedDescriptor::Socket(self.as_socket())
					}
				}
			)*
		};
	}

	as_descriptor_socket!(
		std::net::TcpStream,
		std::net::TcpListener,
		std::net::UdpSocket,
		std::os::windows::io::OwnedSocket,
		std::os::windows::io::BorrowedSocket<'_>,
	);

	impl<S: AsDescriptor> AsDescriptor for crate::core::Handle<S> {
		fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
			(**self).as_descriptor()
		}
	}

	/// A second descriptor on the same socket. Nothing here needs one: the VMM's
	/// devices only run on unix.
	pub fn dup(_handle: BorrowedDescriptor<'_>) -> std::io::Result<OwnedSocket> {
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"duplicating a socket is not implemented here",
		))
	}
}

#[cfg(windows)]
pub use imp::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket, RawHandle, dup};
#[cfg(unix)]
pub use imp::{
	AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket, RawHandle, dup, from_raw_socket,
};
