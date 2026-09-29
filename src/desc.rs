//! Descriptors: one name per family.

#[cfg(unix)]
mod imp {
	use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

	/// A descriptor as the backend names it: the value it keys an operation
	/// by, and the one a completion reports.
	#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
	pub struct RawHandle(RawFd);

	/// A descriptor as a plain value: the [`RawFd`] on this platform, handed
	/// out without giving up the descriptor it belongs to. See
	/// [`Submitter::in_flight`](crate::Submitter::in_flight).
	pub type RawDescriptor = RawFd;

	/// A descriptor this value owns and will close.
	pub type OwnedDescriptor = OwnedFd;
	/// A descriptor borrowed for a stated lifetime.
	///
	/// One type on this platform: this is `BorrowedFd`, so std's own
	/// [`BorrowedFd::try_clone_to_owned`] — handing back the [`OwnedFd`] behind
	/// [`OwnedDescriptor`] — is the whole of duplicating one, with no wrapper of
	/// our own to name.
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

		/// The number as the cross-platform [`RawDescriptor`], for
		/// [`Submitter::in_flight`](crate::Submitter::in_flight).
		pub const fn into_raw(self) -> RawDescriptor {
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

	/// A descriptor as a plain value: the handle or socket number on this
	/// platform, handed out without giving up the descriptor it belongs to.
	/// See [`Submitter::in_flight`](crate::Submitter::in_flight).
	pub type RawDescriptor = usize;

	/// A descriptor this value owns and will close: the owned counterpart of
	/// [`BorrowedDescriptor`], one arm per family.
	#[derive(Debug)]
	pub enum OwnedDescriptor {
		Handle(OwnedHandle),
		Socket(OwnedSocket),
	}
	/// A socket that owns its descriptor.
	pub type OwnedSocket = std::os::windows::io::OwnedSocket;

	impl BorrowedDescriptor<'_> {
		/// A second, independent descriptor on the same resource: it stays open
		/// on its own, and closing one descriptor does not close the other.
		/// std's method of the same name, per family.
		pub fn try_clone_to_owned(&self) -> io::Result<OwnedDescriptor> {
			match self {
				BorrowedDescriptor::Handle(handle) => {
					handle.try_clone_to_owned().map(OwnedDescriptor::Handle)
				}
				BorrowedDescriptor::Socket(socket) => {
					socket.try_clone_to_owned().map(OwnedDescriptor::Socket)
				}
			}
		}
	}

	impl From<OwnedHandle> for OwnedDescriptor {
		fn from(handle: OwnedHandle) -> Self {
			OwnedDescriptor::Handle(handle)
		}
	}

	impl From<OwnedSocket> for OwnedDescriptor {
		fn from(socket: OwnedSocket) -> Self {
			OwnedDescriptor::Socket(socket)
		}
	}

	impl RawHandle {
		pub fn as_platform(self) -> PlatformHandle {
			std::ptr::without_provenance_mut(self.0)
		}

		pub fn from_raw_handle(handle: PlatformHandle) -> Self {
			Self(handle as usize)
		}

		/// The number as the cross-platform [`RawDescriptor`], for
		/// [`Submitter::in_flight`](crate::Submitter::in_flight).
		pub const fn into_raw(self) -> RawDescriptor {
			self.0
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

	impl AsDescriptor for OwnedDescriptor {
		fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
			match self {
				OwnedDescriptor::Handle(handle) => BorrowedDescriptor::Handle(handle.as_handle()),
				OwnedDescriptor::Socket(socket) => BorrowedDescriptor::Socket(socket.as_socket()),
			}
		}
	}

	impl<S: AsDescriptor> AsDescriptor for crate::core::Handle<S> {
		fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
			(**self).as_descriptor()
		}
	}
}

#[cfg(windows)]
pub use imp::{
	AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket, RawDescriptor, RawHandle,
};
#[cfg(unix)]
pub use imp::{
	AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket, RawDescriptor, RawHandle,
	from_raw_socket,
};
