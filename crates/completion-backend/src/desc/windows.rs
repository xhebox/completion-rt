use std::io;
use std::os::windows::io::{AsHandle, AsSocket, BorrowedHandle, BorrowedSocket, OwnedHandle};

/// A borrowed Windows handle or socket.
#[derive(Clone, Copy, Debug)]
pub enum BorrowedDescriptor<'a> {
	Handle(BorrowedHandle<'a>),
	Socket(BorrowedSocket<'a>),
}

/// An owned Windows handle or socket closed on drop.
#[derive(Debug)]
pub enum OwnedDescriptor {
	Handle(OwnedHandle),
	Socket(OwnedSocket),
}
/// An owned Windows socket closed on drop.
pub type OwnedSocket = std::os::windows::io::OwnedSocket;

impl BorrowedDescriptor<'_> {
	/// Duplicates the descriptor into a new [`OwnedDescriptor`].
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

macro_rules! from_handle {
	($($handle:ty),* $(,)?) => {
		$(
			impl From<$handle> for OwnedDescriptor {
				fn from(handle: $handle) -> Self {
					OwnedDescriptor::Handle(OwnedHandle::from(handle))
				}
			}
		)*
	};
}

from_handle!(std::fs::File, std::io::PipeReader, std::io::PipeWriter);

macro_rules! from_socket {
	($($socket:ty),* $(,)?) => {
		$(
			impl From<$socket> for OwnedDescriptor {
				fn from(socket: $socket) -> Self {
					OwnedDescriptor::Socket(OwnedSocket::from(socket))
				}
			}
		)*
	};
}

from_socket!(
	std::net::TcpStream,
	std::net::TcpListener,
	std::net::UdpSocket,
);

/// Borrows the underlying OS descriptor or handle.
pub trait AsDescriptor: Send + Sync {
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
