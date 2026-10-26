use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

/// An owned file descriptor closed on drop.
pub type OwnedDescriptor = OwnedFd;
/// A borrowed file descriptor.
pub type BorrowedDescriptor<'a> = BorrowedFd<'a>;
/// An owned socket descriptor closed on drop.
pub type OwnedSocket = OwnedFd;

/// Borrows the underlying OS descriptor or handle.
pub trait AsDescriptor: Send + Sync {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_>;
}

// Listed rather than blanket over `AsFd`, so the backend takes only these
// types, as on Windows.
macro_rules! as_descriptor {
	($($fd:ty),* $(,)?) => {
		$(
			impl AsDescriptor for $fd {
				fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
					self.as_fd()
				}
			}
		)*
	};
}

as_descriptor!(
	OwnedFd,
	BorrowedFd<'_>,
	std::fs::File,
	io::Stdin,
	io::Stdout,
	io::Stderr,
	io::PipeReader,
	io::PipeWriter,
	std::net::TcpStream,
	std::net::TcpListener,
	std::net::UdpSocket,
	std::os::unix::net::UnixStream,
	std::os::unix::net::UnixListener,
	std::os::unix::net::UnixDatagram,
);
