//! Socket operations: the receive and the send.

use std::io;
use std::sync::Arc;

use crate::flow::{Accept, Kind, Receive, Transfer, whole};
use crate::io::Facade;
use crate::{AsDescriptor, Extent, Extents, Memory, OwnedDescriptor, OwnedSocket, Submitter};

/// A resource a socket facade names: a socket, not a file.
///
/// Receiving and sending are only meaningful on a socket, which is what this
/// marks. It is not sealed: a type of the caller's own is a socket once the
/// caller implements this and [`AsDescriptor`] for it, and that type owns the
/// socket — an operation holds the handle, not the number, so an impl over a
/// bare descriptor would leave it open to be closed and handed out again
/// underneath the operation. `fs::File`, which is also a resource, is
/// deliberately not one.
///
/// A socket that is a byte stream is also a [`StreamHandle`]; a datagram socket
/// is this alone.
pub trait SocketHandle: AsDescriptor + Send + Sync + 'static {}

/// A socket pair that carries a byte stream, with the loops a stream needs and
/// a datagram has no use for.
///
/// A stream keeps no messages: a receive may take half of what a send made, so
/// the loops over the calls are what fill a memory and empty one. A datagram
/// socket takes a message per call and sends one — there is nothing to
/// continue, so none of `recv_exact`, `send_all` and their like is on it.
pub trait StreamHandle: SocketHandle {}

/// A socket a connection can be taken out of.
///
/// The stream the accept reports is named by [`Stream`](Self::Stream): the
/// listener says what its connections are, and the caller gets a typed facade
/// for them. Not a [`SocketHandle`]: a listener is only listened on, and
/// receiving from it is a mistake the type should not allow.
pub trait ListenerHandle: AsDescriptor + Send + Sync + 'static {
	type Stream: StreamHandle + From<OwnedSocket>;
}

impl<S: SocketHandle> Facade<S> {
	/// Receives into the whole of `mem`, in one call: a short count is what
	/// arrived, not an error. [`recv_exact`](Facade::recv_exact) loops.
	pub fn recv(&self, mem: Arc<dyn Memory>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::Recv, mem, extents)
	}

	/// Receives into the named ranges of `mem`, in one call.
	pub fn recv_vectored(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
	) -> Transfer<S> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::Recv, mem, extents)
	}

	/// Receives into the whole of `mem`, taking the descriptors the control
	/// messages carry with the bytes.
	pub fn recv_with_fds(&self, mem: Arc<dyn Memory>) -> Receive<S> {
		let extents = whole(&*mem);
		Receive::new(self, Kind::Recv, mem, extents)
	}

	/// Sends the whole of `mem`, in one call: a short count is what the call
	/// took, not an error. [`send_all`](Facade::send_all) loops.
	pub fn send(&self, mem: Arc<dyn Memory>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::Send, mem, extents)
	}

	/// Sends the named ranges of `mem`, in one call.
	pub fn send_vectored(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
	) -> Transfer<S> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::Send, mem, extents)
	}

	/// Sends the whole of `mem`, carrying `fds` with the bytes.
	///
	/// One call carries the descriptors; see
	/// [`send_all_with_fds`](Facade::send_all_with_fds) for a send that loops.
	pub fn send_with_fds(&self, mem: Arc<dyn Memory>, fds: Vec<OwnedDescriptor>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::with_fds(self, Kind::Send, mem, extents, fds)
	}

	/// The error the socket has pending, taking it away: reading `SO_ERROR`
	/// clears it, so a second call reports nothing — what
	/// `TcpStream::take_error` does, over a descriptor this crate only borrows.
	///
	/// An error the kernel reports asynchronously — a refused datagram, a reset
	/// the next call would otherwise surface — is what this reads; a healthy
	/// socket has none.
	pub fn take_error(&self) -> io::Result<Option<io::Error>> {
		imp::take_error((**self.handle()).as_descriptor())
	}
}

impl<S: StreamHandle> Facade<S> {
	/// Receives into the whole of `mem`, over as many calls as it takes to fill
	/// it.
	///
	/// [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) when the peer closes
	/// first.
	pub fn recv_exact(&self, mem: Arc<dyn Memory>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::RecvAll, mem, extents)
	}

	/// Receives into the named ranges of `mem`, over as many calls as it takes
	/// to fill them.
	pub fn recv_exact_vectored(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
	) -> Transfer<S> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::RecvAll, mem, extents)
	}

	/// [`recv_with_fds`](Facade::recv_with_fds) over as many calls as it takes
	/// to fill `mem`, gathering the descriptors every call brings.
	pub fn recv_exact_with_fds(&self, mem: Arc<dyn Memory>) -> Receive<S> {
		let extents = whole(&*mem);
		Receive::new(self, Kind::RecvAll, mem, extents)
	}

	/// Sends the whole of `mem`, over as many calls as it takes.
	///
	/// [`WriteZero`](std::io::ErrorKind::WriteZero) when a call takes nothing.
	pub fn send_all(&self, mem: Arc<dyn Memory>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::SendAll, mem, extents)
	}

	/// Sends the named ranges of `mem`, over as many calls as it takes.
	pub fn send_all_vectored(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
	) -> Transfer<S> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::SendAll, mem, extents)
	}

	/// Sends the whole of `mem` over as many calls as it takes, carrying `fds`
	/// with the first call's bytes: descriptors travel with bytes, so the one
	/// call the kernel took them in is the one they are announced in.
	pub fn send_all_with_fds(
		&self,
		mem: Arc<dyn Memory>,
		fds: Vec<OwnedDescriptor>,
	) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::with_fds(self, Kind::SendAll, mem, extents, fds)
	}

	/// Shuts the stream down, one direction or both. `Write` is the half-close:
	/// the peer reads end of file from there, and the receive side stays open,
	/// so what the peer sends afterwards is still received. `Both` closes the
	/// two directions.
	///
	/// It is not how an operation in flight is given up — that is a
	/// [`Cancel`](crate::Cancel) on the future's [`until`](Transfer::until). A
	/// shutdown is a plain call on the descriptor: it tells the kernel, and what
	/// an operation already with the kernel then does is the kernel's to report.
	/// A receive in flight when `Read` or `Both` is asked for ends with that
	/// report — zero bytes on Linux; the portable backend's poller reports the
	/// socket ready and the call reads the same zero. A send in flight when
	/// `Write` or `Both` is asked for fails with the error a closed send side
	/// gives — [`BrokenPipe`](io::ErrorKind::BrokenPipe) and the like.
	pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
		imp::shutdown((**self.handle()).as_descriptor(), how)
	}
}

impl<S: ListenerHandle> Facade<S> {
	/// Takes the next connection in: the stream it made, in a facade of its own.
	pub fn accept(&self) -> Accept<S> {
		Accept::new(self, Submitter::accept)
	}
}

impl SocketHandle for std::net::TcpStream {}
impl StreamHandle for std::net::TcpStream {}

impl SocketHandle for std::net::UdpSocket {}

impl ListenerHandle for std::net::TcpListener {
	type Stream = std::net::TcpStream;
}

#[cfg(unix)]
mod platform {
	use std::os::unix::net::{UnixDatagram, UnixListener, UnixStream};

	use super::{ListenerHandle, SocketHandle, StreamHandle};

	impl SocketHandle for UnixStream {}
	impl StreamHandle for UnixStream {}

	impl SocketHandle for UnixDatagram {}

	impl ListenerHandle for UnixListener {
		type Stream = UnixStream;
	}
}

/// The one synchronous socket call each platform spells for itself: the
/// shutdown how is `SHUT_RD`/`SHUT_WR`/`SHUT_RDWR` on unix and
/// `SD_RECEIVE`/`SD_SEND`/`SD_BOTH` on Windows, and `SO_ERROR` comes back
/// through a different call on each.
#[cfg(unix)]
mod imp {
	use std::io;
	use std::net::Shutdown;

	use crate::BorrowedDescriptor;

	pub fn shutdown(descriptor: BorrowedDescriptor<'_>, how: Shutdown) -> io::Result<()> {
		let how = match how {
			Shutdown::Read => rustix::net::Shutdown::Read,
			Shutdown::Write => rustix::net::Shutdown::Write,
			Shutdown::Both => rustix::net::Shutdown::Both,
		};
		rustix::net::shutdown(descriptor, how).map_err(io::Error::from)
	}

	pub fn take_error(descriptor: BorrowedDescriptor<'_>) -> io::Result<Option<io::Error>> {
		// `socket_error` reads and clears `SO_ERROR`: the outer result is the
		// `getsockopt` call, the inner the error the socket held.
		match rustix::net::sockopt::socket_error(descriptor)? {
			Ok(()) => Ok(None),
			Err(errno) => Ok(Some(errno.into())),
		}
	}
}

#[cfg(windows)]
mod imp {
	use std::io;
	use std::mem::size_of;
	use std::net::Shutdown;
	use std::os::windows::io::AsRawSocket;

	use windows_sys::Win32::Networking::WinSock::{
		SD_BOTH, SD_RECEIVE, SD_SEND, SO_ERROR, SOCKET, SOL_SOCKET, WSAGetLastError, getsockopt,
		shutdown as winsock_shutdown,
	};

	use crate::BorrowedDescriptor;

	pub fn shutdown(descriptor: BorrowedDescriptor<'_>, how: Shutdown) -> io::Result<()> {
		let how = match how {
			Shutdown::Read => SD_RECEIVE,
			Shutdown::Write => SD_SEND,
			Shutdown::Both => SD_BOTH,
		};
		if unsafe { winsock_shutdown(socket(descriptor)?, how) } != 0 {
			return Err(last_error());
		}
		Ok(())
	}

	pub fn take_error(descriptor: BorrowedDescriptor<'_>) -> io::Result<Option<io::Error>> {
		let mut value: i32 = 0;
		let mut len = size_of::<i32>() as i32;
		let socket = socket(descriptor)?;
		if unsafe {
			getsockopt(
				socket,
				SOL_SOCKET,
				SO_ERROR,
				&mut value as *mut i32 as *mut u8,
				&mut len,
			)
		} != 0
		{
			return Err(last_error());
		}
		Ok((value != 0).then(|| io::Error::from_raw_os_error(value)))
	}

	/// The socket a descriptor names: `shutdown` and `SO_ERROR` are socket
	/// calls, and a descriptor of the other family is not one.
	fn socket(descriptor: BorrowedDescriptor<'_>) -> io::Result<SOCKET> {
		match descriptor {
			BorrowedDescriptor::Socket(socket) => Ok(socket.as_raw_socket() as SOCKET),
			BorrowedDescriptor::Handle(_) => Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"a socket call on a non-socket descriptor",
			)),
		}
	}

	/// Winsock does not set `errno`: `WSAGetLastError` is where a failed call
	/// reports.
	fn last_error() -> io::Error {
		io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
	}
}
