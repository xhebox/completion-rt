use std::error::Error as _;
use std::future::Future;
use std::io;
use std::net::{Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use completion_backend::{AsDescriptor, Op, OwnedDescriptor, OwnedSocket, Response};
use socket2::SockAddr;

use crate::Result;
use crate::executor::submit;
use crate::io::{Outcome, drain, fill, transfer};
use crate::memory::AsIoVec;

// ---------------------------------------------------------------------------
// Stream (pipe)
// ---------------------------------------------------------------------------

/// Handle for a non-seekable byte stream that is not a socket (such as an
/// anonymous or named pipe).
///
/// Transfers advance the descriptor's own position. On Windows with the IOCP
/// backend, handles passed to [`Stream::new`] must be opened with
/// `FILE_FLAG_OVERLAPPED` (use [`Stream::pipe`], which sets it automatically).
#[derive(Clone)]
pub struct Stream(Arc<OwnedDescriptor>);

impl Stream {
	/// Wraps `stream`.
	#[fastrace::trace]
	pub fn new(stream: impl Into<OwnedDescriptor>) -> Stream {
		Stream(Arc::new(stream.into()))
	}

	/// Creates a connected pair of anonymous pipe streams `(reader, writer)`.
	///
	/// On Windows, both ends are opened with `FILE_FLAG_OVERLAPPED` so they can
	/// be driven by the IOCP backend as well as the portable backend.
	#[fastrace::trace]
	pub fn pipe() -> io::Result<(Stream, Stream)> {
		let (reader, writer) = imp::pipe()?;
		Ok((Stream::new(reader), Stream::new(writer)))
	}

	/// Unwraps the underlying [`OwnedDescriptor`] if no clones of this handle
	/// and no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedDescriptor> {
		Arc::into_inner(self.0)
	}

	/// Performs a single read into `buf`.
	pub fn read<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Read {
			offset: None,
			buf: iovec,
		})
	}

	/// Reads into `buf` until it is completely filled.
	pub fn read_exact<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Outcome<usize>> + 'static {
		fill(Arc::clone(&self.0), buf, |_done, iovec| Op::Read {
			offset: None,
			buf: iovec,
		})
	}

	/// Performs a single write from `buf`.
	pub fn write<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Write {
			offset: None,
			buf: iovec,
		})
	}

	/// Writes all bytes in `buf`, across one or more calls.
	pub fn write_all<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Outcome<usize>> + 'static {
		drain(Arc::clone(&self.0), buf, |_done, iovec| Op::Write {
			offset: None,
			buf: iovec,
		})
	}
}

// ---------------------------------------------------------------------------
// StreamSocket (TCP / Unix stream socket)
// ---------------------------------------------------------------------------

/// Handle for a connected stream socket.
///
/// Transfers use socket `recv`/`send` semantics.
#[derive(Clone)]
pub struct StreamSocket(Arc<OwnedSocket>);

impl StreamSocket {
	/// Wraps `socket`.
	#[fastrace::trace]
	pub fn new(socket: impl Into<OwnedSocket>) -> StreamSocket {
		StreamSocket(Arc::new(socket.into()))
	}

	/// Connects a new stream socket to `addr`.
	#[fastrace::trace]
	pub fn connect(addr: impl ToSockAddr) -> impl Future<Output = Result<StreamSocket>> {
		connected(addr.to_sock_addr().and_then(connect_socket))
	}

	/// Unwraps the underlying [`OwnedSocket`] if no clones of this handle
	/// and no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedSocket> {
		Arc::into_inner(self.0)
	}

	/// Performs a single receive into `buf`.
	pub fn recv<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Recv {
			buf: iovec,
		})
	}

	/// Receives into `buf` until it is completely filled.
	pub fn recv_exact<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Outcome<usize>> + 'static {
		fill(Arc::clone(&self.0), buf, |_done, iovec| Op::Recv {
			buf: iovec,
		})
	}

	/// Performs a single send from `buf`.
	pub fn send<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Send {
			buf: iovec,
		})
	}

	/// Sends all bytes in `buf` across one or more calls.
	pub fn send_all<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Outcome<usize>> + 'static {
		drain(Arc::clone(&self.0), buf, |_done, iovec| Op::Send {
			buf: iovec,
		})
	}

	/// Shuts down the read, write, or both halves of the stream.
	pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
		imp::shutdown(self.0.as_descriptor(), how)
	}

	/// Retrieves and clears the socket's pending error (`SO_ERROR`).
	pub fn take_error(&self) -> io::Result<Option<io::Error>> {
		imp::take_error(self.0.as_descriptor())
	}

	/// Waits until the connection is fully closed in both directions or reset.
	pub fn hangup(&self) -> impl Future<Output = Result<()>> + use<> {
		let fd = Arc::clone(&self.0);
		async move {
			loop {
				match submit(Arc::clone(&fd), Op::Hangup, ())?.await.0 {
					Err(error)
						if error.source().is_some_and(|s| {
							s.downcast_ref::<io::Error>()
								.is_some_and(|e| e.kind() == io::ErrorKind::WouldBlock)
						}) =>
					{
						continue;
					}
					result => return result.map(|_response| ()),
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// DatagramSocket (UDP / Unix dgram)
// ---------------------------------------------------------------------------

/// Handle for a connected datagram socket.
#[derive(Clone)]
pub struct DatagramSocket(Arc<OwnedSocket>);

impl DatagramSocket {
	/// Wraps `socket`.
	#[fastrace::trace]
	pub fn new(socket: impl Into<OwnedSocket>) -> DatagramSocket {
		DatagramSocket(Arc::new(socket.into()))
	}

	/// Unwraps the underlying [`OwnedSocket`] if no clones of this handle and
	/// no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedSocket> {
		Arc::into_inner(self.0)
	}

	/// Performs a single receive into `buf`.
	pub fn recv<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Recv {
			buf: iovec,
		})
	}

	/// Performs a single send from `buf`.
	pub fn send<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Send {
			buf: iovec,
		})
	}

	/// Retrieves and clears the socket's pending error (`SO_ERROR`).
	pub fn take_error(&self) -> io::Result<Option<io::Error>> {
		imp::take_error(self.0.as_descriptor())
	}
}

// ---------------------------------------------------------------------------
// Listener
// ---------------------------------------------------------------------------

/// Handle for a listening socket.
#[derive(Clone)]
pub struct Listener(Arc<OwnedSocket>);

impl Listener {
	/// Wraps `listener`.
	#[fastrace::trace]
	pub fn new(listener: impl Into<OwnedSocket>) -> Listener {
		Listener(Arc::new(listener.into()))
	}

	/// Unwraps the underlying [`OwnedSocket`] if no clones of this handle and
	/// no in-flight accept operations remain.
	pub fn try_take(self) -> Option<OwnedSocket> {
		Arc::into_inner(self.0)
	}

	/// Accepts the next incoming connection on this listener.
	pub fn accept(&self) -> impl Future<Output = Result<StreamSocket>> + 'static {
		let fd = Arc::clone(&self.0);
		async move {
			match submit(fd, Op::Accept, ())?.await.0? {
				Response::Accepted(socket) => Ok(StreamSocket::new(socket)),
				other => unreachable!("an accept's round reports a socket, not {other:?}"),
			}
		}
	}
}

// ---------------------------------------------------------------------------
// ToSockAddr
// ---------------------------------------------------------------------------

/// Socket address convertible for [`StreamSocket::connect`].
pub trait ToSockAddr {
	/// Converts `self` into a [`socket2::SockAddr`].
	fn to_sock_addr(&self) -> io::Result<SockAddr>;
}

impl ToSockAddr for SockAddr {
	fn to_sock_addr(&self) -> io::Result<SockAddr> {
		Ok(self.clone())
	}
}

impl ToSockAddr for SocketAddr {
	fn to_sock_addr(&self) -> io::Result<SockAddr> {
		Ok(SockAddr::from(*self))
	}
}

impl ToSockAddr for SocketAddrV4 {
	fn to_sock_addr(&self) -> io::Result<SockAddr> {
		Ok(SockAddr::from(*self))
	}
}

impl ToSockAddr for SocketAddrV6 {
	fn to_sock_addr(&self) -> io::Result<SockAddr> {
		Ok(SockAddr::from(*self))
	}
}

impl ToSockAddr for Path {
	fn to_sock_addr(&self) -> io::Result<SockAddr> {
		SockAddr::unix(self)
	}
}

impl ToSockAddr for PathBuf {
	fn to_sock_addr(&self) -> io::Result<SockAddr> {
		SockAddr::unix(self)
	}
}

impl<T: ToSockAddr + ?Sized> ToSockAddr for &T {
	fn to_sock_addr(&self) -> io::Result<SockAddr> {
		(**self).to_sock_addr()
	}
}

async fn connected(connection: io::Result<(SockAddr, Arc<OwnedSocket>)>) -> Result<StreamSocket> {
	let (addr, socket) = connection?;
	submit(Arc::clone(&socket), Op::Connect { addr }, ())?
		.await
		.0?;
	Ok(StreamSocket(socket))
}

fn connect_socket(addr: SockAddr) -> io::Result<(SockAddr, Arc<OwnedSocket>)> {
	let socket = socket2::Socket::new(addr.domain(), socket2::Type::STREAM, None)?;
	socket.set_nonblocking(true)?;
	Ok((addr, Arc::new(OwnedSocket::from(socket))))
}

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix as imp;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as imp;
