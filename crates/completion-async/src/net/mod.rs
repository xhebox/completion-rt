use std::error::Error as _;
use std::future::Future;
use std::io;
use std::net::{Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use completion_backend::{
	AsDescriptor, BorrowedDescriptor, Op, OwnedDescriptor, OwnedSocket, Response,
};
use socket2::SockAddr;

use crate::io::{drain, fill, transfer};
use crate::memory::AsIoVec;
use crate::{Result, submit};

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
		transfer(self.clone(), buf, move |iovec| Op::Read {
			offset: None,
			buf: iovec,
		})
	}

	/// Reads into `buf` until it is completely filled.
	pub fn read_exact<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		fill(self.clone(), buf, |_done, iovec| Op::Read {
			offset: None,
			buf: iovec,
		})
	}

	/// Performs a single write from `buf`.
	pub fn write<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(self.clone(), buf, move |iovec| Op::Write {
			offset: None,
			buf: iovec,
		})
	}

	/// Writes all bytes in `buf`, across one or more calls.
	pub fn write_all<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		drain(self.clone(), buf, |_done, iovec| Op::Write {
			offset: None,
			buf: iovec,
		})
	}

	/// Waits once until the descriptor becomes readable.
	///
	/// This is a one-shot wait; call `readable` again to wait for subsequent
	/// readiness events.
	pub fn readable(&self) -> impl Future<Output = Result<()>> + use<> {
		let stream = self.clone();
		async move {
			submit(stream, Op::Readable, ())?
				.await
				.0
				.map(|_response| ())
		}
	}

	/// Waits once until the descriptor becomes writable.
	///
	/// This is a one-shot wait; call `writable` again to wait for subsequent
	/// readiness events.
	pub fn writable(&self) -> impl Future<Output = Result<()>> + use<> {
		let stream = self.clone();
		async move {
			submit(stream, Op::Writable, ())?
				.await
				.0
				.map(|_response| ())
		}
	}
}

impl AsDescriptor for Stream {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.as_descriptor()
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

	/// Wraps this socket in a [`PollSocket`](crate::PollSocket) implementing
	/// [`futures_io::AsyncRead`] and [`futures_io::AsyncWrite`] (and
	/// `tokio::io::{AsyncRead, AsyncWrite}` when the `tokio-io` feature is
	/// enabled).
	#[cfg(feature = "compat")]
	pub fn into_poll(self) -> io::Result<crate::PollSocket> {
		crate::PollSocket::new(self)
	}

	#[cfg(feature = "compat")]
	pub(crate) fn inner_fd(&self) -> &OwnedSocket {
		&self.0
	}

	/// Performs a single receive into `buf`.
	pub fn recv<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(self.clone(), buf, move |iovec| Op::Recv { buf: iovec })
	}

	/// Receives into `buf` until it is completely filled.
	pub fn recv_exact<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		fill(self.clone(), buf, |_done, iovec| Op::Recv { buf: iovec })
	}

	/// Performs a single send from `buf`.
	pub fn send<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(self.clone(), buf, move |iovec| Op::Send { buf: iovec })
	}

	/// Sends all bytes in `buf` across one or more calls.
	pub fn send_all<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		drain(self.clone(), buf, |_done, iovec| Op::Send { buf: iovec })
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
		let mut socket = self.clone();
		async move {
			loop {
				let (result, returned, ()) = submit(socket, Op::Hangup, ())?.await;
				socket = returned;
				match result {
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

	/// Waits once until the socket becomes readable.
	///
	/// This is a one-shot wait; call `readable` again to wait for subsequent
	/// readiness events.
	pub fn readable(&self) -> impl Future<Output = Result<()>> + use<> {
		let socket = self.clone();
		async move {
			submit(socket, Op::Readable, ())?
				.await
				.0
				.map(|_response| ())
		}
	}

	/// Waits once until the socket becomes writable.
	///
	/// This is a one-shot wait; call `writable` again to wait for subsequent
	/// readiness events.
	pub fn writable(&self) -> impl Future<Output = Result<()>> + use<> {
		let socket = self.clone();
		async move {
			submit(socket, Op::Writable, ())?
				.await
				.0
				.map(|_response| ())
		}
	}
}

impl AsDescriptor for StreamSocket {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.as_descriptor()
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
		transfer(self.clone(), buf, move |iovec| Op::Recv { buf: iovec })
	}

	/// Performs a single send from `buf`.
	pub fn send<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(self.clone(), buf, move |iovec| Op::Send { buf: iovec })
	}

	/// Retrieves and clears the socket's pending error (`SO_ERROR`).
	pub fn take_error(&self) -> io::Result<Option<io::Error>> {
		imp::take_error(self.0.as_descriptor())
	}

	/// Waits once until the socket becomes readable.
	///
	/// This is a one-shot wait; call `readable` again to wait for subsequent
	/// readiness events.
	pub fn readable(&self) -> impl Future<Output = Result<()>> + use<> {
		let socket = self.clone();
		async move {
			submit(socket, Op::Readable, ())?
				.await
				.0
				.map(|_response| ())
		}
	}

	/// Waits once until the socket becomes writable.
	///
	/// This is a one-shot wait; call `writable` again to wait for subsequent
	/// readiness events.
	pub fn writable(&self) -> impl Future<Output = Result<()>> + use<> {
		let socket = self.clone();
		async move {
			submit(socket, Op::Writable, ())?
				.await
				.0
				.map(|_response| ())
		}
	}
}

impl AsDescriptor for DatagramSocket {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.as_descriptor()
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
		let listener = self.clone();
		async move {
			match submit(listener, Op::Accept, ())?.await.0? {
				Response::Accepted(socket) => Ok(StreamSocket::new(socket)),
				other => unreachable!("an accept's round reports a socket, not {other:?}"),
			}
		}
	}
}

impl AsDescriptor for Listener {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.as_descriptor()
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

async fn connected(connection: io::Result<(SockAddr, StreamSocket)>) -> Result<StreamSocket> {
	let (addr, socket) = connection?;
	let (response, socket, ()) = submit(socket, Op::Connect { addr }, ())?.await;
	response?;
	Ok(socket)
}

fn connect_socket(addr: SockAddr) -> io::Result<(SockAddr, StreamSocket)> {
	let socket = socket2::Socket::new(addr.domain(), socket2::Type::STREAM, None)?;
	socket.set_nonblocking(true)?;
	Ok((addr, StreamSocket::new(socket)))
}

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix as imp;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as imp;

#[cfg(test)]
mod tests {
	use std::net::{
		Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, TcpListener, UdpSocket,
	};
	use std::path::{Path, PathBuf};

	use assert2::assert;
	use rstest::rstest;
	use socket2::SockAddr;

	use super::{DatagramSocket, Listener, Stream, ToSockAddr};

	#[rstest]
	#[case::v4(SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)))]
	#[case::v6(SocketAddr::from((Ipv6Addr::LOCALHOST, 8080)))]
	fn to_sock_addr_converts_ip_socket_addresses_and_references(#[case] addr: SocketAddr) {
		assert!(addr.to_sock_addr().unwrap().as_socket() == Some(addr));

		let sock = SockAddr::from(addr);
		assert!(sock.to_sock_addr().unwrap().as_socket() == Some(addr));

		let reference = &addr;
		assert!(ToSockAddr::to_sock_addr(&reference).unwrap().as_socket() == Some(addr));
	}

	#[test]
	fn to_sock_addr_converts_typed_ip_addresses() {
		let v4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080);
		assert!(v4.to_sock_addr().unwrap().as_socket() == Some(SocketAddr::V4(v4)));

		let v6 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);
		assert!(v6.to_sock_addr().unwrap().as_socket() == Some(SocketAddr::V6(v6)));
	}

	#[rstest]
	#[case::relative("test.sock")]
	#[case::nested("run/test.sock")]
	fn to_sock_addr_converts_unix_paths(#[case] name: &str) {
		let path = Path::new(name);
		let owned = PathBuf::from(name);
		#[cfg(unix)]
		{
			assert!(path.to_sock_addr().unwrap().as_pathname() == Some(path));
			assert!(owned.to_sock_addr().unwrap().as_pathname() == Some(path));
		}
		#[cfg(not(unix))]
		{
			path.to_sock_addr().unwrap();
			owned.to_sock_addr().unwrap();
		}
	}

	#[test]
	fn try_take_returns_resource_only_when_handle_is_unique() {
		let (reader, _writer) = Stream::pipe().unwrap();
		let clone = reader.clone();
		assert!(clone.try_take().is_none());
		assert!(reader.try_take().is_some());

		let socket = DatagramSocket::new(UdpSocket::bind("127.0.0.1:0").unwrap());
		let clone = socket.clone();
		assert!(clone.try_take().is_none());
		assert!(socket.try_take().is_some());

		let listener = Listener::new(TcpListener::bind("127.0.0.1:0").unwrap());
		let clone = listener.clone();
		assert!(clone.try_take().is_none());
		assert!(listener.try_take().is_some());
	}
}
