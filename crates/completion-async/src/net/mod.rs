use std::error::Error as _;
use std::future::Future;
use std::io;
use std::net::{Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use completion_backend::{AsDescriptor, Error, IoVec, Op, OwnedDescriptor, OwnedSocket, Response};
use socket2::SockAddr;

use crate::Result;
use crate::executor::{Handle, submit};
use crate::io::{Outcome, drain, fill, transfer};
use crate::memory::AsIoVec;

/// Payload of a receive that collects ancillary descriptors alongside bytes.
#[derive(Debug)]
pub struct Received {
	/// Number of payload bytes received.
	pub bytes: usize,
	/// Descriptors received via ancillary control messages (`SCM_RIGHTS`).
	pub fds: Vec<OwnedDescriptor>,
}

async fn recv_with_fds<D: AsDescriptor + 'static, M: AsIoVec>(
	fd: Arc<Handle<D>>,
	buffer: M,
	offset: usize,
) -> Result<Received> {
	recv_with_fds_in(fd, buffer, offset)
		.await
		.map(|(received, _memory)| received)
}

/// Like [`recv_with_fds`], but hands the buffer back so the loop can file its
/// memory with the next round.
async fn recv_with_fds_in<D: AsDescriptor + 'static, M: AsIoVec>(
	fd: Arc<Handle<D>>,
	buffer: M,
	offset: usize,
) -> Result<(Received, M)> {
	let whole = buffer.as_iovec();
	// SAFETY: every caller keeps `offset` at or below the buffer's length, so
	// the rest of the range is inside it.
	let range = IoVec {
		ptr: unsafe { whole.ptr.add(offset) },
		len: whole.len - offset,
	};
	let op = Op::RecvWithFds { buf: range };
	let (response, buffer) = submit(fd, op, buffer)?.await;
	match response? {
		Response::CountWithFds(bytes, fds) => Ok((Received { bytes, fds }, buffer)),
		other => unreachable!("a receive reports a count, not {other:?}"),
	}
}

async fn recv_all_with_fds<D: AsDescriptor + 'static, M: AsIoVec>(
	fd: Arc<Handle<D>>,
	buffer: M,
) -> Outcome<Received> {
	// The loop owns the one buffer handle: each round moves it into the round's
	// entry, and the completion hands it back for the next. A round dropped in
	// flight keeps it in its entry until the kernel finishes.
	let mut buffer = buffer;
	let len = buffer.as_iovec().len;
	let mut done = 0;
	let mut fds = Vec::new();
	while done < len {
		match recv_with_fds_in(Arc::clone(&fd), buffer, done).await {
			Ok((round, returned)) => {
				buffer = returned;
				fds.extend(round.fds);
				if round.bytes == 0 {
					return Outcome::new(
						Received { bytes: done, fds },
						Err(Error::from(io::Error::new(
							io::ErrorKind::UnexpectedEof,
							"the source ended before the memory was full",
						))),
					);
				}
				done += round.bytes;
			}
			Err(error) => return Outcome::new(Received { bytes: done, fds }, Err(error)),
		}
	}
	Outcome::new(Received { bytes: done, fds }, Ok(()))
}

async fn send_all_impl<D: AsDescriptor + 'static, M: AsIoVec>(
	fd: Arc<Handle<D>>,
	buffer: M,
	mut fds: Vec<OwnedDescriptor>,
) -> Outcome<usize> {
	drain(fd, buffer, move |done, iovec| {
		let fds = if done == 0 {
			std::mem::take(&mut fds)
		} else {
			Vec::new()
		};
		Op::Send { buf: iovec, fds }
	})
	.await
}

// ---------------------------------------------------------------------------
// Stream (pipe / PTY / TAP)
// ---------------------------------------------------------------------------

/// Handle for a connected byte stream that is not a socket (pipe, PTY, TAP).
///
/// Transfers advance the descriptor's own position.
#[derive(Clone)]
pub struct Stream(Arc<Handle<OwnedDescriptor>>);

impl Stream {
	/// Wraps `stream`.
	#[fastrace::trace]
	pub fn new(stream: impl Into<OwnedDescriptor>) -> Stream {
		Stream(Arc::new(Handle::new(stream.into())))
	}

	/// Unwraps the underlying [`OwnedDescriptor`] if no clones of this handle
	/// and no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedDescriptor> {
		Arc::into_inner(self.0).map(Handle::into_inner)
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
/// Transfers use socket `recv`/`send` semantics; ancillary descriptor passing
/// is available through the `_with_fds` forms.
#[derive(Clone)]
pub struct StreamSocket(Arc<Handle<OwnedSocket>>);

impl StreamSocket {
	/// Wraps `socket`.
	#[fastrace::trace]
	pub fn new(socket: impl Into<OwnedSocket>) -> StreamSocket {
		StreamSocket(Arc::new(Handle::new(socket.into())))
	}

	/// Connects a new stream socket to `addr`.
	#[fastrace::trace]
	pub fn connect(addr: impl ToSockAddr) -> impl Future<Output = Result<StreamSocket>> {
		connected(addr.to_sock_addr().and_then(connect_socket))
	}

	/// Unwraps the underlying [`OwnedSocket`] if no clones of this handle
	/// and no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedSocket> {
		Arc::into_inner(self.0).map(Handle::into_inner)
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

	/// Performs a single receive into `buf`, also capturing any file
	/// descriptors passed via ancillary control messages (`SCM_RIGHTS`).
	pub fn recv_with_fds<M: AsIoVec>(
		&self,
		buf: M,
	) -> impl Future<Output = Result<Received>> + 'static {
		recv_with_fds(Arc::clone(&self.0), buf, 0)
	}

	/// Receives into `buf` until it is completely filled, collecting all file
	/// descriptors received across calls.
	pub fn recv_exact_with_fds<M: AsIoVec>(
		&self,
		buf: M,
	) -> impl Future<Output = Outcome<Received>> + 'static {
		recv_all_with_fds(Arc::clone(&self.0), buf)
	}

	/// Performs a single send from `buf`.
	pub fn send<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Send {
			buf: iovec,
			fds: Vec::new(),
		})
	}

	/// Sends all bytes in `buf` across one or more calls.
	pub fn send_all<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Outcome<usize>> + 'static {
		send_all_impl(Arc::clone(&self.0), buf, Vec::new())
	}

	/// Performs a single send from `buf`, attaching `fds` as ancillary control
	/// messages (`SCM_RIGHTS`).
	pub fn send_with_fds<M: AsIoVec>(
		&self,
		buf: M,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Send {
			buf: iovec,
			fds,
		})
	}

	/// Sends all bytes in `buf`, attaching `fds` to the first send call.
	pub fn send_all_with_fds<M: AsIoVec>(
		&self,
		buf: M,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		send_all_impl(Arc::clone(&self.0), buf, fds)
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
pub struct DatagramSocket(Arc<Handle<OwnedSocket>>);

impl DatagramSocket {
	/// Wraps `socket`.
	#[fastrace::trace]
	pub fn new(socket: impl Into<OwnedSocket>) -> DatagramSocket {
		DatagramSocket(Arc::new(Handle::new(socket.into())))
	}

	/// Unwraps the underlying [`OwnedSocket`] if no clones of this handle and
	/// no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedSocket> {
		Arc::into_inner(self.0).map(Handle::into_inner)
	}

	/// Performs a single receive into `buf`.
	pub fn recv<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Recv {
			buf: iovec,
		})
	}

	/// Performs a single receive into `buf`, also capturing any file
	/// descriptors passed via ancillary control messages (`SCM_RIGHTS`).
	pub fn recv_with_fds<M: AsIoVec>(
		&self,
		buf: M,
	) -> impl Future<Output = Result<Received>> + 'static {
		recv_with_fds(Arc::clone(&self.0), buf, 0)
	}

	/// Performs a single send from `buf`.
	pub fn send<M: AsIoVec>(&self, buf: M) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Send {
			buf: iovec,
			fds: Vec::new(),
		})
	}

	/// Performs a single send from `buf`, attaching `fds` as ancillary control
	/// messages (`SCM_RIGHTS`).
	pub fn send_with_fds<M: AsIoVec>(
		&self,
		buf: M,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Result<usize>> + 'static {
		transfer(Arc::clone(&self.0), buf, move |iovec| Op::Send {
			buf: iovec,
			fds,
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
pub struct Listener(Arc<Handle<OwnedSocket>>);

impl Listener {
	/// Wraps `listener`.
	#[fastrace::trace]
	pub fn new(listener: impl Into<OwnedSocket>) -> Listener {
		Listener(Arc::new(Handle::new(listener.into())))
	}

	/// Unwraps the underlying [`OwnedSocket`] if no clones of this handle and
	/// no in-flight accept operations remain.
	pub fn try_take(self) -> Option<OwnedSocket> {
		Arc::into_inner(self.0).map(Handle::into_inner)
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

async fn connected(
	connection: io::Result<(SockAddr, Arc<Handle<OwnedSocket>>)>,
) -> Result<StreamSocket> {
	let (addr, socket) = connection?;
	submit(Arc::clone(&socket), Op::Connect { addr }, ())?
		.await
		.0?;
	Ok(StreamSocket(socket))
}

fn connect_socket(addr: SockAddr) -> io::Result<(SockAddr, Arc<Handle<OwnedSocket>>)> {
	let socket = socket2::Socket::new(addr.domain(), socket2::Type::STREAM, None)?;
	socket.set_nonblocking(true)?;
	Ok((addr, Arc::new(Handle::new(OwnedSocket::from(socket)))))
}

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix as imp;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as imp;
