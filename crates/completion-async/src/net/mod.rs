use std::error::Error as _;
use std::future::Future;
use std::io;
use std::net::{Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use completion_backend::{AsDescriptor, Error, Op, OwnedDescriptor, OwnedSocket, Response};
use socket2::SockAddr;

use crate::Result;
use crate::executor::with_rt_or_default;
use crate::io::{Outcome, drain, fill, transfer};
use crate::memory::Buffer;

/// Payload of a receive that collects ancillary descriptors alongside bytes.
#[derive(Debug)]
pub struct Received {
	/// Number of payload bytes received.
	pub bytes: usize,
	/// Descriptors received via ancillary control messages (`SCM_RIGHTS`).
	pub fds: Vec<OwnedDescriptor>,
}

async fn recv_with_fds(fd: Arc<dyn AsDescriptor>, buffer: Buffer) -> Result<Received> {
	let op = Op::RecvWithFds {
		buf: buffer.resolve()?,
	};
	match with_rt_or_default(|rt| rt.submit(fd, op, Some(buffer.into_memory())))?.await? {
		Response::CountWithFds(bytes, fds) => Ok(Received { bytes, fds }),
		other => unreachable!("a receive reports a count, not {other:?}"),
	}
}

async fn recv_all_with_fds(fd: Arc<dyn AsDescriptor>, buffer: Buffer) -> Outcome<Received> {
	let mut done = 0;
	let mut fds = Vec::new();
	while done < buffer.len() {
		let remaining = buffer.clone().skip(done);
		match recv_with_fds(Arc::clone(&fd), remaining).await {
			Ok(round) => {
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

async fn send_all_impl(
	fd: Arc<dyn AsDescriptor>,
	buffer: Buffer,
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
pub struct Stream(Arc<OwnedDescriptor>);

impl Stream {
	/// Wraps `stream`.
	#[fastrace::trace]
	pub fn new(stream: impl Into<OwnedDescriptor>) -> Stream {
		Stream(Arc::new(stream.into()))
	}

	/// Unwraps the underlying [`OwnedDescriptor`] if no clones of this handle
	/// and no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedDescriptor> {
		Arc::into_inner(self.0)
	}

	/// Performs a single read into `buf`.
	pub fn read(&self, buf: impl Into<Buffer>) -> impl Future<Output = Result<usize>> + 'static {
		transfer(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			move |iovec| Op::Read {
				offset: None,
				buf: iovec,
			},
		)
	}

	/// Reads into `buf` until it is completely filled.
	pub fn read_exact(
		&self,
		buf: impl Into<Buffer>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		fill(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			|_done, iovec| Op::Read {
				offset: None,
				buf: iovec,
			},
		)
	}

	/// Performs a single write from `buf`.
	pub fn write(&self, buf: impl Into<Buffer>) -> impl Future<Output = Result<usize>> + 'static {
		transfer(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			move |iovec| Op::Write {
				offset: None,
				buf: iovec,
			},
		)
	}

	/// Writes all bytes in `buf`, across one or more calls.
	pub fn write_all(
		&self,
		buf: impl Into<Buffer>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		drain(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			|_done, iovec| Op::Write {
				offset: None,
				buf: iovec,
			},
		)
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
	pub fn recv(&self, buf: impl Into<Buffer>) -> impl Future<Output = Result<usize>> + 'static {
		transfer(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			move |iovec| Op::Recv { buf: iovec },
		)
	}

	/// Receives into `buf` until it is completely filled.
	pub fn recv_exact(
		&self,
		buf: impl Into<Buffer>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		fill(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			|_done, iovec| Op::Recv { buf: iovec },
		)
	}

	/// Performs a single receive into `buf`, also capturing any file
	/// descriptors passed via ancillary control messages (`SCM_RIGHTS`).
	pub fn recv_with_fds(
		&self,
		buf: impl Into<Buffer>,
	) -> impl Future<Output = Result<Received>> + 'static {
		recv_with_fds(Arc::clone(&self.0) as Arc<dyn AsDescriptor>, buf.into())
	}

	/// Receives into `buf` until it is completely filled, collecting all file
	/// descriptors received across calls.
	pub fn recv_exact_with_fds(
		&self,
		buf: impl Into<Buffer>,
	) -> impl Future<Output = Outcome<Received>> + 'static {
		recv_all_with_fds(Arc::clone(&self.0) as Arc<dyn AsDescriptor>, buf.into())
	}

	/// Performs a single send from `buf`.
	pub fn send(&self, buf: impl Into<Buffer>) -> impl Future<Output = Result<usize>> + 'static {
		transfer(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			move |iovec| Op::Send {
				buf: iovec,
				fds: Vec::new(),
			},
		)
	}

	/// Sends all bytes in `buf` across one or more calls.
	pub fn send_all(
		&self,
		buf: impl Into<Buffer>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		send_all_impl(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			Vec::new(),
		)
	}

	/// Performs a single send from `buf`, attaching `fds` as ancillary control
	/// messages (`SCM_RIGHTS`).
	pub fn send_with_fds(
		&self,
		buf: impl Into<Buffer>,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Result<usize>> + 'static {
		transfer(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			move |iovec| Op::Send { buf: iovec, fds },
		)
	}

	/// Sends all bytes in `buf`, attaching `fds` to the first send call.
	pub fn send_all_with_fds(
		&self,
		buf: impl Into<Buffer>,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		send_all_impl(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			fds,
		)
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
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		async move {
			loop {
				match with_rt_or_default(|rt| rt.submit(Arc::clone(&fd), Op::Hangup, None))?.await {
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
	pub fn recv(&self, buf: impl Into<Buffer>) -> impl Future<Output = Result<usize>> + 'static {
		transfer(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			move |iovec| Op::Recv { buf: iovec },
		)
	}

	/// Performs a single receive into `buf`, also capturing any file
	/// descriptors passed via ancillary control messages (`SCM_RIGHTS`).
	pub fn recv_with_fds(
		&self,
		buf: impl Into<Buffer>,
	) -> impl Future<Output = Result<Received>> + 'static {
		recv_with_fds(Arc::clone(&self.0) as Arc<dyn AsDescriptor>, buf.into())
	}

	/// Performs a single send from `buf`.
	pub fn send(&self, buf: impl Into<Buffer>) -> impl Future<Output = Result<usize>> + 'static {
		transfer(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			move |iovec| Op::Send {
				buf: iovec,
				fds: Vec::new(),
			},
		)
	}

	/// Performs a single send from `buf`, attaching `fds` as ancillary control
	/// messages (`SCM_RIGHTS`).
	pub fn send_with_fds(
		&self,
		buf: impl Into<Buffer>,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Result<usize>> + 'static {
		transfer(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			buf.into(),
			move |iovec| Op::Send { buf: iovec, fds },
		)
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
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		async move {
			match with_rt_or_default(|rt| rt.submit(fd, Op::Accept, None))?.await? {
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
	with_rt_or_default(|rt| {
		rt.submit(
			Arc::clone(&socket) as Arc<dyn AsDescriptor>,
			Op::Connect { addr },
			None,
		)
	})?
	.await?;
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
