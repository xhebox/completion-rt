use std::error::Error as _;
use std::future::Future;
use std::io;
use std::net::{Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use completion_backend::{AsDescriptor, Error, Op, OwnedDescriptor, OwnedSocket, Response, Span};
use socket2::SockAddr;

use crate::Result;
use crate::executor::submit;
use crate::io::{Outcome, counted, drain, fill};

/// Payload of a receive that collects ancillary descriptors alongside bytes.
#[derive(Debug)]
pub struct Received {
	/// Number of payload bytes received.
	pub bytes: usize,
	/// Descriptors received via ancillary control messages (`SCM_RIGHTS`).
	pub fds: Vec<OwnedDescriptor>,
}

fn received(landed: Result<Response>) -> Result<Received> {
	match landed? {
		Response::CountWithFds(bytes, fds) => Ok(Received { bytes, fds }),
		other => unreachable!("a receive's round reports a count, not {other:?}"),
	}
}

async fn recv_with_fds(fd: Arc<dyn AsDescriptor>, span: Span) -> Result<Received> {
	received(submit(fd, Op::RecvWithFds { span }).await)
}

async fn recv_all_with_fds(fd: Arc<dyn AsDescriptor>, span: Span) -> Outcome<Received> {
	let mut done = 0;
	let mut fds = Vec::new();
	while done < span.len() {
		let round = Op::RecvWithFds {
			span: span.clone().skip(done),
		};
		match received(submit(Arc::clone(&fd), round).await) {
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

async fn send_all(
	fd: Arc<dyn AsDescriptor>,
	span: Span,
	mut fds: Vec<OwnedDescriptor>,
) -> Outcome<usize> {
	drain(fd, span, move |done, span| {
		let fds = if done == 0 {
			std::mem::take(&mut fds)
		} else {
			Vec::new()
		};
		Op::Send { span, fds }
	})
	.await
}

/// Handle for a connected byte stream (pipe, PTY, TAP, or stream socket).
///
/// Transfers advance the descriptor's own position (`read`/`write`); stream
/// sockets also support `recv`/`send`, ancillary descriptor passing, and
/// socket operations.
#[derive(Clone)]
pub struct Stream(Arc<OwnedDescriptor>);

impl Stream {
	/// Wraps `stream`.
	#[fastrace::trace]
	pub fn new(stream: impl Into<OwnedDescriptor>) -> Stream {
		Stream(Arc::new(stream.into()))
	}

	/// Connects a new stream socket to `addr`.
	///
	/// The socket domain (`AF_INET`, `AF_INET6`, `AF_UNIX`, etc.) is inferred
	/// from `addr`. The socket is created when `connect` is called and
	/// connected when the returned future is polled.
	#[fastrace::trace]
	pub fn connect(addr: impl ToSockAddr) -> impl Future<Output = Result<Stream>> {
		connected(addr.to_sock_addr().and_then(connect_socket))
	}

	/// Unwraps the underlying [`OwnedDescriptor`] if no clones of this handle
	/// and no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedDescriptor> {
		Arc::into_inner(self.0)
	}

	/// Performs a single read into `span`.
	///
	/// May transfer fewer bytes than `span.len()`; use
	/// [`read_exact`](Self::read_exact) to read until `span` is full.
	pub fn read(&self, span: impl Into<Span>) -> impl Future<Output = Result<usize>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		let span = span.into();
		async move { counted(submit(fd, Op::Read { offset: None, span }).await) }
	}

	/// Reads into `span` until it is completely filled.
	///
	/// Fails with [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) if the source
	/// ends before `span` is full, and reports the bytes it did read as the
	/// outcome's progress.
	pub fn read_exact(
		&self,
		span: impl Into<Span>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		fill(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			span.into(),
			|_done, span| Op::Read { offset: None, span },
		)
	}

	/// Performs a single write from `span`.
	///
	/// May transfer fewer bytes than `span.len()`; use
	/// [`write_all`](Self::write_all) to write the entire span.
	pub fn write(&self, span: impl Into<Span>) -> impl Future<Output = Result<usize>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		let span = span.into();
		async move { counted(submit(fd, Op::Write { offset: None, span }).await) }
	}

	/// Writes all bytes in `span`, across one or more calls.
	///
	/// Fails with [`WriteZero`](io::ErrorKind::WriteZero) if a single call
	/// transfers zero bytes, and reports the bytes it did write as the outcome's
	/// progress.
	pub fn write_all(
		&self,
		span: impl Into<Span>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		drain(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			span.into(),
			|_done, span| Op::Write { offset: None, span },
		)
	}

	/// Performs a single receive into `span`.
	///
	/// May read fewer bytes than `span.len()`; on a stream socket, use
	/// [`recv_exact`](Self::recv_exact) to fill the entire span.
	pub fn recv(&self, span: impl Into<Span>) -> impl Future<Output = Result<usize>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		let span = span.into();
		async move { counted(submit(fd, Op::Recv { span }).await) }
	}

	/// Receives into `span` until it is completely filled.
	///
	/// Fails with [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) if the peer
	/// closes the connection before `span` is full, and reports the bytes it did
	/// receive as the outcome's progress.
	pub fn recv_exact(
		&self,
		span: impl Into<Span>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		fill(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			span.into(),
			|_done, span| Op::Recv { span },
		)
	}

	/// Performs a single receive into `span`, also capturing any file
	/// descriptors passed via ancillary control messages (`SCM_RIGHTS`).
	pub fn recv_with_fds(
		&self,
		span: impl Into<Span>,
	) -> impl Future<Output = Result<Received>> + 'static {
		recv_with_fds(Arc::clone(&self.0) as Arc<dyn AsDescriptor>, span.into())
	}

	/// Receives into `span` until it is completely filled, collecting all file
	/// descriptors received across calls.
	pub fn recv_exact_with_fds(
		&self,
		span: impl Into<Span>,
	) -> impl Future<Output = Outcome<Received>> + 'static {
		recv_all_with_fds(Arc::clone(&self.0) as Arc<dyn AsDescriptor>, span.into())
	}

	/// Performs a single send from `span`.
	///
	/// May write fewer bytes than `span.len()`; on a stream socket, use
	/// [`send_all`](Self::send_all) to send the entire span.
	pub fn send(&self, span: impl Into<Span>) -> impl Future<Output = Result<usize>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		let span = span.into();
		async move {
			counted(
				submit(
					fd,
					Op::Send {
						span,
						fds: Vec::new(),
					},
				)
				.await,
			)
		}
	}

	/// Sends all bytes in `span` across one or more calls.
	///
	/// Fails with [`WriteZero`](io::ErrorKind::WriteZero) if a call transfers
	/// zero bytes, and reports the bytes it did send as the outcome's progress.
	pub fn send_all(
		&self,
		span: impl Into<Span>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		send_all(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			span.into(),
			Vec::new(),
		)
	}

	/// Performs a single send from `span`, attaching `fds` as ancillary control
	/// messages (`SCM_RIGHTS`).
	pub fn send_with_fds(
		&self,
		span: impl Into<Span>,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Result<usize>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		let span = span.into();
		async move { counted(submit(fd, Op::Send { span, fds }).await) }
	}

	/// Sends all bytes in `span`, attaching `fds` to the first send call.
	pub fn send_all_with_fds(
		&self,
		span: impl Into<Span>,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Outcome<usize>> + 'static {
		send_all(
			Arc::clone(&self.0) as Arc<dyn AsDescriptor>,
			span.into(),
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
	///
	/// A peer half-close (`SHUT_WR` / FIN) alone does not resolve this future
	/// while the local write half remains open. Supported on `io_uring` (Linux
	/// 5.13+); other backends fail with
	/// [`Unsupported`](io::ErrorKind::Unsupported).
	pub fn hangup(&self) -> impl Future<Output = Result<()>> + use<> {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		async move {
			// Re-arm if the kernel terminated the multishot poll early (e.g. on CQ
			// overflow), which the backend surfaces as `WouldBlock`.
			loop {
				match submit(Arc::clone(&fd), Op::Hangup).await {
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

/// Handle for a connected datagram socket.
#[derive(Clone)]
pub struct Socket(Arc<OwnedSocket>);

impl Socket {
	/// Wraps `socket`.
	#[fastrace::trace]
	pub fn new(socket: impl Into<OwnedSocket>) -> Socket {
		Socket(Arc::new(socket.into()))
	}

	/// Unwraps the underlying [`OwnedSocket`] if no clones of this handle and
	/// no in-flight operations remain.
	pub fn try_take(self) -> Option<OwnedSocket> {
		Arc::into_inner(self.0)
	}

	/// Performs a single receive into `span`.
	pub fn recv(&self, span: impl Into<Span>) -> impl Future<Output = Result<usize>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		let span = span.into();
		async move { counted(submit(fd, Op::Recv { span }).await) }
	}

	/// Performs a single receive into `span`, also capturing any file
	/// descriptors passed via ancillary control messages (`SCM_RIGHTS`).
	pub fn recv_with_fds(
		&self,
		span: impl Into<Span>,
	) -> impl Future<Output = Result<Received>> + 'static {
		recv_with_fds(Arc::clone(&self.0) as Arc<dyn AsDescriptor>, span.into())
	}

	/// Performs a single send from `span`.
	pub fn send(&self, span: impl Into<Span>) -> impl Future<Output = Result<usize>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		let span = span.into();
		async move {
			counted(
				submit(
					fd,
					Op::Send {
						span,
						fds: Vec::new(),
					},
				)
				.await,
			)
		}
	}

	/// Performs a single send from `span`, attaching `fds` as ancillary control
	/// messages (`SCM_RIGHTS`).
	pub fn send_with_fds(
		&self,
		span: impl Into<Span>,
		fds: Vec<OwnedDescriptor>,
	) -> impl Future<Output = Result<usize>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		let span = span.into();
		async move { counted(submit(fd, Op::Send { span, fds }).await) }
	}

	/// Retrieves and clears the socket's pending error (`SO_ERROR`).
	pub fn take_error(&self) -> io::Result<Option<io::Error>> {
		imp::take_error(self.0.as_descriptor())
	}
}

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
	pub fn accept(&self) -> impl Future<Output = Result<Stream>> + 'static {
		let fd = Arc::clone(&self.0) as Arc<dyn AsDescriptor>;
		async move {
			match submit(fd, Op::Accept).await? {
				Response::Accepted(socket) => Ok(Stream::new(socket)),
				other => unreachable!("an accept's round reports a socket, not {other:?}"),
			}
		}
	}
}

/// Socket address convertible for [`Stream::connect`].
///
/// Implemented for [`SocketAddr`], [`SocketAddrV4`], [`SocketAddrV6`],
/// [`Path`], [`PathBuf`] (Unix domain sockets), and [`socket2::SockAddr`].
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

async fn connected(connection: io::Result<(SockAddr, Arc<OwnedDescriptor>)>) -> Result<Stream> {
	let (addr, socket) = connection?;
	submit(
		Arc::clone(&socket) as Arc<dyn AsDescriptor>,
		Op::Connect { addr },
	)
	.await?;
	Ok(Stream(socket))
}

fn connect_socket(addr: SockAddr) -> io::Result<(SockAddr, Arc<OwnedDescriptor>)> {
	let socket = socket2::Socket::new(addr.domain(), socket2::Type::STREAM, None)?;
	socket.set_nonblocking(true)?;
	Ok((
		addr,
		Arc::new(OwnedDescriptor::from(OwnedSocket::from(socket))),
	))
}

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix as imp;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as imp;
