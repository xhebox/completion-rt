//! Socket traits and async networking operations.

use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use completion_backend::{AsDescriptor, OwnedDescriptor, OwnedSocket, Response, Span};

use crate::Result;
use crate::completion::Cancel;
use crate::op::{Action, Facade, Gate, Kind, Op, Receive, Transfer};

/// Future returned by [`Facade::accept`] that resolves to an accepted stream
/// wrapped in a [`Facade`].
pub struct Accept<S> {
	gate: Gate,
	finished: bool,
	_stream: PhantomData<fn() -> S>,
}

impl<S> Accept<S> {
	pub(crate) fn new(gate: Gate) -> Accept<S> {
		Self {
			gate,
			finished: false,
			_stream: PhantomData,
		}
	}

	/// Cancels the accept when `cancel` is triggered.
	pub fn until(self, cancel: &Cancel) -> Accept<S> {
		let Accept { gate, .. } = self;
		Self {
			gate: gate.until(cancel),
			finished: false,
			_stream: PhantomData,
		}
	}
}

impl<S: ListenerHandle> Future for Accept<S> {
	type Output = Result<Facade<S::Stream>>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Facade<S::Stream>>> {
		let this = self.get_mut();
		assert!(!this.finished, "an accept is not polled after it resolved");
		match this.gate.poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(Err(error)) => {
				this.finished = true;
				Poll::Ready(Err(error))
			}
			Poll::Ready(Ok(Response::Accepted(socket))) => {
				this.finished = true;
				let reactor = this.gate.reactor().clone();
				Poll::Ready(Ok(Facade::from_parts(Arc::new(socket.into()), reactor)))
			}
			Poll::Ready(Ok(other)) => {
				unreachable!("an accept's round reports a socket, not {other:?}")
			}
		}
	}
}

/// Marker trait for datagram and stream sockets.
///
/// Implementing `SocketHandle` enables [`Facade::recv`], [`Facade::recv_with_fds`],
/// [`Facade::send`], [`Facade::send_with_fds`], and [`Facade::take_error`].
pub trait SocketHandle: AsDescriptor + Send + Sync + 'static {}

impl SocketHandle for std::net::UdpSocket {}
impl SocketHandle for std::net::TcpStream {}

#[cfg(unix)]
impl SocketHandle for std::os::unix::net::UnixDatagram {}
#[cfg(unix)]
impl SocketHandle for std::os::unix::net::UnixStream {}

#[cfg(windows)]
impl SocketHandle for OwnedSocket {}

/// Marker trait for connected byte-stream sockets (such as `TcpStream` and `UnixStream`).
///
/// Extends [`SocketHandle`] with [`Facade::recv_exact`], [`Facade::recv_exact_with_fds`],
/// [`Facade::send_all`], [`Facade::send_all_with_fds`], [`Facade::shutdown`], and
/// [`Facade::hangup`].
pub trait StreamHandle: SocketHandle {}

impl StreamHandle for std::net::TcpStream {}

#[cfg(unix)]
impl StreamHandle for std::os::unix::net::UnixStream {}

impl<S: SocketHandle> Facade<S> {
	/// Performs a single receive into `span`.
	///
	/// May read fewer bytes than `span.len()`; on stream sockets, use
	/// [`recv_exact`](Facade::recv_exact) to fill the entire span.
	pub fn recv(&self, span: Span) -> Transfer {
		Transfer::new(self, Kind::Recv, span)
	}

	/// Performs a single receive into `span`, also capturing any file descriptors
	/// passed via ancillary control messages (`SCM_RIGHTS`).
	pub fn recv_with_fds(&self, span: Span) -> Receive {
		Receive::new(self, Kind::Recv, span)
	}

	/// Performs a single send from `span`.
	///
	/// May write fewer bytes than `span.len()`; on stream sockets, use
	/// [`send_all`](Facade::send_all) to send the entire span.
	pub fn send(&self, span: Span) -> Transfer {
		Transfer::new(self, Kind::Send, span)
	}

	/// Performs a single send from `span`, attaching `fds` as ancillary control
	/// messages (`SCM_RIGHTS`).
	pub fn send_with_fds(&self, span: Span, fds: Vec<OwnedDescriptor>) -> Transfer {
		Transfer::with_fds(self, Kind::Send, span, fds)
	}

	/// Retrieves and clears the socket's pending error (`SO_ERROR`).
	pub fn take_error(&self) -> io::Result<Option<io::Error>> {
		imp::take_error((**self.resource()).as_descriptor())
	}
}

impl<S: StreamHandle> Facade<S> {
	/// Receives into `span` until it is completely filled.
	///
	/// Fails with [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) if the peer
	/// closes the connection before `span` is full.
	pub fn recv_exact(&self, span: Span) -> Transfer {
		Transfer::new(self, Kind::RecvAll, span)
	}

	/// Receives into `span` until it is completely filled, collecting all file
	/// descriptors received across calls.
	pub fn recv_exact_with_fds(&self, span: Span) -> Receive {
		Receive::new(self, Kind::RecvAll, span)
	}

	/// Sends all bytes in `span` across one or more calls.
	///
	/// Fails with [`WriteZero`](std::io::ErrorKind::WriteZero) if a call transfers
	/// zero bytes.
	pub fn send_all(&self, span: Span) -> Transfer {
		Transfer::new(self, Kind::SendAll, span)
	}

	/// Sends all bytes in `span`, attaching `fds` to the first send call.
	pub fn send_all_with_fds(&self, span: Span, fds: Vec<OwnedDescriptor>) -> Transfer {
		Transfer::with_fds(self, Kind::SendAll, span, fds)
	}

	/// Shuts down the read, write, or both halves of the stream.
	pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
		imp::shutdown((**self.resource()).as_descriptor(), how)
	}

	/// Waits until the connection is fully closed in both directions or reset.
	///
	/// A peer half-close (`SHUT_WR` / FIN) alone does not resolve this future while
	/// the local write half remains open. Supported on `io_uring` (Linux 5.13+);
	/// other backends fail with [`Unsupported`](std::io::ErrorKind::Unsupported).
	pub fn hangup(&self) -> Op {
		Op::new(self, Action::Hangup)
	}
}

/// Trait for listening sockets that accept incoming stream connections.
///
/// Implementing `ListenerHandle` enables [`Facade::accept`], which wraps each
/// accepted [`OwnedSocket`] into `Facade<Self::Stream>`.
pub trait ListenerHandle: AsDescriptor + Send + Sync + 'static {
	/// Connected stream type produced by [`Facade::accept`].
	type Stream: StreamHandle + From<OwnedSocket>;
}

impl ListenerHandle for std::net::TcpListener {
	type Stream = std::net::TcpStream;
}

#[cfg(unix)]
impl ListenerHandle for std::os::unix::net::UnixListener {
	type Stream = std::os::unix::net::UnixStream;
}

impl<S: ListenerHandle> Facade<S> {
	/// Accepts the next incoming connection on this listener.
	pub fn accept(&self) -> Accept<S> {
		Accept::new(Gate::new(self, Action::Accept))
	}
}

#[cfg(unix)]
mod imp {
	use std::io;
	use std::net::Shutdown;

	use completion_backend::BorrowedDescriptor;

	pub(super) fn shutdown(descriptor: BorrowedDescriptor<'_>, how: Shutdown) -> io::Result<()> {
		let how = match how {
			Shutdown::Read => rustix::net::Shutdown::Read,
			Shutdown::Write => rustix::net::Shutdown::Write,
			Shutdown::Both => rustix::net::Shutdown::Both,
		};
		rustix::net::shutdown(descriptor, how).map_err(io::Error::from)
	}

	pub(super) fn take_error(descriptor: BorrowedDescriptor<'_>) -> io::Result<Option<io::Error>> {
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

	use completion_backend::BorrowedDescriptor;

	pub(super) fn shutdown(descriptor: BorrowedDescriptor<'_>, how: Shutdown) -> io::Result<()> {
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

	pub(super) fn take_error(descriptor: BorrowedDescriptor<'_>) -> io::Result<Option<io::Error>> {
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
