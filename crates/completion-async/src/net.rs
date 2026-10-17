//! Socket operations: the methods a facade over a socket has, and the accept
//! that makes one more.

use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use completion_rt::{AsDescriptor, OwnedDescriptor, OwnedSocket, Response, Span};

use crate::Error;
use crate::completion::Cancel;
use crate::op::{Action, Facade, Gate, Kind, Op, Receive, Transfer};

/// An accept: the connection the kernel made, in a facade of its own.
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

	/// Gives the accept up when `cancel` fires. A connection the kernel made
	/// anyway goes with the future, and is closed with it.
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
	type Output = Result<Facade<S::Stream>, Error>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Facade<S::Stream>, Error>> {
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

/// A socket: a resource that sends and receives, with message boundaries
/// ([`UdpSocket`](std::net::UdpSocket), `UnixDatagram`) or as a byte stream.
///
/// Implementing this is what gives a facade [`recv`](Facade::recv),
/// [`send`](Facade::send) and their `_with_fds` forms. The descriptor has to be
/// a socket: on anything else these fail with the OS's `ENOTSOCK` (or
/// `WSAENOTSOCK`).
pub trait SocketHandle: AsDescriptor + Send + Sync + 'static {}

impl SocketHandle for std::net::UdpSocket {}
impl SocketHandle for std::net::TcpStream {}

#[cfg(unix)]
impl SocketHandle for std::os::unix::net::UnixDatagram {}
#[cfg(unix)]
impl SocketHandle for std::os::unix::net::UnixStream {}

#[cfg(windows)]
impl SocketHandle for OwnedSocket {}

/// A connected byte-stream socket: TCP, `AF_UNIX` `SOCK_STREAM`, `AF_VSOCK`.
///
/// Implementing this is what gives a facade [`recv_exact`](Facade::recv_exact),
/// [`send_all`](Facade::send_all), [`shutdown`](Facade::shutdown) and
/// [`hangup`](Facade::hangup). The socket has no message boundaries, so a
/// receive that comes back short can be repeated for the rest; a zero-byte
/// receive is the peer's EOF; and `shutdown(Write)` sends this side's EOF. A
/// datagram socket has none of that: its exact forms would split or drop
/// messages, and its hangup would never come.
pub trait StreamHandle: SocketHandle {}

impl StreamHandle for std::net::TcpStream {}

#[cfg(unix)]
impl StreamHandle for std::os::unix::net::UnixStream {}

impl<S: SocketHandle> Facade<S> {
	/// Receives into `span`, in one call: a short count is what arrived, not
	/// an error. [`recv_exact`](Facade::recv_exact) loops.
	pub fn recv(&self, span: Span) -> Transfer {
		Transfer::new(self, Kind::Recv, span)
	}

	/// Receives into `span`, taking the descriptors the control messages
	/// carry with the bytes.
	pub fn recv_with_fds(&self, span: Span) -> Receive {
		Receive::new(self, Kind::Recv, span)
	}

	/// Sends `span`, in one call: a short count is what the call took, not an
	/// error. [`send_all`](Facade::send_all) loops.
	pub fn send(&self, span: Span) -> Transfer {
		Transfer::new(self, Kind::Send, span)
	}

	/// Sends `span`, carrying `fds` with the bytes.
	///
	/// One call carries the descriptors; see
	/// [`send_all_with_fds`](Facade::send_all_with_fds) for a send that loops.
	pub fn send_with_fds(&self, span: Span, fds: Vec<OwnedDescriptor>) -> Transfer {
		Transfer::with_fds(self, Kind::Send, span, fds)
	}

	/// Takes and clears the socket's pending error. Failure to read the error is returned as the outer `Err`.
	pub fn take_error(&self) -> io::Result<Option<io::Error>> {
		imp::take_error((**self.resource()).as_descriptor())
	}
}

impl<S: StreamHandle> Facade<S> {
	/// Receives into `span`, over as many calls as it takes to fill it.
	///
	/// [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) when the peer closes
	/// first.
	pub fn recv_exact(&self, span: Span) -> Transfer {
		Transfer::new(self, Kind::RecvAll, span)
	}

	/// [`recv_with_fds`](Facade::recv_with_fds) over as many calls as it takes
	/// to fill `span`, gathering the descriptors every call brings.
	pub fn recv_exact_with_fds(&self, span: Span) -> Receive {
		Receive::new(self, Kind::RecvAll, span)
	}

	/// Sends `span`, over as many calls as it takes.
	///
	/// [`WriteZero`](std::io::ErrorKind::WriteZero) when a call takes nothing.
	pub fn send_all(&self, span: Span) -> Transfer {
		Transfer::new(self, Kind::SendAll, span)
	}

	/// Sends all of `span`, attaching `fds` only to the first call's bytes.
	pub fn send_all_with_fds(&self, span: Span, fds: Vec<OwnedDescriptor>) -> Transfer {
		Transfer::with_fds(self, Kind::SendAll, span, fds)
	}

	/// Shuts down one or both directions. `Write` sends EOF to the peer without closing the receive side.
	pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
		imp::shutdown((**self.resource()).as_descriptor(), how)
	}

	/// Resolves once the connection is gone both ways, or has failed.
	///
	/// A peer that shut only its write side has not hung up: the stream
	/// still takes sends. On TCP a peer's close arrives as that same
	/// half-close, so the hangup comes once this side shuts its write
	/// side too, or the connection resets. io_uring only; the other
	/// backends fail it with [`Unsupported`](std::io::ErrorKind::Unsupported).
	pub fn hangup(&self) -> Op {
		Op::new(self, Action::Hangup)
	}
}

/// A listening socket whose accepted connections have a type of their own.
///
/// Implementing this is what gives a listener [`Facade::accept`]; the stream it
/// yields is the associated type, so an accepted connection is a facade over
/// the concrete socket type rather than one the caller narrows. `Stream` has
/// to be the type of the connections this listener accepts: the accepted
/// socket goes to its `From<OwnedSocket>` as it is.
pub trait ListenerHandle: AsDescriptor + Send + Sync + 'static {
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
	/// Takes the next connection in: the stream it made, in a facade of its own.
	pub fn accept(&self) -> Accept<S> {
		Accept::new(Gate::new(self, Action::Accept))
	}
}

#[cfg(unix)]
mod imp {
	use std::io;
	use std::net::Shutdown;

	use completion_rt::BorrowedDescriptor;

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

	use completion_rt::BorrowedDescriptor;

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
