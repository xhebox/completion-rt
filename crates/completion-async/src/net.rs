//! Socket operations and the futures that drive them.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use completion_rt::{
	AsDescriptor, Handle, Op as LowOp, OwnedDescriptor, OwnedSocket, Reactor, Response,
};

use crate::completion::{Cancel, Completion, Until, submit};
use crate::outcome::Outcome;
use crate::space::Span;
use crate::{Error, Facade};

#[derive(Clone, Copy)]
pub(super) enum Kind {
	Read { fdoff: u64 },
	Write { fdoff: u64 },
	ReadAll { fdoff: u64 },
	WriteAll { fdoff: u64 },
	Recv,
	RecvAll,
	Send,
	SendAll,
}

impl Kind {
	fn mode(self) -> Mode {
		match self {
			Kind::Read { .. } | Kind::Write { .. } | Kind::Recv | Kind::Send => Mode::Once,
			Kind::ReadAll { .. } | Kind::RecvAll => Mode::Fill,
			Kind::WriteAll { .. } | Kind::SendAll => Mode::Drain,
		}
	}
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
	Once,
	Fill,
	Drain,
}

pub(super) enum Control {
	None,
	Out(Vec<OwnedDescriptor>),
	In(Vec<OwnedDescriptor>),
}

enum Call {
	Idle,
	Plain(Completion),
	Armed(Until),
}

impl Call {
	fn idle() -> Call {
		Call::Idle
	}

	fn arm(&mut self, completion: Completion, cancel: Option<&Cancel>) {
		*self = match cancel {
			Some(cancel) => Call::Armed(completion.until(cancel)),
			None => Call::Plain(completion),
		};
	}

	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Response, Error>>> {
		match std::mem::replace(self, Call::Idle) {
			Call::Idle => unreachable!("a round is polled only with one in flight"),
			Call::Plain(mut completion) => match Pin::new(&mut completion).poll(cx) {
				Poll::Pending => {
					*self = Call::Plain(completion);
					Poll::Pending
				}
				Poll::Ready(result) => Poll::Ready(Some(result)),
			},
			Call::Armed(mut until) => match Pin::new(&mut until).poll(cx) {
				Poll::Pending => {
					*self = Call::Armed(until);
					Poll::Pending
				}
				Poll::Ready(Ok(result)) => Poll::Ready(Some(result)),
				Poll::Ready(Err(_)) => Poll::Ready(None),
			},
		}
	}
}

fn short(mode: Mode) -> io::Error {
	match mode {
		Mode::Fill => io::Error::new(
			io::ErrorKind::UnexpectedEof,
			"the source ended before the memory was full",
		),
		_ => io::Error::new(io::ErrorKind::WriteZero, "a call moved nothing"),
	}
}

pub(super) type Submit<S> =
	Box<dyn Fn(&Reactor, &Handle<S>) -> Result<Completion, Error> + Send + Sync>;

pub(super) struct Flow<'a, S> {
	handle: Handle<S>,
	reactor: Reactor,
	cancel: Option<Cancel>,
	kind: Kind,
	span: Span<'a>,
	done: usize,
	control: Control,
	call: Call,
}

impl<'a, S> Flow<'a, S> {
	pub(super) fn new(
		facade: &Facade<S>,
		kind: Kind,
		span: Span<'a>,
		control: Control,
	) -> Flow<'a, S> {
		Self {
			handle: facade.handle().clone(),
			reactor: facade.reactor().clone(),
			cancel: None,
			kind,
			span,
			done: 0,
			control,
			call: Call::idle(),
		}
	}

	pub(super) fn until(mut self, cancel: &Cancel) -> Flow<'a, S> {
		self.cancel = Some(cancel.clone());
		self
	}

	pub(super) fn done(&self) -> usize {
		self.done
	}

	pub(super) fn take_fds(&mut self) -> Vec<OwnedDescriptor> {
		match &mut self.control {
			Control::In(fds) => std::mem::take(fds),
			_ => Vec::new(),
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Flow<'_, S> {
	pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		loop {
			if matches!(self.call, Call::Idle) {
				if self.kind.mode() != Mode::Once && self.done >= self.span.len() {
					return Poll::Ready(Ok(()));
				}
				if self.cancel.as_ref().is_some_and(Cancel::is_cancelled) {
					return Poll::Ready(Err(Error::cancelled()));
				}
				match self.submit() {
					Ok(completion) => self.call.arm(completion, self.cancel.as_ref()),
					Err(error) => return Poll::Ready(Err(error)),
				}
			}
			match self.call.poll(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => return Poll::Ready(Err(Error::cancelled())),
				Poll::Ready(Some(result)) => match self.landed(result) {
					Ok(false) => return Poll::Ready(Ok(())),
					Ok(true) => continue,
					Err(error) => return Poll::Ready(Err(error)),
				},
			}
		}
	}

	fn submit(&mut self) -> Result<Completion, Error> {
		let span = self.span.low();
		let extents = span.extents().skip(self.done);
		let shift = self.done as u64;
		let space = span.space().clone();
		let reactor = &self.reactor;
		let handle = &self.handle;
		let op = match self.kind {
			Kind::Read { fdoff } | Kind::ReadAll { fdoff } => LowOp::Read {
				offset: fdoff + shift,
				space,
				extents,
			},
			Kind::Write { fdoff } | Kind::WriteAll { fdoff } => LowOp::Write {
				offset: fdoff + shift,
				space,
				extents,
			},
			Kind::Recv | Kind::RecvAll => match &mut self.control {
				Control::In(_) => LowOp::RecvWithFds { space, extents },
				_ => LowOp::Recv { space, extents },
			},
			Kind::Send | Kind::SendAll => match (&mut self.control, self.done) {
				(Control::Out(fds), 0) => LowOp::SendWithFds {
					space,
					extents,
					fds: std::mem::take(fds),
				},
				_ => LowOp::Send { space, extents },
			},
		};
		submit(reactor, handle, op)
	}

	fn landed(&mut self, result: Result<Response, Error>) -> Result<bool, Error> {
		let response = result?;
		let (count, fds) = match response {
			Response::CountWithFds(count, fds) => (count, fds),
			other => (other.count()?, Vec::new()),
		};
		if let Control::In(gathered) = &mut self.control {
			gathered.extend(fds);
		}
		if self.kind.mode() == Mode::Once {
			self.done = count;
			return Ok(false);
		}
		if count == 0 {
			return Err(Error::from(short(self.kind.mode())));
		}
		self.done += count;
		Ok(true)
	}
}

pub(super) struct Gate<S> {
	handle: Handle<S>,
	reactor: Reactor,
	cancel: Option<Cancel>,
	submit: Submit<S>,
	response: Option<Response>,
	call: Call,
}

impl<S> Gate<S> {
	pub(super) fn new(
		facade: &Facade<S>,
		submit: impl Fn(&Reactor, &Handle<S>) -> Result<Completion, Error> + Send + Sync + 'static,
	) -> Gate<S> {
		Self {
			handle: facade.handle().clone(),
			reactor: facade.reactor().clone(),
			cancel: None,
			submit: Box::new(submit),
			response: None,
			call: Call::idle(),
		}
	}

	pub(super) fn until(mut self, cancel: &Cancel) -> Gate<S> {
		self.cancel = Some(cancel.clone());
		self
	}

	pub(super) fn take_response(&mut self) -> Option<Response> {
		self.response.take()
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Gate<S> {
	pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		loop {
			if matches!(self.call, Call::Idle) {
				if self.cancel.as_ref().is_some_and(Cancel::is_cancelled) {
					return Poll::Ready(Err(Error::cancelled()));
				}
				match (self.submit)(&self.reactor, &self.handle) {
					Ok(completion) => self.call.arm(completion, self.cancel.as_ref()),
					Err(error) => return Poll::Ready(Err(error)),
				}
			}
			match self.call.poll(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => return Poll::Ready(Err(Error::cancelled())),
				Poll::Ready(Some(result)) => {
					self.response = Some(result?);
					return Poll::Ready(Ok(()));
				}
			}
		}
	}
}

/// A byte transfer whose [`Outcome`](crate::Outcome) retains partial progress on error or cancellation.
pub struct Transfer<'a, S> {
	flow: Flow<'a, S>,
	finished: bool,
}

impl<'a, S> Transfer<'a, S> {
	pub(super) fn new(facade: &Facade<S>, kind: Kind, span: Span<'a>) -> Transfer<'a, S> {
		Self {
			flow: Flow::new(facade, kind, span, Control::None),
			finished: false,
		}
	}

	pub(super) fn with_fds(
		facade: &Facade<S>,
		kind: Kind,
		span: Span<'a>,
		fds: Vec<OwnedDescriptor>,
	) -> Transfer<'a, S> {
		Self {
			flow: Flow::new(facade, kind, span, Control::Out(fds)),
			finished: false,
		}
	}

	/// Cancels the transfer while retaining bytes already transferred.
	pub fn until(self, cancel: &Cancel) -> Transfer<'a, S> {
		let Transfer { flow, .. } = self;
		Self {
			flow: flow.until(cancel),
			finished: false,
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Future for Transfer<'_, S> {
	type Output = Outcome<usize>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome<usize>> {
		let this = self.get_mut();
		assert!(!this.finished, "a transfer is not polled after it resolved");
		match this.flow.poll(cx) {
			Poll::Ready(result) => {
				this.finished = true;
				Poll::Ready(Outcome::new(this.flow.done(), result))
			}
			Poll::Pending => Poll::Pending,
		}
	}
}

/// Bytes received and descriptors passed in control messages.
#[derive(Debug)]
pub struct Received {
	pub bytes: usize,
	pub fds: Vec<OwnedDescriptor>,
}

impl Received {
	fn new(bytes: usize, fds: Vec<OwnedDescriptor>) -> Received {
		Self { bytes, fds }
	}
}

/// A receive that also takes the descriptors a control message carries.
pub struct Receive<'a, S> {
	flow: Flow<'a, S>,
	finished: bool,
}

impl<'a, S> Receive<'a, S> {
	pub(super) fn new(facade: &Facade<S>, kind: Kind, span: Span<'a>) -> Receive<'a, S> {
		Self {
			flow: Flow::new(facade, kind, span, Control::In(Vec::new())),
			finished: false,
		}
	}

	/// Cancels the receive while retaining bytes and descriptors already received.
	pub fn until(self, cancel: &Cancel) -> Receive<'a, S> {
		let Receive { flow, .. } = self;
		Self {
			flow: flow.until(cancel),
			finished: false,
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Future for Receive<'_, S> {
	type Output = Outcome<Received>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome<Received>> {
		let this = self.get_mut();
		assert!(!this.finished, "a receive is not polled after it resolved");
		match this.flow.poll(cx) {
			Poll::Ready(result) => {
				this.finished = true;
				let received = Received::new(this.flow.done(), this.flow.take_fds());
				Poll::Ready(Outcome::new(received, result))
			}
			Poll::Pending => Poll::Pending,
		}
	}
}

/// An operation with nothing to hand back: a wait for readiness, or a sync.
pub struct Op<S> {
	gate: Gate<S>,
	finished: bool,
}

impl<S> Op<S> {
	pub(super) fn new(
		facade: &Facade<S>,
		submit: impl Fn(&Reactor, &Handle<S>) -> Result<Completion, Error> + Send + Sync + 'static,
	) -> Op<S> {
		Self {
			gate: Gate::new(facade, submit),
			finished: false,
		}
	}

	/// Gives the operation up when `cancel` fires.
	pub fn until(self, cancel: &Cancel) -> Op<S> {
		let Op { gate, .. } = self;
		Self {
			gate: gate.until(cancel),
			finished: false,
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Future for Op<S> {
	type Output = Result<(), Error>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		let this = self.get_mut();
		assert!(
			!this.finished,
			"an operation is not polled after it resolved"
		);
		match this.gate.poll(cx) {
			Poll::Ready(result) => {
				this.finished = true;
				Poll::Ready(result)
			}
			Poll::Pending => Poll::Pending,
		}
	}
}

/// An accept: the connection the kernel made, in a facade of its own.
pub struct Accept<S> {
	gate: Gate<S>,
	finished: bool,
}

impl<S> Accept<S> {
	pub(super) fn new(
		facade: &Facade<S>,
		submit: impl Fn(&Reactor, &Handle<S>) -> Result<Completion, Error> + Send + Sync + 'static,
	) -> Accept<S> {
		Self {
			gate: Gate::new(facade, submit),
			finished: false,
		}
	}

	/// Gives the accept up when `cancel` fires. A connection the kernel made
	/// anyway goes with the future, and is closed with it.
	pub fn until(self, cancel: &Cancel) -> Accept<S> {
		let Accept { gate, .. } = self;
		Self {
			gate: gate.until(cancel),
			finished: false,
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
			Poll::Ready(Ok(())) => {
				this.finished = true;
				let response = this.gate.take_response();
				match response {
					Some(Response::Accepted(socket)) => Poll::Ready(Ok(Facade::from_parts(
						Handle::new(socket.into()),
						this.gate.reactor.clone(),
					))),
					other => unreachable!("an accept's round reports a socket, not {other:?}"),
				}
			}
		}
	}
}

/// A resource that sends and receives: one with message boundaries
/// ([`UdpSocket`](std::net::UdpSocket), `UnixDatagram`) or a byte stream.
macro_rules! socket {
	($($resource:ty),* $(,)?) => {
		$(
			impl Facade<$resource> {
				/// Receives into `span`, in one call: a short count is what arrived, not
				/// an error. [`recv_exact`](Facade::recv_exact) loops.
				pub fn recv<'a>(&self, span: Span<'a>) -> Transfer<'a, $resource> {
					Transfer::new(self, Kind::Recv, span)
				}

				/// Receives into `span`, taking the descriptors the control messages
				/// carry with the bytes.
				pub fn recv_with_fds<'a>(&self, span: Span<'a>) -> Receive<'a, $resource> {
					Receive::new(self, Kind::Recv, span)
				}

				/// Sends `span`, in one call: a short count is what the call took, not an
				/// error. [`send_all`](Facade::send_all) loops.
				pub fn send<'a>(&self, span: Span<'a>) -> Transfer<'a, $resource> {
					Transfer::new(self, Kind::Send, span)
				}

				/// Sends `span`, carrying `fds` with the bytes.
				///
				/// One call carries the descriptors; see
				/// [`send_all_with_fds`](Facade::send_all_with_fds) for a send that loops.
				pub fn send_with_fds<'a>(
					&self,
					span: Span<'a>,
					fds: Vec<OwnedDescriptor>,
				) -> Transfer<'a, $resource> {
					Transfer::with_fds(self, Kind::Send, span, fds)
				}

				/// Takes and clears the socket's pending error. Failure to read the error is returned as the outer `Err`.
				pub fn take_error(&self) -> io::Result<Option<io::Error>> {
					imp::take_error((**self.handle()).as_descriptor())
				}
			}
		)*
	};
}

socket!(std::net::UdpSocket, std::net::TcpStream,);

#[cfg(unix)]
socket!(
	std::os::unix::net::UnixDatagram,
	std::os::unix::net::UnixStream,
);

#[cfg(windows)]
socket!(completion_rt::OwnedSocket);

/// A byte-stream socket: the same sends and receives a datagram has, over as
/// many calls as it takes to move everything, plus `shutdown`.
macro_rules! stream {
	($($resource:ty),* $(,)?) => {
		$(
			impl Facade<$resource> {
				/// Receives into `span`, over as many calls as it takes to fill it.
				///
				/// [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) when the peer closes
				/// first.
				pub fn recv_exact<'a>(&self, span: Span<'a>) -> Transfer<'a, $resource> {
					Transfer::new(self, Kind::RecvAll, span)
				}

				/// [`recv_with_fds`](Facade::recv_with_fds) over as many calls as it takes
				/// to fill `span`, gathering the descriptors every call brings.
				pub fn recv_exact_with_fds<'a>(&self, span: Span<'a>) -> Receive<'a, $resource> {
					Receive::new(self, Kind::RecvAll, span)
				}

				/// Sends `span`, over as many calls as it takes.
				///
				/// [`WriteZero`](std::io::ErrorKind::WriteZero) when a call takes nothing.
				pub fn send_all<'a>(&self, span: Span<'a>) -> Transfer<'a, $resource> {
					Transfer::new(self, Kind::SendAll, span)
				}

				/// Sends all of `span`, attaching `fds` only to the first call's bytes.
				pub fn send_all_with_fds<'a>(
					&self,
					span: Span<'a>,
					fds: Vec<OwnedDescriptor>,
				) -> Transfer<'a, $resource> {
					Transfer::with_fds(self, Kind::SendAll, span, fds)
				}

				/// Shuts down one or both directions. `Write` sends EOF to the peer without closing the receive side.
				pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
					imp::shutdown((**self.handle()).as_descriptor(), how)
				}

				/// Resolves once the connection is gone both ways, or has failed.
				///
				/// A peer that shut only its write side has not hung up: the stream
				/// still takes sends. On TCP a peer's close arrives as that same
				/// half-close, so the hangup comes once this side shuts its write
				/// side too, or the connection resets. io_uring only; the other
				/// backends fail it with [`Unsupported`](std::io::ErrorKind::Unsupported).
				pub fn hangup(&self) -> Op<$resource> {
					Op::new(self, |reactor, handle| {
						submit(reactor, handle, LowOp::Hangup)
					})
				}
			}
		)*
	};
}

stream!(std::net::TcpStream);

#[cfg(unix)]
stream!(std::os::unix::net::UnixStream);

/// A listening socket whose accepted connections have a type of their own.
///
/// Implementing this is what gives a listener [`Facade::accept`]; the stream it
/// yields is the associated type, so an accepted connection is a facade over
/// the concrete socket type rather than one the caller narrows.
pub trait ListenerHandle: AsDescriptor + Send + Sync + 'static {
	type Stream: AsDescriptor + Send + Sync + 'static + From<OwnedSocket>;
}

impl ListenerHandle for std::net::TcpListener {
	type Stream = std::net::TcpStream;
}

#[cfg(unix)]
impl ListenerHandle for std::os::unix::net::UnixListener {
	type Stream = std::os::unix::net::UnixStream;
}

/// A listener takes the next connection in.
macro_rules! listener {
	($($resource:ty),* $(,)?) => {$(
		impl Facade<$resource> {
			/// Takes the next connection in: the stream it made, in a facade of its own.
			pub fn accept(&self) -> Accept<$resource> {
				Accept::new(self, |reactor, handle| {
					submit(reactor, handle, LowOp::Accept)
				})
			}
		}
	)*};
}

listener!(std::net::TcpListener);

#[cfg(unix)]
listener!(std::os::unix::net::UnixListener);

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
