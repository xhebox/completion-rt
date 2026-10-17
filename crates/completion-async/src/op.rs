//! Resource wrapper ([`Facade`]), operation futures ([`Transfer`], [`Receive`],
//! [`Op`]), and result types ([`Outcome`], [`Received`]).

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use completion_backend::{
	AsDescriptor, Error, Op as LowOp, OwnedDescriptor, Response, Span, Status,
};

use crate::Executor;
use crate::completion::{Cancel, Completion, submit};
use crate::global;
use crate::reactor::Reactor;

/// Shared handle to an I/O resource bound to an [`Executor`](crate::Executor).
///
/// Cloning a `Facade` is cheap (`Arc` clone) and refers to the same underlying
/// resource and reactor. Available I/O methods depend on the resource type `S`:
///
/// - [`std::fs::File`] (`File`): positional reads/writes (`read_at`, `read_exact_at`,
///   `write_at`, `write_all_at`) and file maintenance (`sync`, `flush`, `allocate`, `punch`).
/// - Pipes, stdio, and raw owned descriptors: positional reads/writes.
/// - [`SocketHandle`](crate::SocketHandle) / [`StreamHandle`](crate::StreamHandle):
///   `recv`, `send`, fd-passing variants, `shutdown`, and `hangup`.
/// - [`ListenerHandle`](crate::ListenerHandle): `accept`.
/// - [`Signal`](crate::Signal): `wait_readable` and `wait_writable`.
///
/// Unsupported operations are rejected at compile time:
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn read(socket: &Facade<std::net::TcpStream>) {
/// 	let _ = socket.read_at(vec![0u8; 4], 0);
/// }
/// ```
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn sync(socket: &Facade<std::net::TcpStream>) {
/// 	let _ = socket.sync();
/// }
/// ```
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn sync(pipe: &Facade<std::io::PipeWriter>) {
/// 	let _ = pipe.sync();
/// }
/// ```
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn wait(socket: &Facade<std::net::TcpStream>) {
/// 	let _ = socket.wait_readable();
/// }
/// ```
pub struct Facade<S> {
	resource: Arc<S>,
	reactor: Reactor,
}

impl<S> Facade<S> {
	/// Wraps `resource` and binds it to the current thread's default [`Executor`].
	///
	/// See [`Facade::with_executor`] for platform-specific resource requirements.
	#[fastrace::trace]
	pub fn new(resource: impl Into<S>) -> Facade<S> {
		Self::from_parts(Arc::new(resource.into()), global::reactor())
	}

	/// Wraps `resource` and binds it to `executor`.
	///
	/// # Platform notes
	/// - **Windows**: Files must be opened with `FILE_FLAG_OVERLAPPED` (use
	///   [`OpenOptions`](crate::OpenOptions)), or IOCP submissions will fail with
	///   invalid-parameter errors.
	/// - **Portable Unix** (non-Linux): Non-file descriptors (sockets, pipes, etc.)
	///   must be configured in non-blocking mode.
	#[fastrace::trace]
	pub fn with_executor(resource: impl Into<S>, executor: &Executor) -> Facade<S> {
		Self::from_parts(Arc::new(resource.into()), executor.reactor().clone())
	}

	pub(crate) fn from_parts(resource: Arc<S>, reactor: Reactor) -> Facade<S> {
		Self { resource, reactor }
	}

	pub(crate) fn reactor(&self) -> &Reactor {
		&self.reactor
	}

	pub(crate) fn resource(&self) -> &Arc<S> {
		&self.resource
	}

	/// Unwraps the inner resource if this `Facade` is the only reference to it.
	///
	/// Returns `Err(self)` if another `Facade` clone or an in-flight operation
	/// still holds a reference.
	pub fn try_take(self) -> Result<S, Facade<S>> {
		let Facade { resource, reactor } = self;
		match Arc::try_unwrap(resource) {
			Ok(resource) => Ok(resource),
			Err(resource) => Err(Facade { resource, reactor }),
		}
	}
}

impl<S: AsDescriptor + 'static> Facade<S> {
	/// The resource behind the facade, as the operation names it: a clone the
	/// operation gives back when its entry goes.
	pub(crate) fn fd(&self) -> Arc<dyn AsDescriptor> {
		self.resource.clone()
	}
}

impl<S> Clone for Facade<S> {
	fn clone(&self) -> Facade<S> {
		Self {
			resource: self.resource.clone(),
			reactor: self.reactor.clone(),
		}
	}
}

/// Result of a transfer operation, preserving any partial progress (`value`)
/// alongside the completion status (`result`).
#[derive(Debug)]
pub struct Outcome<T> {
	/// Progress accumulated before the operation finished, failed, or was cancelled
	/// (e.g. number of bytes transferred or [`Received`]).
	pub value: T,
	/// `Ok(())` if the operation completed in full, or `Err` if it stopped early.
	pub result: Result<(), Error>,
}

impl<T> Outcome<T> {
	pub(crate) fn new(value: T, result: Result<(), Error>) -> Outcome<T> {
		Self { value, result }
	}

	/// Converts the outcome into a standard `Result`, returning `self.value` on
	/// `Ok` and discarding partial progress on `Err`.
	pub fn into_result(self) -> Result<T, Error> {
		self.result.map(|()| self.value)
	}
}

#[derive(Clone, Copy)]
pub(crate) enum Kind {
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

enum Control {
	None,
	Out(Vec<OwnedDescriptor>),
	In(Vec<OwnedDescriptor>),
}

/// The one-round operations: each names its low layer's arm.
#[derive(Clone, Copy)]
pub(crate) enum Action {
	Fsync,
	Flush,
	Allocate { offset: u64, len: u64 },
	Punch { offset: u64, len: u64 },
	Accept,
	Readable,
	Writable,
	Hangup,
}

impl Action {
	fn into_op(self) -> LowOp {
		match self {
			Action::Fsync => LowOp::Fsync,
			Action::Flush => LowOp::Flush,
			Action::Allocate { offset, len } => LowOp::Allocate { offset, len },
			Action::Punch { offset, len } => LowOp::Punch { offset, len },
			Action::Accept => LowOp::Accept,
			Action::Readable => LowOp::Readable,
			Action::Writable => LowOp::Writable,
			Action::Hangup => LowOp::Hangup,
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

/// A transfer's rounds: each submits one call and adds up what it moved.
struct Flow {
	fd: Arc<dyn AsDescriptor>,
	reactor: Reactor,
	cancel: Option<Cancel>,
	kind: Kind,
	span: Span,
	done: usize,
	control: Control,
	call: Option<Completion>,
}

impl Flow {
	fn new(
		facade: &Facade<impl AsDescriptor + 'static>,
		kind: Kind,
		span: Span,
		control: Control,
	) -> Flow {
		Self {
			fd: facade.fd(),
			reactor: facade.reactor().clone(),
			cancel: None,
			kind,
			span,
			done: 0,
			control,
			call: None,
		}
	}

	fn until(mut self, cancel: &Cancel) -> Flow {
		self.cancel = Some(cancel.clone());
		self
	}

	fn done(&self) -> usize {
		self.done
	}

	fn take_fds(&mut self) -> Vec<OwnedDescriptor> {
		match &mut self.control {
			Control::In(fds) => std::mem::take(fds),
			_ => Vec::new(),
		}
	}

	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		loop {
			if self.call.is_none() {
				if self.kind.mode() != Mode::Once && self.done >= self.span.len() {
					return Poll::Ready(Ok(()));
				}
				if self.cancel.as_ref().is_some_and(Cancel::is_cancelled) {
					return Poll::Ready(Err(Error::cancelled()));
				}
				match self.round() {
					Ok(completion) => self.call = Some(completion),
					Err(error) => return Poll::Ready(Err(error)),
				}
			}
			let Some(call) = &mut self.call else {
				unreachable!("a round is polled only with one in flight");
			};
			let landed = call.poll(cx);
			match landed {
				Poll::Pending => return Poll::Pending,
				// The cancel won: the kernel has let go of the span, which is
				// what the transfer reports now that it is free of it.
				Poll::Ready(None) => {
					self.call = None;
					return Poll::Ready(Err(Error::cancelled()));
				}
				Poll::Ready(Some(result)) => {
					self.call = None;
					match self.landed(result) {
						Ok(false) => return Poll::Ready(Ok(())),
						Ok(true) => continue,
						Err(error) => return Poll::Ready(Err(error)),
					}
				}
			}
		}
	}

	fn round(&mut self) -> Result<Completion, Error> {
		let span = self.span.clone().skip(self.done);
		let shift = self.done as u64;
		let op = match self.kind {
			Kind::Read { fdoff } | Kind::ReadAll { fdoff } => LowOp::Read {
				offset: fdoff + shift,
				span,
			},
			Kind::Write { fdoff } | Kind::WriteAll { fdoff } => LowOp::Write {
				offset: fdoff + shift,
				span,
			},
			Kind::Recv | Kind::RecvAll => match &mut self.control {
				Control::In(_) => LowOp::RecvWithFds { span },
				_ => LowOp::Recv { span },
			},
			Kind::Send | Kind::SendAll => {
				let fds = match (&mut self.control, self.done) {
					(Control::Out(fds), 0) => std::mem::take(fds),
					_ => Vec::new(),
				};
				LowOp::Send { span, fds }
			}
		};
		submit(
			&self.reactor,
			Arc::clone(&self.fd),
			op,
			self.cancel.as_ref(),
		)
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

/// One round of an operation with nothing to add up: it either lands a
/// response or fails.
pub(crate) struct Gate {
	fd: Arc<dyn AsDescriptor>,
	reactor: Reactor,
	cancel: Option<Cancel>,
	action: Action,
	call: Option<Completion>,
}

impl Gate {
	pub(crate) fn new<S: AsDescriptor + 'static>(facade: &Facade<S>, action: Action) -> Gate {
		Self {
			fd: facade.fd(),
			reactor: facade.reactor().clone(),
			cancel: None,
			action,
			call: None,
		}
	}

	pub(crate) fn until(mut self, cancel: &Cancel) -> Gate {
		self.cancel = Some(cancel.clone());
		self
	}

	pub(crate) fn reactor(&self) -> &Reactor {
		&self.reactor
	}

	pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Response, Error>> {
		loop {
			if self.call.is_none() {
				if self.cancel.as_ref().is_some_and(Cancel::is_cancelled) {
					return Poll::Ready(Err(Error::cancelled()));
				}
				match submit(
					&self.reactor,
					Arc::clone(&self.fd),
					self.action.into_op(),
					self.cancel.as_ref(),
				) {
					Ok(completion) => self.call = Some(completion),
					Err(error) => return Poll::Ready(Err(error)),
				}
			}
			let Some(call) = &mut self.call else {
				unreachable!("a round is polled only with one in flight");
			};
			let landed = call.poll(cx);
			match landed {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => {
					self.call = None;
					return Poll::Ready(Err(Error::cancelled()));
				}
				Poll::Ready(Some(Ok(response))) => {
					self.call = None;
					return Poll::Ready(Ok(response));
				}
				// The wait is owed: the mechanism failed it in a way that may
				// succeed again — a hangup poll the kernel ended rather than
				// report a hangup on, an interrupted call — so it is armed once
				// more instead of failing a caller that asked to wait. A cancel
				// raised meanwhile is seen at the top of the loop.
				Poll::Ready(Some(Err(error))) if error.status() == Status::Temporary => {
					self.call = None;
					continue;
				}
				Poll::Ready(Some(Err(error))) => {
					self.call = None;
					return Poll::Ready(Err(error));
				}
			}
		}
	}
}

/// Future for a byte transfer operation, resolving to `Outcome<usize>` with the
/// number of bytes transferred.
pub struct Transfer {
	flow: Flow,
	finished: bool,
}

impl Transfer {
	pub(crate) fn new<S: AsDescriptor + 'static>(
		facade: &Facade<S>,
		kind: Kind,
		span: Span,
	) -> Transfer {
		Self {
			flow: Flow::new(facade, kind, span, Control::None),
			finished: false,
		}
	}

	pub(crate) fn with_fds<S: AsDescriptor + 'static>(
		facade: &Facade<S>,
		kind: Kind,
		span: Span,
		fds: Vec<OwnedDescriptor>,
	) -> Transfer {
		Self {
			flow: Flow::new(facade, kind, span, Control::Out(fds)),
			finished: false,
		}
	}

	/// Attaches a [`Cancel`] token to abort the transfer while preserving the count
	/// of bytes transferred before cancellation in [`Outcome::value`].
	pub fn until(self, cancel: &Cancel) -> Transfer {
		let Transfer { flow, .. } = self;
		Self {
			flow: flow.until(cancel),
			finished: false,
		}
	}
}

impl Future for Transfer {
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

/// Payload of [`Receive`], containing both the byte count and any ancillary
/// descriptors received.
#[derive(Debug)]
pub struct Received {
	/// Number of payload bytes received.
	pub bytes: usize,
	/// Descriptors received via ancillary control messages (`SCM_RIGHTS`).
	pub fds: Vec<OwnedDescriptor>,
}

impl Received {
	fn new(bytes: usize, fds: Vec<OwnedDescriptor>) -> Received {
		Self { bytes, fds }
	}
}

/// Future for a socket receive operation that also accepts ancillary file
/// descriptors, resolving to `Outcome<Received>`.
pub struct Receive {
	flow: Flow,
	finished: bool,
}

impl Receive {
	pub(crate) fn new<S: AsDescriptor + 'static>(
		facade: &Facade<S>,
		kind: Kind,
		span: Span,
	) -> Receive {
		Self {
			flow: Flow::new(facade, kind, span, Control::In(Vec::new())),
			finished: false,
		}
	}

	/// Attaches a [`Cancel`] token to abort the receive while preserving any bytes
	/// and descriptors received before cancellation in [`Outcome::value`].
	pub fn until(self, cancel: &Cancel) -> Receive {
		let Receive { flow, .. } = self;
		Self {
			flow: flow.until(cancel),
			finished: false,
		}
	}
}

impl Future for Receive {
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

/// Future for a non-transfer operation (such as `sync`, `flush`, `allocate`,
/// `punch`, `hangup`, or readiness waits on [`Signal`](crate::Signal)).
pub struct Op {
	gate: Gate,
	finished: bool,
}

impl Op {
	pub(crate) fn new<S: AsDescriptor + 'static>(facade: &Facade<S>, action: Action) -> Op {
		Self {
			gate: Gate::new(facade, action),
			finished: false,
		}
	}

	/// Cancels the operation when `cancel` is triggered.
	pub fn until(self, cancel: &Cancel) -> Op {
		let Op { gate, .. } = self;
		Self {
			gate: gate.until(cancel),
			finished: false,
		}
	}
}

impl Future for Op {
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
				Poll::Ready(result.map(|_| ()))
			}
			Poll::Pending => Poll::Pending,
		}
	}
}
