//! The facade, the results its operations yield, and the futures driving them.
//!
//! [`Flow`] is a transfer's round-by-round state: each round submits one
//! [`LowOp`] and adds up what it moved. [`Gate`] is the one-round form, for an
//! operation with nothing to add up.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use completion_rt::{AsDescriptor, Error, Op as LowOp, OwnedDescriptor, Response, Span, Status};

use crate::Executor;
use crate::completion::{Cancel, Completion, submit};
use crate::global;
use crate::reactor::Reactor;

/// An owned resource with async operations submitted through its
/// [`Executor`](crate::Executor).
///
/// Which operations a facade has is its resource type, named where the
/// operations are: a `File` is read at an offset and synced, a `TcpStream` is
/// received and sent on. A socket is not read at an offset or synced:
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
/// Nor is a pipe synced:
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn sync(pipe: &Facade<std::io::PipeWriter>) {
/// 	let _ = pipe.sync();
/// }
/// ```
///
/// Nor does a resource with a transfer have a readiness wait: the backend arms
/// the readiness itself when a call would block. Only [`Signal`](crate::Signal)
/// waits.
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
	/// Wraps `resource`, its operations going to this thread's global
	/// executor.
	///
	/// See [`Facade::with_executor`] for the resource's requirements.
	#[fastrace::trace]
	pub fn new(resource: impl Into<S>) -> Facade<S> {
		Self::from_parts(Arc::new(resource.into()), global::reactor())
	}

	/// Wraps `resource`, its operations going to `executor`.
	///
	/// On Windows the resource must be opened for overlapped I/O: a file
	/// through [`OpenOptions`](crate::OpenOptions), which asks for
	/// `FILE_FLAG_OVERLAPPED`. A handle opened without that flag cannot be owned
	/// by a completion port, so its operations fail with the invalid-parameter
	/// error the association gives.
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

	/// Gives the resource back if nothing else names it: `Err(self)` while
	/// another facade clone or an operation still holds it.
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

/// A value and whether the operation completed; partial progress is retained on error or cancellation.
#[derive(Debug)]
pub struct Outcome<T> {
	pub value: T,
	pub result: Result<(), Error>,
}

impl<T> Outcome<T> {
	pub(crate) fn new(value: T, result: Result<(), Error>) -> Outcome<T> {
		Self { value, result }
	}

	/// Discards partial progress on failure.
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

/// A byte transfer whose [`Outcome`] retains partial progress on error or cancellation.
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

	/// Cancels the transfer while retaining bytes already transferred.
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

	/// Cancels the receive while retaining bytes and descriptors already received.
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

/// An operation with nothing to hand back: a wait for readiness, or a sync.
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

	/// Gives the operation up when `cancel` fires.
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
