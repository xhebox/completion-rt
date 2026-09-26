//! The loop behind a facade operation: one round with the kernel, and the next
//! over what is left of the memory, watched by a cancel when the caller gave
//! one.
//!
//! [`Flow`] is that loop, for every operation that counts bytes: [`Kind`] names
//! what a round submits, and [`Mode`] says whether a round follows it. [`Gate`]
//! is the sibling for an operation with nothing to count — an accept, a wait for
//! readiness, a sync — which lands a whole response in one round.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::net::ListenerHandle;
use crate::outcome::{Error, Outcome, Received};
use crate::{
	AsDescriptor, Cancel, Completion, Extent, Extents, Facade, Handle, Memory, OwnedDescriptor,
	Response, Submitter, Until,
};

/// What a round asks the kernel for.
#[derive(Clone, Copy)]
pub(super) enum Kind {
	/// A read of the memory at `fdoff`, in one call.
	Read { fdoff: u64 },
	/// A write of the memory at `fdoff`, in one call.
	Write { fdoff: u64 },
	/// A read that fills the whole memory.
	ReadAll { fdoff: u64 },
	/// A write that empties the whole memory.
	WriteAll { fdoff: u64 },
	/// One receive: a datagram socket takes one message per call.
	Recv,
	/// A receive that fills the whole memory.
	RecvAll,
	/// One send.
	Send,
	/// A send that empties the whole memory.
	SendAll,
}

impl Kind {
	/// How many rounds the kind takes, and what a round that moves nothing
	/// means.
	fn mode(self) -> Mode {
		match self {
			Kind::Read { .. } | Kind::Write { .. } | Kind::Recv | Kind::Send => Mode::Once,
			Kind::ReadAll { .. } | Kind::RecvAll => Mode::Fill,
			Kind::WriteAll { .. } | Kind::SendAll => Mode::Drain,
		}
	}
}

/// What the rounds after the first are for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
	/// No round follows: the one call's count is the result, short or not.
	Once,
	/// Rounds until the memory is full; a round that moves nothing is the end.
	Fill,
	/// Rounds until the memory is empty; a round that moves nothing is a stall.
	Drain,
}

/// What a control buffer carries around the bytes.
pub(super) enum Control {
	/// Nothing: the operation has no control buffer.
	None,
	/// Descriptors going out, with the first round's bytes.
	Out(Vec<OwnedDescriptor>),
	/// Descriptors coming in, gathered as the rounds land.
	In(Vec<OwnedDescriptor>),
}

/// One round with the kernel, and the caller's cancel when there is one.
enum Call {
	/// Nothing submitted: before the first round and after the last.
	Idle,
	/// A round that cannot be given up: polled until the kernel answers.
	Plain(Completion),
	/// A round watched by a cancel: `None` when the cancel ended it instead.
	Armed(Until<()>),
}

impl Call {
	fn idle() -> Call {
		Call::Idle
	}

	/// Hands `completion` to the round, watched by `cancel` if there is one.
	fn arm(&mut self, completion: Completion, cancel: Option<&Cancel>) {
		*self = match cancel {
			Some(cancel) => Call::Armed(completion.until(cancel)),
			None => Call::Plain(completion),
		};
	}

	/// The round's answer; `None` when the cancel ended it instead.
	fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Response>>> {
		match std::mem::replace(self, Call::Idle) {
			Call::Idle => unreachable!("a round is polled only with one in flight"),
			Call::Plain(mut completion) => match Pin::new(&mut completion).poll(cx) {
				Poll::Pending => {
					*self = Call::Plain(completion);
					Poll::Pending
				}
				Poll::Ready(((), result)) => Poll::Ready(Some(result)),
			},
			Call::Armed(mut until) => match Pin::new(&mut until).poll(cx) {
				Poll::Pending => {
					*self = Call::Armed(until);
					Poll::Pending
				}
				// A round that landed anyway — it won the race, or the backend
				// could not stop it — is the round's own result, counted like
				// any other; the cancel ending it is `None`.
				Poll::Ready(Ok(((), result))) => Poll::Ready(Some(result)),
				Poll::Ready(Err(_)) => Poll::Ready(None),
			},
		}
	}
}

/// The error a loop reports when a call moved nothing: a [`Mode::Fill`] loop has
/// run into the end of the source, a [`Mode::Drain`] one cannot make progress.
fn short(mode: Mode) -> io::Error {
	match mode {
		Mode::Fill => io::Error::new(
			io::ErrorKind::UnexpectedEof,
			"the source ended before the memory was full",
		),
		_ => io::Error::new(io::ErrorKind::WriteZero, "a call moved nothing"),
	}
}

/// The whole of `memory`, as the one extent a whole-buffer transfer names.
pub(super) fn whole(memory: &dyn Memory) -> Extents {
	Extents::from_slice(&[Extent {
		offset: 0,
		len: memory.len(),
	}])
}

/// A transfer's loop: rounds over what is left of the memory.
pub(super) struct Flow<S> {
	handle: Handle<S>,
	submitter: Submitter,
	cancel: Option<Cancel>,
	kind: Kind,
	memory: Arc<dyn Memory>,
	extents: Extents,
	/// Bytes moved so far; a single round's count is the whole of it.
	done: usize,
	control: Control,
	call: Call,
}

impl<S> Flow<S> {
	pub(super) fn new(
		facade: &Facade<S>,
		kind: Kind,
		memory: Arc<dyn Memory>,
		extents: Extents,
		control: Control,
	) -> Flow<S> {
		Self {
			handle: facade.handle().clone(),
			submitter: facade.submitter().clone(),
			cancel: None,
			kind,
			memory,
			extents,
			done: 0,
			control,
			call: Call::idle(),
		}
	}

	/// Watches `cancel` across the whole loop, not one round of it.
	pub(super) fn until(mut self, cancel: &Cancel) -> Flow<S> {
		self.cancel = Some(cancel.clone());
		self
	}

	/// The bytes the loop has moved.
	pub(super) fn done(&self) -> usize {
		self.done
	}

	/// The descriptors the loop has gathered.
	pub(super) fn take_fds(&mut self) -> Vec<OwnedDescriptor> {
		match &mut self.control {
			Control::In(fds) => std::mem::take(fds),
			_ => Vec::new(),
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Flow<S> {
	pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		loop {
			if matches!(self.call, Call::Idle) {
				if self.kind.mode() != Mode::Once && self.done >= self.extents.len_bytes() {
					return Poll::Ready(Ok(()));
				}
				if self.cancel.as_ref().is_some_and(Cancel::is_cancelled) {
					return Poll::Ready(Err(Error::Cancelled));
				}
				match self.submit() {
					Ok(completion) => self.call.arm(completion, self.cancel.as_ref()),
					Err(error) => return Poll::Ready(Err(Error::Io(error))),
				}
			}
			match self.call.poll(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => return Poll::Ready(Err(Error::Cancelled)),
				Poll::Ready(Some(result)) => match self.landed(result) {
					Ok(false) => return Poll::Ready(Ok(())),
					Ok(true) => continue,
					Err(error) => return Poll::Ready(Err(error)),
				},
			}
		}
	}

	/// Hands the next round over: the memory from `done` on, at the file
	/// position `done` past where the first round starts.
	fn submit(&mut self) -> io::Result<Completion> {
		let extents = self.extents.skip(self.done);
		let shift = self.done as u64;
		let memory = Arc::clone(&self.memory);
		let submitter = &self.submitter;
		let handle = &self.handle;
		match self.kind {
			Kind::Read { fdoff } | Kind::ReadAll { fdoff } => {
				submitter.read(handle, fdoff + shift, memory, extents)
			}
			Kind::Write { fdoff } | Kind::WriteAll { fdoff } => {
				submitter.write(handle, fdoff + shift, memory, extents)
			}
			Kind::Recv | Kind::RecvAll => match &mut self.control {
				Control::In(_) => submitter.recv_with_fds(handle, memory, extents),
				_ => submitter.recv(handle, memory, extents),
			},
			Kind::Send | Kind::SendAll => match (&mut self.control, self.done) {
				// The descriptors travel with the first round's bytes, the only
				// round that carries them.
				(Control::Out(fds), 0) => {
					submitter.send_with_fds(handle, memory, extents, std::mem::take(fds))
				}
				_ => submitter.send(handle, memory, extents),
			},
		}
	}

	/// Reads a round's response into the progress — the bytes and the
	/// descriptors it carried — and says whether another round follows.
	fn landed(&mut self, result: io::Result<Response>) -> Result<bool, Error> {
		let response = result.map_err(Error::Io)?;
		let (count, fds) = match response {
			Response::CountWithFds(count, fds) => (count, fds),
			other => (other.count().map_err(Error::Io)?, Vec::new()),
		};
		// Only a receive's control buffer takes descriptors in; a send's is
		// empty by the time its response lands.
		if let Control::In(gathered) = &mut self.control {
			gathered.extend(fds);
		}
		if self.kind.mode() == Mode::Once {
			self.done = count;
			return Ok(false);
		}
		if count == 0 {
			return Err(Error::Io(short(self.kind.mode())));
		}
		self.done += count;
		Ok(true)
	}
}

/// One operation that lands a whole response: nothing to count, and no round
/// to repeat.
pub(super) struct Gate<S> {
	handle: Handle<S>,
	submitter: Submitter,
	cancel: Option<Cancel>,
	/// What the one round submits, taken from [`Submitter`] by name.
	submit: fn(&Submitter, &Handle<S>) -> io::Result<Completion>,
	response: Option<Response>,
	call: Call,
}

impl<S> Gate<S> {
	pub(super) fn new(
		facade: &Facade<S>,
		submit: fn(&Submitter, &Handle<S>) -> io::Result<Completion>,
	) -> Gate<S> {
		Self {
			handle: facade.handle().clone(),
			submitter: facade.submitter().clone(),
			cancel: None,
			submit,
			response: None,
			call: Call::idle(),
		}
	}

	/// Watches `cancel` over the one round.
	pub(super) fn until(mut self, cancel: &Cancel) -> Gate<S> {
		self.cancel = Some(cancel.clone());
		self
	}

	/// The response the round landed. Taking it is what hands a socket an
	/// accept produced to its caller; left behind, it closes it.
	pub(super) fn take_response(&mut self) -> Option<Response> {
		self.response.take()
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Gate<S> {
	pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		loop {
			if matches!(self.call, Call::Idle) {
				if self.cancel.as_ref().is_some_and(Cancel::is_cancelled) {
					return Poll::Ready(Err(Error::Cancelled));
				}
				match (self.submit)(&self.submitter, &self.handle) {
					Ok(completion) => self.call.arm(completion, self.cancel.as_ref()),
					Err(error) => return Poll::Ready(Err(Error::Io(error))),
				}
			}
			match self.call.poll(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => return Poll::Ready(Err(Error::Cancelled)),
				Poll::Ready(Some(result)) => {
					self.response = Some(result.map_err(Error::Io)?);
					return Poll::Ready(Ok(()));
				}
			}
		}
	}
}

/// A byte transfer: a read, a write, a receive, or a send.
///
/// Awaiting it gives the bytes it moved and whether it finished. A transfer
/// that stops short — a call that failed, or a cancel — still reports what it
/// had moved by then.
pub struct Transfer<S> {
	flow: Flow<S>,
	finished: bool,
}

impl<S> Transfer<S> {
	pub(super) fn new(
		facade: &Facade<S>,
		kind: Kind,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> Transfer<S> {
		Self {
			flow: Flow::new(facade, kind, memory, extents, Control::None),
			finished: false,
		}
	}

	/// A send whose control buffer carries `fds` with the first round's bytes.
	pub(super) fn with_fds(
		facade: &Facade<S>,
		kind: Kind,
		memory: Arc<dyn Memory>,
		extents: Extents,
		fds: Vec<OwnedDescriptor>,
	) -> Transfer<S> {
		Self {
			flow: Flow::new(facade, kind, memory, extents, Control::Out(fds)),
			finished: false,
		}
	}

	/// Gives the transfer up when `cancel` fires: a cancel that lands mid-loop
	/// ends it there, and the value it lands is the progress it had made.
	pub fn until(self, cancel: &Cancel) -> Transfer<S> {
		let Transfer { flow, .. } = self;
		Self {
			flow: flow.until(cancel),
			finished: false,
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Future for Transfer<S> {
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

/// A receive that also takes the descriptors a control message carries.
pub struct Receive<S> {
	flow: Flow<S>,
	finished: bool,
}

impl<S> Receive<S> {
	pub(super) fn new(
		facade: &Facade<S>,
		kind: Kind,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> Receive<S> {
		Self {
			flow: Flow::new(facade, kind, memory, extents, Control::In(Vec::new())),
			finished: false,
		}
	}

	/// Gives the receive up when `cancel` fires; a cancel that lands mid-loop
	/// keeps the descriptors that had arrived.
	pub fn until(self, cancel: &Cancel) -> Receive<S> {
		let Receive { flow, .. } = self;
		Self {
			flow: flow.until(cancel),
			finished: false,
		}
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Future for Receive<S> {
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
		submit: fn(&Submitter, &Handle<S>) -> io::Result<Completion>,
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
		submit: fn(&Submitter, &Handle<S>) -> io::Result<Completion>,
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
					Some(Response::Accepted(socket)) => {
						Poll::Ready(Ok(Facade::new(socket, &this.gate.submitter)))
					}
					other => unreachable!("an accept's round reports a socket, not {other:?}"),
				}
			}
		}
	}
}
