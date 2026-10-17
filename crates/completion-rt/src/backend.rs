//! The completion mechanism of the target, and the operations it runs.

use std::io;
use std::task::Waker;
use std::time::Duration;

use crate::desc::{BorrowedDescriptor, OwnedDescriptor, OwnedSocket, RawHandle};
use crate::error::Error;
use crate::memory::{IoVecs, Space, Span};

#[cfg(not(any(unix, windows)))]
compile_error!("the completion crate has no backend for this target");

#[cfg(windows)]
mod iocp;
#[cfg(all(unix, not(target_os = "linux")))]
mod portable;
#[cfg(target_os = "linux")]
mod uring;

/// Configure the backend.
#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub entries: u32,
	/// Completions appear only while polling (`io_uring` IOPOLL).
	pub iopoll: bool,
	/// A kernel thread consumes submissions (`io_uring` SQPOLL).
	pub sqpoll: bool,
}

impl Default for Config {
	fn default() -> Self {
		Config {
			entries: 256,
			iopoll: false,
			sqpoll: false,
		}
	}
}

/// Completed operation result.
#[derive(Debug)]
pub enum Response {
	/// The bytes a transfer moved.
	Count(usize),
	/// A receive that also brought descriptors.
	CountWithFds(usize, Vec<OwnedDescriptor>),
	/// The socket an accept produced; a response that is never taken closes it.
	Accepted(OwnedSocket),
	/// A sync or a wait: the completion itself is all of it.
	Done,
}

impl Response {
	/// Returns the transferred byte count, closing any received descriptors; other response variants return an error.
	pub fn count(self) -> Result<usize, Error> {
		match self {
			Response::Count(count) | Response::CountWithFds(count, _) => Ok(count),
			Response::Accepted(_) => Err(Error::from(io::Error::other(
				"an accept reported a socket, not a count",
			))),
			Response::Done => Err(Error::from(io::Error::other(
				"a transfer reported no count",
			))),
		}
	}
}

/// An operation named by its arguments, without the descriptor it runs on.
///
/// The default `B` is the [`Span`] a submission passes in: a transfer names its
/// memory as a span of a [`Space`], resolved against it before the backend is
/// handed anything. A backend then runs the same operation over `IoVecs`, the
/// addresses the span resolved to. The descriptor is the submission's own and
/// never part of the operation.
pub enum Op<B = Span> {
	Read {
		offset: u64,
		span: B,
	},
	Write {
		offset: u64,
		span: B,
	},
	Fsync,
	Flush,
	Allocate {
		offset: u64,
		len: u64,
	},
	Punch {
		offset: u64,
		len: u64,
	},
	Recv {
		span: B,
	},
	RecvWithFds {
		span: B,
	},
	Send {
		span: B,
		fds: Vec<OwnedDescriptor>,
	},
	Accept,
	Readable,
	Writable,
	/// Resolves once the peer is gone or the socket has failed; a peer that
	/// only shut its write side has not hung up.
	Hangup,
}

impl Op<Span> {
	/// The caller memory the operation reads or writes, if any. Control
	/// buffers are the backend's own, so a descriptor-carrying send with no
	/// payload bytes names none.
	pub fn space(&self) -> Option<Space> {
		match self {
			Op::Read { span, .. }
			| Op::Write { span, .. }
			| Op::Recv { span, .. }
			| Op::RecvWithFds { span, .. }
			| Op::Send { span, .. } => (!span.is_empty()).then(|| span.space().clone()),
			Op::Fsync
			| Op::Flush
			| Op::Allocate { .. }
			| Op::Punch { .. }
			| Op::Accept
			| Op::Readable
			| Op::Writable
			| Op::Hangup => None,
		}
	}

	/// The operation with every span resolved against its space: the form the
	/// backend runs.
	pub(crate) fn resolve(self) -> io::Result<Op<IoVecs>> {
		Ok(match self {
			Op::Read { offset, span } => Op::Read {
				offset,
				span: span.resolve()?,
			},
			Op::Write { offset, span } => Op::Write {
				offset,
				span: span.resolve()?,
			},
			Op::Fsync => Op::Fsync,
			Op::Flush => Op::Flush,
			Op::Allocate { offset, len } => Op::Allocate { offset, len },
			Op::Punch { offset, len } => Op::Punch { offset, len },
			Op::Recv { span } => Op::Recv {
				span: span.resolve()?,
			},
			Op::RecvWithFds { span } => Op::RecvWithFds {
				span: span.resolve()?,
			},
			Op::Send { span, fds } => Op::Send {
				span: span.resolve()?,
				fds,
			},
			Op::Accept => Op::Accept,
			Op::Readable => Op::Readable,
			Op::Writable => Op::Writable,
			Op::Hangup => Op::Hangup,
		})
	}
}

/// What a mechanism has to provide for [`Backend`] to run operations on it.
///
/// The id an operation is known by is the caller's: it mints one when it files
/// the operation away, and the backend carries that id through its own
/// mechanism — an `io_uring` `user_data`, a completion port's `OVERLAPPED` — so
/// [`poll`](Mechanism::poll) reports the same id the submission was given. The
/// backend knows nothing about what the caller keeps under that id.
///
/// This is the interface between [`Backend`] and one mechanism per target, so
/// it is closed: [`Op`] and the ids are this crate's own types, and a backend
/// outside the crate has nothing to implement it with.
pub(crate) trait Mechanism {
	/// Submits `op` on `fd` under `id`, or returns the failure that kept it out
	/// of the mechanism.
	///
	/// A refusal is the submission's own error: the mechanism never held the
	/// operation, so no completion will report it.
	fn submit(&mut self, id: u64, fd: RawHandle, op: Op<IoVecs>) -> Result<(), Error>;

	/// Asks the operation `id` to give up, without waiting for it: its
	/// completion still comes back through [`poll`](Mechanism::poll).
	fn cancel(&mut self, id: u64);

	/// Collects everything the mechanism has completed into `completed`.
	///
	/// One call reports all the completions the mechanism has in hand, not one
	/// of them: `timeout` of zero means "do not wait for any".
	fn poll(
		&mut self,
		timeout: Option<Duration>,
		completed: &mut Vec<(u64, Result<Response, Error>)>,
	) -> Result<(), Error>;

	/// A [`Waker`] that wakes this backend out of a blocking [`poll`](Mechanism::poll).
	fn waker(&self) -> Waker;
}

#[cfg(windows)]
type BackendImpl = iocp::Iocp;
#[cfg(all(unix, not(target_os = "linux")))]
type BackendImpl = portable::Portable;
#[cfg(target_os = "linux")]
type BackendImpl = uring::Uring;

/// The OS completion backend: submits operations, cancels them, and reports
/// their completions.
///
/// It is a handle on one mechanism per target and nothing more. The ids are the
/// caller's own, carried through the mechanism and handed back with each
/// completion; what a completion means, what memory and descriptors an
/// operation may name, and when to wait are all the caller's to decide.
pub struct Backend(BackendImpl);

impl Backend {
	/// Opens the target's completion mechanism.
	pub fn new(config: &Config) -> Result<Self, Error> {
		Ok(Backend(BackendImpl::new(config)?))
	}

	/// Submits `op` on `fd` under `id`, or returns the failure that kept it out
	/// of the mechanism.
	///
	/// The descriptor is borrowed for the call only; `id` names the operation
	/// from here, and its completion reports the same id. A transfer's extents
	/// are resolved here: one outside its space fails with
	/// [`InvalidInput`](std::io::ErrorKind::InvalidInput).
	pub fn submit(&mut self, id: u64, fd: BorrowedDescriptor<'_>, op: Op) -> Result<(), Error> {
		let op = op.resolve()?;
		let raw = RawHandle::from_descriptor(fd);
		self.0.submit(id, raw, op)
	}

	/// Asks the operation `id` to give up, without waiting for it: its
	/// completion still comes back through [`poll`](Backend::poll).
	pub fn cancel(&mut self, id: u64) {
		self.0.cancel(id)
	}

	/// Collects everything the mechanism has completed into `completed`.
	///
	/// One call reports all the completions the mechanism has in hand, not one
	/// of them: `timeout` of zero means "do not wait for any".
	pub fn poll(
		&mut self,
		timeout: Option<Duration>,
		completed: &mut Vec<(u64, Result<Response, Error>)>,
	) -> Result<(), Error> {
		self.0.poll(timeout, completed)
	}

	/// A [`Waker`] that wakes this backend out of a blocking
	/// [`poll`](Backend::poll).
	pub fn waker(&self) -> Waker {
		self.0.waker()
	}
}
