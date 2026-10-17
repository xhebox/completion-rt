//! Target completion backend and operation types.

use std::io;
use std::task::Waker;
use std::time::Duration;

use crate::desc::{BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
use crate::error::Error;
use crate::memory::{Buffer, Span};

#[cfg(not(any(unix, windows)))]
compile_error!("completion-backend does not support this target");

#[cfg(windows)]
mod iocp;
#[cfg(all(unix, not(target_os = "linux")))]
mod portable;
#[cfg(target_os = "linux")]
mod uring;

/// Configuration for [`Backend::new`].
#[derive(Clone, Copy, Debug)]
pub struct Config {
	/// Submission queue capacity (`io_uring` only).
	pub entries: u32,
	/// Poll the kernel for completions instead of sleeping on interrupts (`io_uring` `IORING_SETUP_IOPOLL`).
	pub iopoll: bool,
	/// Let a kernel thread consume the submission ring (`io_uring` `IORING_SETUP_SQPOLL`).
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

/// Output of a completed [`Op`].
#[derive(Debug)]
pub enum Response {
	/// Number of bytes transferred.
	Count(usize),
	/// Number of bytes and ancillary descriptors received.
	CountWithFds(usize, Vec<OwnedDescriptor>),
	/// Accepted connection socket.
	Accepted(OwnedSocket),
	/// Operation completed without a return value (`Fsync`, `Flush`, `Allocate`, `Punch`, `Readable`, `Writable`, `Hangup`).
	Done,
}

impl Response {
	/// Extracts the transferred byte count, dropping any received descriptors.
	/// Returns an error for [`Response::Accepted`] and [`Response::Done`].
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

/// An I/O operation submitted to [`Backend::submit`].
pub enum Op {
	Read {
		offset: u64,
		span: Span,
	},
	Write {
		offset: u64,
		span: Span,
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
		span: Span,
	},
	RecvWithFds {
		span: Span,
	},
	Send {
		span: Span,
		fds: Vec<OwnedDescriptor>,
	},
	Accept,
	Readable,
	Writable,
	/// Waits until the peer closes both directions or the connection resets.
	Hangup,
}

impl Op {
	/// Returns the [`Span`] this operation transfers through, or `None` if it
	/// transfers no bytes.
	pub(crate) fn span(&self) -> Option<&Span> {
		match self {
			Op::Read { span, .. }
			| Op::Write { span, .. }
			| Op::Recv { span, .. }
			| Op::RecvWithFds { span, .. }
			| Op::Send { span, .. } => Some(span),
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

	/// Returns the [`Buffer`] accessed by this operation, or `None` if it transfers no bytes.
	pub fn buffer(&self) -> Option<Buffer> {
		self.span()
			.filter(|span| span.len() > 0)
			.map(|span| span.buffer().clone())
	}
}

#[cfg(windows)]
type BackendImpl = iocp::Iocp;
#[cfg(all(unix, not(target_os = "linux")))]
type BackendImpl = portable::Portable;
#[cfg(target_os = "linux")]
type BackendImpl = uring::Uring;

/// Platform completion driver.
///
/// Operations are keyed by a caller-supplied `u64` id (avoid `0` and
/// `u64::MAX`, which `io_uring` reserves for internal tokens). Once
/// [`Backend::submit`] returns `Ok(())`, the caller must keep the descriptor
/// and any [`Buffer`] referenced by the operation alive until [`Backend::poll`]
/// yields a completion for `id`.
pub struct Backend(BackendImpl);

impl Backend {
	/// Creates a new completion backend with `config`.
	pub fn new(config: &Config) -> Result<Self, Error> {
		Ok(Backend(BackendImpl::new(config)?))
	}

	/// Submits `op` on `fd` under `id`.
	///
	/// Resolves any [`Span`] extents before submission, returning
	/// [`InvalidInput`](std::io::ErrorKind::InvalidInput) if an extent is out of
	/// bounds. If this returns `Err`, the operation was not queued and no
	/// completion will be produced for `id`.
	pub fn submit(&mut self, id: u64, fd: BorrowedDescriptor<'_>, op: Op) -> Result<(), Error> {
		self.0.submit(id, fd, op)
	}

	/// Requests asynchronous cancellation of the in-flight operation `id`.
	///
	/// The operation's final result (cancelled or completed) is still delivered
	/// by [`Backend::poll`].
	pub fn cancel(&mut self, id: u64) {
		self.0.cancel(id)
	}

	/// Polls for completed operations, waiting up to `timeout` if none are ready
	/// (`Some(Duration::ZERO)` never blocks; `None` waits indefinitely), and invokes
	/// `complete(id, result)` for each completion.
	pub fn poll(
		&mut self,
		timeout: Option<Duration>,
		complete: impl FnMut(u64, Result<Response, Error>),
	) -> Result<(), Error> {
		self.0.poll(timeout, complete)
	}

	/// Returns a thread-safe [`Waker`] that wakes a blocking [`Backend::poll`].
	pub fn waker(&self) -> Waker {
		self.0.waker()
	}
}
