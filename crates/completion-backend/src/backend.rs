use std::task::Waker;
use std::time::Duration;

use enum_dispatch::enum_dispatch;
use socket2::SockAddr;

use crate::desc::{BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
use crate::error::Result;
use crate::memory::Span;

#[cfg(not(any(unix, windows)))]
compile_error!("completion-backend does not support this target");

#[cfg(windows)]
mod iocp;
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
	/// Completion mechanism to use; [`BackendKind::default`] selects the
	/// platform default.
	pub backend: BackendKind,
}

impl Default for Config {
	fn default() -> Self {
		Config {
			entries: 256,
			iopoll: false,
			sqpoll: false,
			backend: BackendKind::default(),
		}
	}
}

/// An I/O operation submitted to a [`Backend`].
pub enum Op {
	/// Reads into `span` at `offset` (or at the descriptor's current position when `None`).
	Read { offset: Option<u64>, span: Span },
	/// Writes from `span` at `offset` (or at the descriptor's current position when `None`).
	Write { offset: Option<u64>, span: Span },
	/// Flushes all modified data and metadata to disk (`fsync`).
	Fsync,
	/// Checks for deferred write-back errors without forcing data to disk.
	Flush,
	/// Reserves `len` bytes from `offset`, extending the file when the range ends
	/// past its length.
	Allocate { offset: u64, len: u64 },
	/// Deallocates `len` bytes from `offset`, zeroing the range and keeping the
	/// file's length.
	Punch { offset: u64, len: u64 },
	/// Receives into `span`.
	Recv { span: Span },
	/// Receives into `span`, also capturing ancillary file descriptors (`SCM_RIGHTS`).
	RecvWithFds { span: Span },
	/// Sends from `span`, attaching `fds` in an ancillary control message (`SCM_RIGHTS`).
	Send {
		span: Span,
		fds: Vec<OwnedDescriptor>,
	},
	/// Accepts the next connection on a listening socket.
	Accept,
	/// Connects a socket to `addr`.
	Connect { addr: SockAddr },
	/// Waits until the descriptor becomes readable.
	Readable,
	/// Waits until the descriptor becomes writable.
	Writable,
	/// Waits until the peer closes both directions or the connection resets.
	Hangup,
}

impl Op {
	/// Returns the [`Span`] this operation transfers through, or `None` if it
	/// transfers no bytes.
	pub fn span(&self) -> Option<&Span> {
		match self {
			Op::Read { span, .. }
			| Op::Write { span, .. }
			| Op::Recv { span }
			| Op::RecvWithFds { span }
			| Op::Send { span, .. } => Some(span),
			Op::Fsync
			| Op::Flush
			| Op::Allocate { .. }
			| Op::Punch { .. }
			| Op::Accept
			| Op::Connect { .. }
			| Op::Readable
			| Op::Writable
			| Op::Hangup => None,
		}
	}
}

/// Output of a completed [`Op`], reported by [`Backend::poll`].
#[derive(Debug)]
pub enum Response {
	/// Number of bytes transferred.
	Count(usize),
	/// Number of bytes and ancillary descriptors received.
	CountWithFds(usize, Vec<OwnedDescriptor>),
	/// Accepted connection socket.
	Accepted(OwnedSocket),
	/// Operation completed without a return value (`Fsync`, `Flush`, `Allocate`, `Punch`, `Connect`, `Readable`, `Writable`, `Hangup`).
	Done,
}

#[enum_dispatch]
trait Driver<T> {
	fn submit(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64>;
	fn cancel(&mut self, id: u64);
	fn cancel_all(&mut self);
	fn is_idle(&self) -> bool;
	fn poll(
		&mut self,
		timeout: Option<Duration>,
		complete: impl FnMut(u64, T, Result<Response>),
	) -> Result<()>;
	fn waker(&self) -> Waker;
}

/// Completion mechanism used by a [`Backend`].
#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	Hash,
	strum::EnumString,
	strum::Display,
	strum::IntoStaticStr,
	strum::VariantArray,
)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum BackendKind {
	/// Readiness polling (`polling`) with file I/O on a thread pool.
	#[cfg_attr(all(unix, not(target_os = "linux")), default)]
	Portable,
	/// Linux `io_uring`.
	#[cfg(target_os = "linux")]
	#[default]
	Uring,
	/// Windows I/O completion port (IOCP).
	#[cfg(windows)]
	#[default]
	Iocp,
}

/// Platform completion driver.
///
/// Once [`Backend::submit`] returns `Ok(id)`, the caller must keep the
/// descriptor and any [`Buffer`](crate::Buffer) referenced by the operation
/// alive until [`Backend::poll`] yields a completion for `id`. Operation IDs
/// carry a generation, so cancelling an ID whose operation already completed is
/// a no-op.
#[enum_dispatch(Driver<T>)]
pub enum Backend<T = ()> {
	Portable(portable::Portable<T>),
	#[cfg(target_os = "linux")]
	Uring(uring::Uring<T>),
	#[cfg(windows)]
	Iocp(iocp::Iocp<T>),
}

impl<T> Backend<T> {
	/// Creates a new completion backend with `config`.
	pub fn new(config: &Config) -> Result<Self> {
		let backend = match config.backend {
			BackendKind::Portable => portable::Portable::new(config)?.into(),
			#[cfg(target_os = "linux")]
			BackendKind::Uring => uring::Uring::new(config)?.into(),
			#[cfg(windows)]
			BackendKind::Iocp => iocp::Iocp::new(config)?.into(),
		};
		Ok(backend)
	}

	/// Returns the completion mechanism this backend runs on.
	pub fn kind(&self) -> BackendKind {
		match self {
			Backend::Portable(_) => BackendKind::Portable,
			#[cfg(target_os = "linux")]
			Backend::Uring(_) => BackendKind::Uring,
			#[cfg(windows)]
			Backend::Iocp(_) => BackendKind::Iocp,
		}
	}

	/// Submits `op` on `fd`, and returns the id its completion reports under.
	///
	/// Resolves the [`Span`](crate::Span) of a transfer before submission, returning
	/// [`InvalidInput`](std::io::ErrorKind::InvalidInput) if its range is out of
	/// bounds. If this returns `Err`, the operation was not queued and no
	/// completion will be produced for it.
	pub fn submit(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64> {
		Driver::submit(self, fd, op, data)
	}

	/// Requests asynchronous cancellation of the in-flight operation `id`.
	///
	/// The operation's final result (cancelled or completed) is still delivered
	/// by [`Backend::poll`].
	pub fn cancel(&mut self, id: u64) {
		Driver::cancel(self, id)
	}

	/// Requests cancellation of every operation currently in flight.
	///
	/// As with [`Backend::cancel`], each operation's final result is still
	/// delivered by [`Backend::poll`].
	pub fn cancel_all(&mut self) {
		Driver::cancel_all(self)
	}

	/// Returns `true` if no operation is in flight and no completion is waiting
	/// to be handed over.
	pub fn is_idle(&self) -> bool {
		Driver::is_idle(self)
	}

	/// Polls for completed operations, waiting up to `timeout` if none are ready
	/// (`Some(Duration::ZERO)` never blocks; `None` waits indefinitely), and invokes
	/// `complete(id, data, result)` for each completion.
	pub fn poll(
		&mut self,
		timeout: Option<Duration>,
		complete: impl FnMut(u64, T, Result<Response>),
	) -> Result<()> {
		Driver::poll(self, timeout, complete)
	}

	/// Returns a thread-safe [`Waker`] that wakes a blocking [`Backend::poll`].
	pub fn waker(&self) -> Waker {
		Driver::waker(self)
	}
}
