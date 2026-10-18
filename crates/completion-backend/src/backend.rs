//! Target completion backend and operation types.

use std::task::Waker;
use std::time::Duration;

use enum_dispatch::enum_dispatch;
use strum::EnumDiscriminants;

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
	/// The completion mechanism to run on; [`BackendKind::default`] takes the
	/// target's own.
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
	pub fn span(&self) -> Option<&Span> {
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
}

/// What a target's completion mechanism does for [`Backend`].
#[enum_dispatch]
trait Driver {
	fn submit(&mut self, id: u64, fd: BorrowedDescriptor<'_>, op: Op) -> Result<()>;
	fn cancel(&mut self, id: u64);
	fn poll(
		&mut self,
		timeout: Option<Duration>,
		complete: impl FnMut(u64, Result<Response>),
	) -> Result<()>;
	fn waker(&self) -> Waker;
}

macro_rules! backends {
	($( $(#[cfg($cfg:meta)])? $(#[default($($default:tt)*)])? $variant:ident($ty:ty) ),+ $(,)?) => {
		/// The completion mechanism this target runs on: the ring or the portable
		/// backend on Linux, IOCP or the portable one on Windows, and the portable one
		/// alone on the other unix targets.
		#[derive(EnumDiscriminants)]
		#[strum_discriminants(name(BackendKind), vis(pub))]
		#[strum_discriminants(doc = "The completion mechanism a [`Backend`] runs on, chosen at runtime.")]
		#[strum_discriminants(derive(
			Default,
			Hash,
			strum::EnumString,
			strum::Display,
			strum::IntoStaticStr,
			strum::VariantArray
		))]
		#[strum_discriminants(strum(serialize_all = "lowercase", ascii_case_insensitive))]
		#[enum_dispatch(Driver)]
		enum BackendImpl {
			$(
				$(#[cfg($cfg)])?
				$(#[cfg_attr($($default)*, strum_discriminants(default))])?
				$variant($ty),
			)+
		}

		impl BackendKind {
			fn build(self, config: &Config) -> Result<BackendImpl> {
				match self {
					$( $(#[cfg($cfg)])? Self::$variant => Ok(<$ty>::new(config)?.into()), )+
				}
			}
		}
	};
}

backends! {
	#[cfg(target_os = "linux")]
	#[default(target_os = "linux")]
	Uring(uring::Uring),
	#[default(all(unix, not(target_os = "linux")))]
	Portable(portable::Portable),
	#[cfg(windows)]
	#[default(windows)]
	Iocp(iocp::Iocp),
}

/// Platform completion driver.
///
/// Operations are keyed by a caller-supplied `u64` id (avoid `0` and
/// `u64::MAX`, which `io_uring` reserves for internal tokens). Once
/// [`Backend::submit`] returns `Ok(())`, the caller must keep the descriptor
/// and any [`Buffer`](crate::Buffer) referenced by the operation alive until
/// [`Backend::poll`] yields a completion for `id`.
pub struct Backend(BackendImpl);

impl Backend {
	/// Creates a new completion backend with `config`.
	pub fn new(config: &Config) -> Result<Self> {
		Ok(Backend(config.backend.build(config)?))
	}

	/// Returns the completion mechanism this backend runs on.
	pub fn kind(&self) -> BackendKind {
		BackendKind::from(&self.0)
	}

	/// Submits `op` on `fd` under `id`.
	///
	/// Resolves any [`Span`] extents before submission, returning
	/// [`InvalidInput`](std::io::ErrorKind::InvalidInput) if an extent is out of
	/// bounds. If this returns `Err`, the operation was not queued and no
	/// completion will be produced for `id`.
	pub fn submit(&mut self, id: u64, fd: BorrowedDescriptor<'_>, op: Op) -> Result<()> {
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
		complete: impl FnMut(u64, Result<Response>),
	) -> Result<()> {
		self.0.poll(timeout, complete)
	}

	/// Returns a thread-safe [`Waker`] that wakes a blocking [`Backend::poll`].
	pub fn waker(&self) -> Waker {
		self.0.waker()
	}
}
