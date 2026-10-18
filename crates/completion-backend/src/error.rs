//! Error types for completion operations.

use std::error::Error as StdError;
use std::fmt;
use std::io;

/// The result of a completion operation or runtime call, with [`Error`] as the
/// failure every one of them reports.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Category of an [`Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
	/// The operation was cancelled.
	Cancelled,
	/// The executor was dropped before the task could be spawned.
	ExecutorClosed,
	/// An OS or I/O failure occurred; see [`Error::source`](StdError::source) for the underlying [`io::Error`].
	Failed,
	/// The reactor has been shut down and no longer accepts work.
	Stopped,
}

/// Whether retrying the operation may succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Status {
	/// Retrying the same call will fail again.
	Permanent,
	/// The failure is transient (`Interrupted`, `WouldBlock`, or `TimedOut`) and the call can be retried.
	Temporary,
}

/// An error returned by a completion operation or runtime call.
#[derive(Debug)]
pub struct Error {
	kind: ErrorKind,
	source: Option<io::Error>,
}

impl Error {
	#[doc(hidden)]
	pub fn cancelled() -> Error {
		Error {
			kind: ErrorKind::Cancelled,
			source: None,
		}
	}

	#[doc(hidden)]
	pub fn executor_closed() -> Error {
		Error {
			kind: ErrorKind::ExecutorClosed,
			source: None,
		}
	}

	#[doc(hidden)]
	pub fn stopped() -> Error {
		Error {
			kind: ErrorKind::Stopped,
			source: None,
		}
	}

	/// Returns the [`ErrorKind`] for this error.
	pub fn kind(&self) -> ErrorKind {
		self.kind
	}

	/// Returns whether the error is [`Status::Temporary`] or [`Status::Permanent`].
	pub fn status(&self) -> Status {
		match self.source.as_ref().map(io::Error::kind) {
			Some(
				io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut,
			) => Status::Temporary,
			_ => Status::Permanent,
		}
	}
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match (self.kind, &self.source) {
			(ErrorKind::Failed, Some(source)) => source.fmt(f),
			(ErrorKind::Failed, None) => f.write_str("the call failed"),
			(ErrorKind::Cancelled, _) => f.write_str("the operation was given up"),
			(ErrorKind::ExecutorClosed, _) => {
				f.write_str("the executor was dropped before spawning a task")
			}
			(ErrorKind::Stopped, _) => f.write_str("the reactor has been stopped"),
		}
	}
}

impl StdError for Error {
	fn source(&self) -> Option<&(dyn StdError + 'static)> {
		self.source
			.as_ref()
			.map(|source| source as &(dyn StdError + 'static))
	}
}

impl From<io::Error> for Error {
	fn from(error: io::Error) -> Error {
		Error {
			kind: ErrorKind::Failed,
			source: Some(error),
		}
	}
}

impl From<Error> for io::Error {
	fn from(error: Error) -> io::Error {
		match error.source {
			Some(source) => source,
			// Cancellation is not Interrupted: retrying needs the caller's decision.
			None => io::Error::other(error),
		}
	}
}
