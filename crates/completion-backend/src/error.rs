//! The error every reactor operation reports, and the async layer over it.

use std::error::Error as StdError;
use std::fmt;
use std::io;

/// What the caller does next. Matching on it is the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
	/// The caller gave up on the operation.
	Cancelled,
	/// The executor was dropped, so nothing will run the task.
	ExecutorClosed,
	/// The kernel or backend failed the call; the error is its [`source`](StdError::source).
	Failed,
	/// The reactor has been shut down, so nothing new can be armed or submitted.
	Stopped,
}

/// Whether repeating the same call can succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Status {
	/// Repeating the call cannot succeed as issued.
	Permanent,
	/// Repeating the call may succeed.
	Temporary,
}

/// An error from a reactor operation.
#[derive(Debug)]
pub struct Error {
	kind: ErrorKind,
	source: Option<io::Error>,
}

impl Error {
	/// The caller gave up on the operation.
	#[doc(hidden)]
	pub fn cancelled() -> Error {
		Error {
			kind: ErrorKind::Cancelled,
			source: None,
		}
	}

	/// The executor was dropped. Only the async layer, which owns executors, builds this.
	#[doc(hidden)]
	pub fn executor_closed() -> Error {
		Error {
			kind: ErrorKind::ExecutorClosed,
			source: None,
		}
	}

	/// The reactor has been shut down; only the layer that owns the shutdown
	/// builds this.
	#[doc(hidden)]
	pub fn stopped() -> Error {
		Error {
			kind: ErrorKind::Stopped,
			source: None,
		}
	}

	pub fn kind(&self) -> ErrorKind {
		self.kind
	}

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
