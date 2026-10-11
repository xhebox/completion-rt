//! The error every completion-async operation reports.

use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::panic::Location;

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

/// An error from a completion-async operation.
#[derive(Debug)]
pub struct Error {
	kind: ErrorKind,
	status: Status,
	source: Option<io::Error>,
	location: &'static Location<'static>,
}

impl Error {
	#[track_caller]
	pub(crate) fn cancelled() -> Error {
		Error {
			kind: ErrorKind::Cancelled,
			status: Status::Permanent,
			source: None,
			location: Location::caller(),
		}
	}

	#[track_caller]
	pub(crate) fn executor_closed() -> Error {
		Error {
			kind: ErrorKind::ExecutorClosed,
			status: Status::Permanent,
			source: None,
			location: Location::caller(),
		}
	}

	pub fn kind(&self) -> ErrorKind {
		self.kind
	}

	pub fn status(&self) -> Status {
		self.status
	}

	/// The call site that created this error.
	pub fn location(&self) -> &'static Location<'static> {
		self.location
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
	#[track_caller]
	fn from(error: io::Error) -> Error {
		let status = match error.kind() {
			io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
				Status::Temporary
			}
			_ => Status::Permanent,
		};
		Error {
			kind: ErrorKind::Failed,
			status,
			source: Some(error),
			location: Location::caller(),
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
