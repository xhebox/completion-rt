use std::error::Error as StdError;
use std::fmt;
use std::io;

/// Result alias using [`Error`] as the default error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Category of an [`Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
	/// The operation was cancelled.
	Cancelled,
	/// An OS or I/O failure occurred; see [`Error::source`](StdError::source) for the underlying [`io::Error`].
	Failed,
	/// The reactor has been shut down and no longer accepts work.
	Stopped,
}

/// An error returned by a completion operation or runtime call.
#[derive(Debug)]
pub struct Error {
	kind: ErrorKind,
	source: Option<io::Error>,
}

impl Error {
	/// Returns an [`Error`] with [`ErrorKind::Cancelled`].
	pub fn cancelled() -> Error {
		Error {
			kind: ErrorKind::Cancelled,
			source: None,
		}
	}

	/// Returns an [`Error`] with [`ErrorKind::Stopped`].
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
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match (self.kind, &self.source) {
			(ErrorKind::Failed, Some(source)) => source.fmt(f),
			(ErrorKind::Failed, None) => f.write_str("the call failed"),
			(ErrorKind::Cancelled, _) => f.write_str("the operation was given up"),
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
