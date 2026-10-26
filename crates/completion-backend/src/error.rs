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
	/// An OS or I/O failure occurred; recover the [`io::Error`] with [`io::Error::try_from`].
	Failed,
	/// The reactor has been shut down and no longer accepts work.
	Stopped,
}

/// An error returned by a completion operation or runtime call.
#[derive(Debug)]
pub struct Error {
	kind: ErrorKind,
	done: usize,
	source: Option<io::Error>,
}

impl Error {
	/// Returns an [`Error`] with [`ErrorKind::Cancelled`].
	pub fn cancelled() -> Error {
		Error {
			kind: ErrorKind::Cancelled,
			done: 0,
			source: None,
		}
	}

	/// Returns an [`Error`] with [`ErrorKind::Stopped`].
	pub fn stopped() -> Error {
		Error {
			kind: ErrorKind::Stopped,
			done: 0,
			source: None,
		}
	}

	/// Attaches the number of bytes transferred before this error occurred.
	pub fn with_done(mut self, done: usize) -> Error {
		self.done = done;
		self
	}

	/// Returns the [`ErrorKind`] for this error.
	pub fn kind(&self) -> ErrorKind {
		self.kind
	}

	/// Returns the number of bytes transferred before this error occurred.
	pub fn done(&self) -> usize {
		self.done
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
			done: 0,
			source: Some(error),
		}
	}
}

impl TryFrom<Error> for io::Error {
	type Error = Error;

	fn try_from(error: Error) -> Result<io::Error> {
		match error.source {
			Some(source) => Ok(source),
			None => Err(error),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::error::Error as _;
	use std::io;

	use assert2::assert;
	use insta::assert_snapshot;
	use rstest::rstest;

	use super::{Error, ErrorKind};

	#[rstest]
	#[case::cancelled(Error::cancelled(), ErrorKind::Cancelled)]
	#[case::stopped(Error::stopped(), ErrorKind::Stopped)]
	fn cancelled_and_stopped_errors_preserve_their_kind_when_io_error_conversion_fails(
		#[case] error: Error,
		#[case] kind: ErrorKind,
	) {
		assert!(error.kind() == kind);
		assert!(error.source().is_none());
		assert!(let Err(returned) = io::Error::try_from(error));
		assert!(returned.kind() == kind);
	}

	#[test]
	fn an_io_error_round_trips_through_a_failed_error() {
		let error = Error::from(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
		assert!(error.kind() == ErrorKind::Failed);
		assert!(error.done() == 0);
		assert!(error.source().is_some());
		assert!(error.to_string().contains("denied"));
		let error = error.with_done(42);
		assert!(error.done() == 42);
		assert!(let Ok(io_error) = io::Error::try_from(error));
		assert!(io_error.kind() == io::ErrorKind::PermissionDenied);
	}

	#[test]
	fn display_describes_each_error_kind() {
		assert_snapshot!(Error::cancelled(), @"the operation was given up");
		assert_snapshot!(Error::stopped(), @"the reactor has been stopped");
		assert_snapshot!(Error::from(io::Error::other("io failure")), @"io failure");
	}
}
