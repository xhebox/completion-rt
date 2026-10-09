//! Results of facade operations.

use std::error::Error as StdError;
use std::fmt;
use std::io;

/// Why a facade operation stopped.
#[derive(Debug)]
pub enum Error {
	Cancelled,
	Io(io::Error),
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Error::Cancelled => f.write_str("the operation was given up"),
			Error::Io(error) => error.fmt(f),
		}
	}
}

impl StdError for Error {
	fn source(&self) -> Option<&(dyn StdError + 'static)> {
		match self {
			Error::Cancelled => None,
			Error::Io(error) => Some(error),
		}
	}
}

impl From<io::Error> for Error {
	fn from(error: io::Error) -> Error {
		Error::Io(error)
	}
}

impl From<Error> for io::Error {
	fn from(error: Error) -> io::Error {
		match error {
			Error::Io(error) => error,
			// Cancellation is not Interrupted: retrying needs the caller's decision.
			Error::Cancelled => io::Error::new(io::ErrorKind::Other, Error::Cancelled),
		}
	}
}

/// A value and whether the operation completed; partial progress is retained on error or cancellation.
#[derive(Debug)]
pub struct Outcome<T> {
	pub value: T,
	pub result: Result<(), Error>,
}

impl<T> Outcome<T> {
	pub(super) fn new(value: T, result: Result<(), Error>) -> Outcome<T> {
		Self { value, result }
	}

	/// Discards partial progress on failure.
	pub fn into_result(self) -> Result<T, Error> {
		self.result.map(|()| self.value)
	}
}
