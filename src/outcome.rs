//! What a facade operation hands back.

use std::error::Error as StdError;
use std::fmt;
use std::io;

use crate::OwnedDescriptor;

/// Why a facade operation did not finish.
#[derive(Debug)]
pub enum Error {
	/// The caller's own cancel ended the operation; see the `until` on the
	/// future it came from.
	Cancelled,
	/// The kernel refused the call, or the operation failed.
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
			// `Other` with the error itself behind it: a given-up operation is
			// not `Interrupted`, which tells a caller to make the same call
			// again, and the caller who asked for the cancel is the only one
			// who can tell what to do about it.
			Error::Cancelled => io::Error::new(io::ErrorKind::Other, Error::Cancelled),
		}
	}
}

/// What an operation got through, and whether it finished.
///
/// A transfer that fails or is given up reports the calls it did land: `value`
/// is the progress up to that point, and `result` says why it stopped there.
#[derive(Debug)]
pub struct Outcome<T> {
	/// The bytes moved, or the descriptors gathered.
	pub value: T,
	/// `Ok(())` when the operation finished; the reason it stopped otherwise.
	pub result: Result<(), Error>,
}

impl<T> Outcome<T> {
	pub(super) fn new(value: T, result: Result<(), Error>) -> Outcome<T> {
		Self { value, result }
	}

	/// The value when the operation finished, the reason it did not otherwise.
	pub fn into_result(self) -> Result<T, Error> {
		self.result.map(|()| self.value)
	}
}

/// What a receive that asked for control data took in.
#[derive(Debug)]
pub struct Received {
	/// The bytes that arrived.
	pub bytes: usize,
	/// The descriptors the control messages carried.
	pub fds: Vec<OwnedDescriptor>,
}

impl Received {
	pub(super) fn new(bytes: usize, fds: Vec<OwnedDescriptor>) -> Received {
		Self { bytes, fds }
	}
}
