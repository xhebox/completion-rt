//! Results of facade operations.

use crate::Error;

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
