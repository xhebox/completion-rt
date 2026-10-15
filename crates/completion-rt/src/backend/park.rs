//! One wake protocol for the reactor.
//!
//! The state word stands in for the futex behind `std::thread::{park, unpark}`:
//! a wake flips the word, and rings the backend's doorbell when a park has gone
//! to sleep in the backend.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

/// What wakes a park that is asleep in its backend: the backend's own
/// primitive, rung on the park's state word.
pub(crate) trait Doorbell: Send + Sync {
	/// Wakes the sleeping park. `word` is the park's state word, just set to
	/// the notified value.
	fn ring(&self, word: &AtomicU32);
}

/// No park is outstanding.
const EMPTY: u32 = 0;
/// A park is between its compare-exchange and its return.
pub(crate) const PARKED: u32 = 1;
/// A wake arrived with no park to take it.
const NOTIFIED: u32 = 2;

/// The reactor's single wake protocol.
pub(crate) struct Park {
	state: AtomicU32,
	doorbell: Arc<dyn Doorbell>,
}

impl Park {
	pub(crate) fn new(doorbell: Arc<dyn Doorbell>) -> Park {
		Self {
			state: AtomicU32::new(EMPTY),
			doorbell,
		}
	}

	/// The word a futex-waiting backend sleeps on: it waits while the word is
	/// [`PARKED`]. Only a futex-waiting backend reads it.
	#[allow(dead_code)]
	pub(crate) fn state(&self) -> &AtomicU32 {
		&self.state
	}

	/// Raises a wake for the reactor.
	pub(crate) fn unpark(&self) {
		// Only a park that has gone to sleep needs waking: one taken by the
		// failed compare-exchange in `park` is left in the state word.
		if self.state.swap(NOTIFIED, Ordering::AcqRel) == PARKED {
			self.doorbell.ring(&self.state);
		}
	}

	/// Whether a wake is pending, for a backend that spins instead of waiting.
	/// Only an IOPOLL ring reads it.
	#[allow(dead_code)]
	pub(crate) fn woke(&self) -> bool {
		self.state.load(Ordering::Acquire) == NOTIFIED
	}

	/// Sleeps in `poll` unless a wake arrived first.
	pub(crate) fn park(&self, poll: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
		if self
			.state
			.compare_exchange(EMPTY, PARKED, Ordering::AcqRel, Ordering::Acquire)
			.is_err()
		{
			// A wake landed before the compare-exchange: take it back so the
			// next park can sleep, and do not wait.
			self.state.store(EMPTY, Ordering::Release);
			return Ok(());
		}
		let result = poll();
		self.state.store(EMPTY, Ordering::Release);
		result
	}
}
