//! One wake protocol for the reactor.
//!
//! The state word and the doorbell stand in for the futex behind
//! `std::thread::{park, unpark}`: a wake flips the word, or rings the doorbell
//! when the reactor has already gone to sleep in the backend.

use std::io;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::event::Event;

/// No park is outstanding.
const EMPTY: u32 = 0;
/// A park is between its compare-exchange and its return.
const PARKED: u32 = 1;
/// A wake arrived with no park to take it.
const NOTIFIED: u32 = 2;

/// The reactor's single wake protocol.
pub(crate) struct Park {
	state: AtomicU32,
	doorbell: Event,
	/// An embedded reactor never parks inside the reactor — an external poller
	/// waits on `poll_fd` — so the state never reaches `PARKED` and every
	/// unpark has to ring the doorbell.
	external: bool,
}

impl Park {
	pub(crate) fn new(doorbell: Event, external: bool) -> Park {
		Self {
			state: AtomicU32::new(EMPTY),
			doorbell,
			external,
		}
	}

	/// Raises a wake for the reactor.
	pub(crate) fn unpark(&self) {
		if self.external {
			let _ = self.doorbell.notify();
			return;
		}
		// Only a park that has gone to sleep needs the doorbell: one taken by
		// the failed compare-exchange in `park` is left in the state word.
		if self.state.swap(NOTIFIED, Ordering::AcqRel) == PARKED {
			let _ = self.doorbell.notify();
		}
	}

	/// Whether a wake is pending, for a backend that spins instead of waiting.
	#[cfg(target_os = "linux")]
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
