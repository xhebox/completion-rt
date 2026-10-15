//! What names a submitted operation, and what it reports while in flight.

use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use crate::backend::Response;
use crate::desc::RawDescriptor;
use crate::error::Error;

use super::{Entry, Shared, release};

/// One operation submitted to a [`Reactor`], held until its result is taken.
///
/// Dropping it before then settles the operation: one still in the kernel that
/// names caller memory is waited out before the drop returns. Unless
/// [`Token::cancel`] came first, that wait is a bug: it is logged as an error and
/// panics in a debug build. Forgetting a token leaks its entry, never the memory.
pub struct Token {
	pub(super) shared: Rc<Shared>,
	pub(super) id: u64,
	/// Set once the result is taken; the entry is gone and the drop has nothing to settle.
	pub(super) spent: bool,
}

impl Token {
	/// The result if it has landed, which spends the token; otherwise `waker`
	/// is woken when it lands.
	pub fn poll(&mut self, waker: &Waker) -> Poll<Result<Response, Error>> {
		assert!(
			!self.spent,
			"a token is not polled after its result was taken"
		);
		let mut events = self.shared.events.borrow_mut();
		let entry = events
			.get_mut(self.id)
			.expect("an entry lives as long as its token");
		match entry.response.take() {
			Some(response) => {
				events.remove(self.id);
				self.spent = true;
				Poll::Ready(response)
			}
			None => {
				if !entry.waker.as_ref().is_some_and(|w| w.will_wake(waker)) {
					entry.waker = Some(waker.clone());
				}
				Poll::Pending
			}
		}
	}

	/// Asks the operation to give up, without waiting for it. The result still
	/// lands; a cancel lands as [`ErrorKind::Cancelled`](crate::ErrorKind::Cancelled).
	pub fn cancel(&self) {
		if let Some(entry) = self.shared.events.borrow_mut().get_mut(self.id) {
			entry.give_up.ask();
		}
		self.shared.park.unpark();
	}
}

impl Drop for Token {
	fn drop(&mut self) {
		if !self.spent {
			release(&self.shared, self.id);
		}
	}
}
/// Cancellation stays recorded after dispatch so a later drop can distinguish cancellation from an unsignalled abandonment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GiveUp {
	None,
	Asked,
	Handled,
}

impl GiveUp {
	pub(super) fn asked(self) -> bool {
		self != GiveUp::None
	}

	pub(super) fn ask(&mut self) {
		if *self == GiveUp::None {
			*self = GiveUp::Asked;
		}
	}

	pub(super) fn handled(&mut self) {
		if *self == GiveUp::Asked {
			*self = GiveUp::Handled;
		}
	}
}

/// A diagnostic snapshot; descriptor numbers are borrowed metadata and keep no resource open.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InFlight {
	/// The id the reactor minted for the operation.
	pub id: u64,
	/// The operation's name: `"read"`, `"send"`, `"timeout"`, and the rest.
	pub op: &'static str,
	/// Where the operation is in its life.
	pub state: InFlightState,
	/// Its holder went without the result; the entry stays until the backend lets go.
	pub detached: bool,
	/// The descriptor the operation names, as a plain number; `None` for a
	/// timer, which names none. The snapshot keeps nothing open.
	pub descriptor: Option<RawDescriptor>,
	/// How long since the operation was submitted.
	pub age: Duration,
	/// Whether cancellation was requested, including requests not yet sent to the backend.
	pub cancel_requested: bool,
	/// Whether the operation reads or writes caller memory.
	pub names_memory: bool,
}

impl InFlight {
	pub(super) fn of(id: u64, entry: &Entry, now: Instant) -> InFlight {
		Self {
			id,
			op: entry.kind,
			state: entry.state,
			detached: entry.detached,
			descriptor: entry.descriptor.as_ref().map(|held| held.raw),
			age: now.saturating_duration_since(entry.submitted),
			cancel_requested: entry.give_up.asked(),
			names_memory: entry.names_memory,
		}
	}
}

/// Where an operation is in its life, as [`InFlight::state`] reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InFlightState {
	/// Submitted, waiting for the reactor to hand it to a backend.
	Pending,
	/// A backend has it.
	InFlight,
	/// The result has landed and waits for its holder.
	Done,
}
