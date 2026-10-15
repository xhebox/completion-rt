//! The timer future.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use completion_rt::{Reactor, TimerKey};

use crate::global;
use crate::{Error, Executor};

/// Resolves to `Ok(())` once `duration` has elapsed.
///
/// The timer is armed on construction: the deadline is `Instant::now() +
/// duration`, fixed there, and the wheel already holds it. Polling is what waits
/// for that deadline, so a `Timeout` that sits unpolled for a while still fires
/// on time. The constructor reports a reactor that has already stopped; the poll
/// reports one that stops in between.
pub struct Timeout {
	reactor: Reactor,
	key: TimerKey,
}

impl Timeout {
	/// A timer that fires `duration` from now, armed on this thread's global
	/// executor.
	#[fastrace::trace]
	pub fn new(duration: Duration) -> Result<Timeout, Error> {
		Timeout::arm(duration, global::reactor())
	}

	/// A timer that fires `duration` from now, armed on `executor`.
	#[fastrace::trace]
	pub fn with_executor(duration: Duration, executor: &Executor) -> Result<Timeout, Error> {
		Timeout::arm(duration, executor.reactor().clone())
	}

	fn arm(duration: Duration, reactor: Reactor) -> Result<Timeout, Error> {
		let key = reactor.insert_timer(Instant::now() + duration)?;
		Ok(Timeout { reactor, key })
	}
}

impl Future for Timeout {
	type Output = Result<(), Error>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		let this = self.get_mut();
		this.reactor.poll_timer(this.key, cx)
	}
}

impl Drop for Timeout {
	fn drop(&mut self) {
		// Unconditional: the key was armed by the constructor, and a `cancel`
		// of a fired or already-cancelled key is a no-op.
		self.reactor.cancel_timer(self.key);
	}
}
