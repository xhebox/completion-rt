//! The timer future.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use completion_rt::Error;

use crate::Executor;
use crate::completion::{Cancel, Completion, submit_timer};
use crate::global;
use crate::reactor::Reactor;

/// Resolves to `Ok(())` once `duration` has elapsed.
///
/// The deadline is fixed on construction: it is `Instant::now() + duration`, and
/// the poll that arms it names it, so a `Timeout` that sits unpolled for a
/// while still fires on time. The constructor cannot fail; a reactor that has
/// already stopped is reported by the poll.
pub struct Timeout {
	reactor: Reactor,
	deadline: Instant,
	cancel: Option<Cancel>,
	call: Option<Completion>,
	finished: bool,
}

impl Timeout {
	/// A timer that fires `duration` from now, armed on this thread's global
	/// executor.
	#[fastrace::trace]
	pub fn new(duration: Duration) -> Timeout {
		Timeout::arm(duration, global::reactor())
	}

	/// A timer that fires `duration` from now, armed on `executor`.
	#[fastrace::trace]
	pub fn with_executor(duration: Duration, executor: &Executor) -> Timeout {
		Timeout::arm(duration, executor.reactor().clone())
	}

	fn arm(duration: Duration, reactor: Reactor) -> Timeout {
		Self {
			reactor,
			deadline: Instant::now() + duration,
			cancel: None,
			call: None,
			finished: false,
		}
	}

	/// Gives the timer up when `cancel` fires.
	pub fn until(mut self, cancel: &Cancel) -> Timeout {
		self.cancel = Some(cancel.clone());
		self
	}
}

impl Future for Timeout {
	type Output = Result<(), Error>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		let this = self.get_mut();
		assert!(!this.finished, "a timeout is not polled after it resolved");
		loop {
			if this.call.is_none() {
				if this.cancel.as_ref().is_some_and(Cancel::is_cancelled) {
					this.finished = true;
					return Poll::Ready(Err(Error::cancelled()));
				}
				match submit_timer(&this.reactor, this.deadline, this.cancel.as_ref()) {
					Ok(completion) => this.call = Some(completion),
					Err(error) => {
						this.finished = true;
						return Poll::Ready(Err(error));
					}
				}
			}
			let Some(call) = &mut this.call else {
				unreachable!("a timer is polled only with its timer in flight");
			};
			match call.poll(cx) {
				Poll::Pending => return Poll::Pending,
				// The cancel won: the timer has been disarmed.
				Poll::Ready(None) => {
					this.call = None;
					this.finished = true;
					return Poll::Ready(Err(Error::cancelled()));
				}
				Poll::Ready(Some(Ok(_))) => {
					this.call = None;
					this.finished = true;
					return Poll::Ready(Ok(()));
				}
				Poll::Ready(Some(Err(error))) => {
					this.call = None;
					this.finished = true;
					return Poll::Ready(Err(error));
				}
			}
		}
	}
}
