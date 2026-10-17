//! Timer future ([`Timeout`]).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use completion_backend::Error;

use crate::Executor;
use crate::completion::{Cancel, Completion, submit_timer};
use crate::global;
use crate::reactor::Reactor;

/// Future that resolves to `Ok(())` after a specified [`Duration`].
///
/// The deadline (`Instant::now() + duration`) is recorded when `Timeout::new` or
/// `Timeout::with_executor` is called, and registered with the reactor on first poll.
pub struct Timeout {
	reactor: Reactor,
	deadline: Instant,
	cancel: Option<Cancel>,
	call: Option<Completion>,
	finished: bool,
}

impl Timeout {
	/// Creates a timer that expires after `duration` on the current thread's
	/// default [`Executor`].
	#[fastrace::trace]
	pub fn new(duration: Duration) -> Timeout {
		Timeout::arm(duration, global::reactor())
	}

	/// Creates a timer that expires after `duration` on `executor`.
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

	/// Cancels the timer when `cancel` is triggered.
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
