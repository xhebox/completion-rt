//! The timer future.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use completion_rt::{Reactor, TimerKey};

use crate::Executor;

/// Resolves to `Ok(())` once `duration` has elapsed.
///
/// The timer is armed on its first poll, so a failure — a reactor that has
/// stopped, say — is returned there rather than from the constructor.
pub struct Timeout {
	duration: Duration,
	reactor: Reactor,
	key: Option<TimerKey>,
}

impl Timeout {
	/// A timer that fires `duration` after its first poll, armed on this
	/// thread's global executor.
	pub fn new(duration: Duration) -> Timeout {
		Timeout {
			duration,
			reactor: crate::global::reactor(),
			key: None,
		}
	}

	/// A timer that fires `duration` after its first poll, armed on `executor`.
	pub fn with_executor(duration: Duration, executor: &Executor) -> Timeout {
		Timeout {
			duration,
			reactor: executor.reactor().clone(),
			key: None,
		}
	}
}

impl Future for Timeout {
	type Output = io::Result<()>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		let this = self.get_mut();
		let key = match this.key {
			Some(key) => key,
			None => match this.reactor.insert_timer(Instant::now() + this.duration) {
				Ok(key) => *this.key.insert(key),
				Err(error) => return Poll::Ready(Err(error)),
			},
		};
		match this.reactor.poll_timer(key, cx) {
			Poll::Ready(result) => {
				this.key = None;
				Poll::Ready(result)
			}
			Poll::Pending => Poll::Pending,
		}
	}
}

impl Drop for Timeout {
	fn drop(&mut self) {
		if let Some(key) = self.key.take() {
			self.reactor.cancel_timer(key);
		}
	}
}
