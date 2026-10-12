//! The timer future and the handle it arms through.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use completion_rt::TimerKey;

/// The timer wheel of an [`Executor`](crate::Executor), or of this thread's
/// global one through [`timers`](crate::timers). Cloning it is cheap; every
/// clone arms timers on the same reactor.
#[derive(Clone)]
pub struct Timers(pub(crate) completion_rt::Timers);

/// Resolves to `Ok(())` once `duration` has elapsed.
///
/// The timer is armed on its first poll, so a failure — a reactor that has
/// stopped, say — is returned there rather than from the constructor.
pub struct Timeout {
	duration: Duration,
	timers: Timers,
	key: Option<TimerKey>,
}

impl Timeout {
	/// A timer that fires `duration` after its first poll, armed on this
	/// thread's global executor's [`timers`](crate::timers).
	pub fn new(duration: Duration) -> Timeout {
		Timeout::with_timers(duration, &crate::timers())
	}

	/// A timer that fires `duration` after its first poll, armed on `timers`.
	pub fn with_timers(duration: Duration, timers: &Timers) -> Timeout {
		Timeout {
			duration,
			timers: timers.clone(),
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
			None => match this.timers.0.insert(Instant::now() + this.duration) {
				Ok(key) => *this.key.insert(key),
				Err(error) => return Poll::Ready(Err(error)),
			},
		};
		match this.timers.0.poll(key, cx) {
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
			self.timers.0.cancel(key);
		}
	}
}
