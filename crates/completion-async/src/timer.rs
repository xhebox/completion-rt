use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Poll, ready};
use std::time::{Duration, Instant};

use completion_backend::Result;

use crate::global::submit_timer;

/// Waits until `duration` has elapsed.
///
/// The deadline (`Instant::now() + duration`) is captured when `timeout` is
/// called; the timer is registered on first poll with the active [`Scope`](crate::Scope)
/// and resolves to [`ErrorKind::Cancelled`](crate::ErrorKind::Cancelled) if that
/// scope is cancelled.
pub fn timeout(duration: Duration) -> impl Future<Output = Result<()>> {
	let deadline = Instant::now() + duration;
	let mut slot = None;
	poll_fn(move |cx| {
		let slot = slot.get_or_insert_with(|| submit_timer(deadline));
		ready!(Pin::new(slot).poll(cx))?;
		Poll::Ready(Ok(()))
	})
}
