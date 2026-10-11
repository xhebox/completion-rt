//! The handle an I/O object submits through, and the timer future.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use completion_rt::Completion;

/// The reactor an I/O object submits to: an [`Executor`](crate::Executor)
/// hands one out, and [`submitter`](crate::submitter) hands out the thread's
/// global one. It is passed to
/// [`Facade::with_submitter`](crate::Facade::with_submitter),
/// [`Facade::bind_with_submitter`](crate::Facade::bind_with_submitter) and
/// [`Timeout::with_submitter`]; their counterparts without it use the global one.
#[derive(Clone)]
pub struct Submitter(pub(crate) completion_rt::Submitter);

/// Resolves to `Ok(())` once `duration` has elapsed.
///
/// The timer is submitted on its first poll, so a submission error — a reactor
/// that has stopped, say — is returned there rather than from the constructor.
pub struct Timeout {
	duration: Duration,
	submitter: Submitter,
	completion: Option<Completion>,
}

impl Timeout {
	/// A timer that fires `duration` after its first poll, driven through this
	/// thread's global executor's [`submitter`](crate::submitter).
	pub fn new(duration: Duration) -> Timeout {
		Timeout::with_submitter(duration, &crate::submitter())
	}

	/// A timer that fires `duration` after its first poll, driven through
	/// `submitter`.
	pub fn with_submitter(duration: Duration, submitter: &Submitter) -> Timeout {
		Timeout {
			duration,
			submitter: submitter.clone(),
			completion: None,
		}
	}
}

impl Future for Timeout {
	type Output = io::Result<()>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		let this = self.get_mut();
		if this.completion.is_none() {
			match this.submitter.0.timeout(this.duration) {
				Ok(completion) => this.completion = Some(completion),
				Err(error) => return Poll::Ready(Err(error)),
			}
		}
		let completion = this.completion.as_mut().expect("just submitted");
		match Pin::new(completion).poll(cx) {
			Poll::Ready(((), result)) => {
				this.completion = None;
				Poll::Ready(result.map(|_| ()))
			}
			Poll::Pending => Poll::Pending,
		}
	}
}
