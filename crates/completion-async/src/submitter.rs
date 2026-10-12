//! The handle an I/O object submits through.

use completion_rt::Submitter as LowSubmitter;

/// The reactor an I/O object submits to: an [`Executor`](crate::Executor)
/// hands one out, and [`submitter`](crate::submitter) hands out the thread's
/// global one. It is passed to
/// [`Facade::with_submitter`](crate::Facade::with_submitter) and
/// [`Facade::bind_with_submitter`](crate::Facade::bind_with_submitter); their
/// counterparts without it use the global one.
#[derive(Clone)]
pub struct Submitter(pub(crate) LowSubmitter);
