//! The completion reactor: the backend, the operations in flight, and the
//! cancellable completions they resolve to.
//!
//! This is the low layer. A [`Reactor`] owns one completion backend and hands
//! out a [`Submitter`] that any thread submits an operation through — `read`,
//! `send`, `accept`, and the rest — and the returned [`Completion`] resolves
//! when the completion arrives, yielding what that operation produces. Dropping
//! a `Completion` without awaiting cancels the operation and returns only once
//! the backend has let go of the memory it was given.
//!
//! The async surface over it — the executor, the facade, and the transfer
//! futures — lives in the `completion-async` crate, which re-exports the data
//! types and the descriptor trait from here.

mod backend;
mod core;
mod desc;
mod event;
mod memory;
mod park;

pub use backend::Config;
pub use core::{
	Cancel, Cancelled, Completion, Handle, InFlight, InFlightState, Reactor, Response, Submitter,
	Until,
};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket, RawDescriptor};
pub use event::Event;
pub use memory::{Extent, Memory, Span};

// Plumbing the async crate reaches across the crate boundary. `pub(crate)` does
// not cross crates, so these are public but not part of the supported surface.
#[doc(hidden)]
pub use core::{Lease, ReactorWaker, cancel_task_future};
#[doc(hidden)]
pub use memory::Extents;
