//! The completion reactor: the backend, the operations in flight, and the
//! cancellable completions they resolve to.
//!
//! This is the low layer. A [`Reactor`] owns one completion backend and is
//! driven and submitted to from the thread that built it; only its waker crosses
//! threads. An operation — `read`, `send`, `accept`, and the
//! rest, each named as an [`Op`] — returns a [`Token`] that names it until its
//! result is taken or it is released. Releasing a token whose operation is still
//! in flight cancels it and returns only once the backend has let go of the
//! memory it was given.
//!
//! The async surface over it — the executor, the facade, and the transfer
//! futures — lives in the `completion-async` crate, which re-exports the data
//! types and the descriptor trait from here.

mod backend;
mod desc;
mod error;
mod memory;
mod reactor;

pub use backend::{Config, Response};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket, RawDescriptor};
pub use error::{Error, ErrorKind, Status};
pub use memory::{Extent, Extents, IoVec, Memory};
pub use reactor::{
	Handle, InFlight, InFlightState, Lease, Op, Reactor, Space, Span, TimerKey, Token,
};
