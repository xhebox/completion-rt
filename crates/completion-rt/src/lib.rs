//! The completion reactor: the backend, the operations in flight, and the
//! cancellable completions they resolve to.
//!
//! This is the low layer. A [`Reactor`] owns one completion backend and is
//! driven and submitted to from the thread that built it; only its waker and
//! its [`Event`] cross threads. An operation — `read`, `send`, `accept`, and the
//! rest, each named as an [`Op`] — returns a [`Token`] that names it until its
//! result is taken or it is released. Releasing a token whose operation is still
//! in flight cancels it and returns only once the backend has let go of the
//! memory it was given.
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
mod timer;

pub use backend::Config;
pub use core::{Handle, InFlight, InFlightState, Lease, Op, Reactor, Response, Token};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket, RawDescriptor};
pub use event::Event;
pub use memory::{Extent, Extents, Memory, Span};
pub use timer::TimerKey;
