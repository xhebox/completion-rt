//! The OS completion backend: `io_uring` on Linux, a completion port on
//! Windows, and a readiness poller with a thread pool for other unix.
//!
//! This is the low layer and nothing above it. A [`Backend`] submits an
//! [`Op`] on a descriptor under an id of the caller's choosing; the mechanism
//! carries that id and hands it back with the operation's [`Response`]. It
//! keeps no table of operations, has no keys, and never waits on its own: what
//! an id means, which memory and descriptors an operation may name, and when to
//! poll are all the caller's.
//!
//! The async surface over it — the executor, the facade, the transfer futures,
//! and the timers — lives in the `completion-async` crate, which re-exports the
//! data types and the descriptor trait from here.

mod backend;
mod desc;
mod error;
mod memory;

pub use backend::{Backend, Config, Op, Response};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
pub use error::{Error, ErrorKind, Status};
pub use memory::{Extent, IoVec, Memory, Space, Span};
