//! OS completion backend for Linux (`io_uring`), Windows (IOCP), and, on every
//! target, a portable one: `polling` for readiness with a thread pool for file
//! I/O, which [`BackendKind::Portable`] selects over the target's own.
//!
//! [`Backend`] submits an [`Op`] on a borrowed descriptor with a caller-chosen
//! `u64` id, cancels in-flight operations by id, and reports completed
//! `(id, Result<Response>)` pairs to the callback passed to [`Backend::poll`].
//!
//! The caller owns the operation table and must keep each operation's
//! descriptor and [`Buffer`] alive until [`Backend::poll`] reports its
//! completion. For the high-level async runtime, executor, and resource
//! facades, use `completion-async`.

mod backend;
mod desc;
mod error;
mod memory;

pub use backend::{Backend, BackendKind, Config, Op, Response};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
pub use error::{Error, ErrorKind, Result, Status};
pub use memory::{Buffer, BufferPool, Memory, Span};
