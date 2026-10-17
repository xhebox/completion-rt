//! OS completion backend for Linux (`io_uring`), Windows (IOCP), and other
//! Unix targets (`polling` with a thread pool for regular files).
//!
//! [`Backend`] submits an [`Op`] on a borrowed descriptor with a caller-chosen
//! `u64` id, cancels in-flight operations by id, and reports completed
//! `(id, Result<Response, Error>)` pairs to the callback passed to [`Backend::poll`].
//!
//! The caller owns the operation table and must keep each operation's
//! descriptor and [`Buffer`] alive until [`Backend::poll`] reports its
//! completion. For the high-level async runtime, executor, and resource
//! facades, use `completion-async`.

mod backend;
mod desc;
mod error;
mod memory;

pub use backend::{Backend, Config, Op, Response};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
pub use error::{Error, ErrorKind, Status};
pub use memory::{Buffer, BufferPool, Extent, IoVec, Memory, Span};
