//! OS completion backend for Linux (`io_uring`), Windows (IOCP), and, on every
//! target, a portable one: `polling` for readiness with a thread pool for file
//! I/O, which [`BackendKind::Portable`] selects over the target's own.
//!
//! [`Backend`] submits an [`Op`] on a borrowed descriptor, hands back the
//! `u64` id its completion reports under, cancels in-flight operations by id,
//! and reports completed `(id, data, Result<Response>)` triples to the callback
//! passed to [`Backend::poll`], `data` being the value the submission filed
//! with the operation.
//!
//! The caller must keep each operation's descriptor and [`Buffer`] alive until
//! [`Backend::poll`] reports its completion.
//!
//! [`Driver`] wraps a [`Backend`] with what a single-threaded runtime needs on
//! top of it: the thread's wait, its timers, and the cancellation of what is in
//! flight. [`enter`] runs a closure on the driver entered on this thread, and
//! [`Slot`] tracks one submitted operation or timer through to its response.
//! For the high-level async runtime, executor, and resource handles, use
//! `completion-async`.

mod backend;
mod desc;
mod error;
mod memory;

pub use backend::{Backend, BackendKind, Config, Op, Response};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
pub use error::{Error, ErrorKind, Result};
pub use memory::{Buffer, BufferPool, IoVec, Memory, Span};
