//! OS completion backend for Linux (`io_uring`), Windows (IOCP), and, on every
//! target, a portable one: [`polling`](https://github.com/smol-rs/polling) for
//! readiness with a [`blocking`](https://github.com/smol-rs/blocking) thread
//! pool for file I/O, which [`BackendKind::Portable`] selects over the target's
//! own.
//!
//! [`Backend`] submits an [`Op`] on a borrowed descriptor, hands back the
//! `u64` id its completion reports under, cancels in-flight operations by id,
//! and reports completed `(id, data, Result<Response>)` triples to the callback
//! passed to [`Backend::poll`], `data` being the value the submission filed
//! with the operation.
//!
//! The caller must keep each operation's descriptor and [`IoVec`] memory alive
//! until [`Backend::poll`] reports its completion. For the high-level async
//! runtime, executor, and resource handles, use `completion-async`.
//!
//! # Platform notes
//!
//! - Linux `io_uring` requires kernel 5.11 or newer ([`Op::Hangup`] requires
//!   5.13+). On 6.15+, enabling the `iowait` Cargo feature withholds
//!   `IORING_ENTER_NO_IOWAIT` so idle ring waits count as system iowait.
//! - On Windows with `BackendKind::Iocp`, file and pipe handles submitted to
//!   [`Backend::submit`] must be opened with `FILE_FLAG_OVERLAPPED`.

mod backend;
mod desc;
mod error;

pub use backend::{Backend, BackendKind, Config, IoVec, Op, Response, Waker};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
pub use error::{Error, ErrorKind, Result};
