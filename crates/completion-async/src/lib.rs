//! Async I/O runtime built on OS completion backends (`io_uring`, IOCP, and
//! `polling`).
//!
//! Run futures on the current thread's default executor with [`block_on`] and
//! [`spawn`], or create a dedicated [`Executor`]. A [`Scope`] is the
//! cancellation tree an operation, timer, or task runs in: [`Scope::spawn`]
//! runs a task in it, every operation the task submits is filed with it, and a
//! task spawned while it is active inherits it. [`Executor::scope`] and
//! [`scope`] both derive a child of the root scope, which nothing can cancel.
//!
//! A resource is wrapped in the handle whose operations it takes:
//!
//! | Resource | Operations |
//! |---|---|
//! | [`File`] | `read_at`, `read_exact_at`, `write_at`, `write_all_at`, `sync`, `flush`, `allocate`, `punch` |
//! | [`Stream`] (pipes, PTYs, TAPs, stream sockets) | `read`, `read_exact`, `write`, `write_all`, `recv`, `recv_exact`, `send`, `send_all`, the `_with_fds` forms, `shutdown`, `take_error`, `hangup` |
//! | [`Socket`] (datagram sockets) | `recv`, `recv_with_fds`, `send`, `send_with_fds`, `take_error` |
//! | [`Listener`] (listening sockets) | `accept` |
//! | [`Readiness`] (eventfd, pidfd, signalfd) | `readable`, `writable` |
//!
//! A single call may transfer fewer bytes than requested and returns the byte
//! count; `_exact` and `_all` repeat until the [`Span`] is full and return an
//! [`Outcome`] preserving partial progress on error or cancellation.
//!
//! Each handle is built from a standard resource (`impl Into<std::fs::File>`,
//! `impl Into<OwnedDescriptor>`, `impl Into<OwnedSocket>`), is cheap to clone,
//! and is `Send + Sync`. It binds no reactor: each operation resolves the
//! reactor when first polled — the [`Executor`] whose [`block_on`] is running
//! on the thread, or else the thread's default one. `try_take` returns the
//! underlying resource once no clones or in-flight operations remain.
//!
//! Data transfers operate on [`Span`]s sliced from a [`Buffer`]. A
//! [`BufferPool`] hands out pre-faulted, page-aligned buffers for repeated
//! transfers.
//!
//! Run operations and timers inside a [`Scope`] to abort them and wait
//! until the kernel releases their buffers:
//!
//! ```no_run
//! # use completion_async::{Buffer, File, Scope, block_on};
//! # fn example(file: std::fs::File, buffer: Buffer, scope: Scope) -> completion_async::Result<()> {
//! let file = File::new(file);
//! let outcome = block_on(async {
//! 	scope.spawn(file.write_all_at(buffer.span(0, 512), 0)).await
//! })?;
//! # Ok(())
//! # }
//! ```

mod executor;
mod io;
mod net;
mod readiness;
mod timer;

pub use completion_backend::{
	AsDescriptor, BackendKind, BorrowedDescriptor, Buffer, BufferPool, Config, Error, ErrorKind,
	IoVec, Memory, OwnedDescriptor, OwnedSocket, Result, Span,
};
pub use executor::{Executor, JoinHandle, block_on, spawn};
pub use io::{File, OpenOptions, Outcome};
pub use net::{Listener, Received, Socket, Stream, ToSockAddr};
pub use readiness::Readiness;
pub use timer::timeout;
