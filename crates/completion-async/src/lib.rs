//! Async I/O runtime built on OS completion backends (`io_uring`, IOCP, and
//! `polling`).
//!
//! Run futures on the current thread's runtime with [`block_on`]. [`spawn`]
//! runs a `Send` task on the work-stealing pool, and [`spawn_local`] pins a
//! task to the current thread. The runtime is created with [`Config::default`]
//! on first use, or from a custom [`Config`] by calling [`init`] first.
//!
//! A resource is wrapped in the handle whose operations it takes:
//!
//! | Resource | Operations |
//! |---|---|
//! | [`File`] | `read_at`, `read_exact_at`, `write_at`, `write_all_at`, `sync`, `flush`, `allocate`, `punch` |
//! | [`Stream`] (pipes and byte streams) | `pipe`, `read`, `read_exact`, `write`, `write_all` |
//! | [`StreamSocket`] (stream sockets) | `recv`, `recv_exact`, `send`, `send_all`, `shutdown`, `take_error`, `hangup` |
//! | [`DatagramSocket`] (datagram sockets) | `recv`, `send`, `take_error` |
//! | [`Listener`] (listening sockets) | `accept` |
//! | [`Readiness`] (readiness descriptors and waitable handles) | `readable`, `writable` |
//!
//! A single call may transfer fewer bytes than requested and returns the byte
//! count; `_exact` and `_all` repeat until the [`AsIoVec`] is full and return an
//! [`Outcome`] preserving partial progress on error.
//!
//! Each handle is built from a standard resource (`impl Into<std::fs::File>`,
//! `impl Into<OwnedDescriptor>`, `impl Into<OwnedSocket>`), is cheap to clone,
//! and is `Send + Sync`. It binds no reactor: each operation resolves the
//! reactor when first polled: the runtime installed on the polling thread. `try_take` returns the
//! underlying resource once no clones or in-flight operations remain.
//!
//! Data transfers take an [`AsIoVec`] by value. [`Buffer`] implements it; a
//! clone is a cheap handle on the same bytes, so to read them after a transfer,
//! pass `buffer.clone()` and keep the original.
//!
//! Dropping an operation's future cancels and detaches it: the reactor keeps
//! the buffer and descriptor alive until the kernel finishes. For a
//! [`File`] this is safe — re-read from the same offset. For a [`Stream`],
//! [`StreamSocket`] or [`DatagramSocket`], bytes the kernel already consumed
//! are lost. Use [`Readiness`]
//! to wait for readability, then read synchronously, when cancellation safety
//! matters.

mod executor;
mod io;
mod memory;
mod net;
mod readiness;

pub use completion_backend::{
	AsDescriptor, BackendKind, BorrowedDescriptor, Config, Error, ErrorKind, IoVec,
	OwnedDescriptor, OwnedSocket, Result,
};
pub use executor::{JoinHandle, block_on, init, spawn, spawn_local, timeout};
pub use io::{File, OpenOptions, Outcome};
pub use memory::{AsIoVec, Buffer};
pub use net::{DatagramSocket, Listener, Stream, StreamSocket, ToSockAddr};
pub use readiness::Readiness;
