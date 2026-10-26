//! Async I/O runtime built on OS completion backends (`io_uring`, IOCP, and
//! [`polling`](https://github.com/smol-rs/polling)).
//!
//! Run futures on the current thread's runtime with [`block_on`] and
//! [`spawn`]. The runtime is created with [`Config::default`] on first use, or
//! from a custom [`Config`] by calling [`init`] first. To `.await` this
//! crate's I/O futures inside another runtime (such as `tokio` or `smol`),
//! call [`init_reactor`] once at startup: it spawns a dedicated background
//! reactor thread that is always preferred over thread-local runtimes.
//!
//! A resource is wrapped in the handle whose operations it takes:
//!
//! | Resource | Operations |
//! |---|---|
//! | [`File`] | `read_at`, `read_exact_at`, `write_at`, `write_all_at`, `sync`, `flush`, `allocate`, `punch` |
//! | [`Stream`] (pipes and byte streams) | `pipe`, `read`, `read_exact`, `write`, `write_all`, `readable`, `writable` |
//! | [`StreamSocket`] (stream sockets) | `recv`, `recv_exact`, `send`, `send_all`, `shutdown`, `take_error`, `hangup`, `readable`, `writable` |
//! | [`DatagramSocket`] (datagram sockets) | `recv`, `send`, `take_error`, `readable`, `writable` |
//! | [`Listener`] (listening sockets) | `accept` |
//!
//! A single call may transfer fewer bytes than requested and returns the byte
//! count; `_exact` and `_all` repeat until the [`AsIoVec`] is full.
//!
//! Each handle is built from a standard resource (`impl Into<std::fs::File>`,
//! `impl Into<OwnedDescriptor>`, `impl Into<OwnedSocket>`), is cheap to clone,
//! and is `Send + Sync`. It binds no reactor: each operation resolves the
//! reactor when first polled — the background reactor if [`init_reactor`] was
//! called, or the runtime installed on the polling thread otherwise.
//! `try_take` returns the underlying resource once no clones or in-flight
//! operations remain.
//!
//! Data transfers take an [`AsIoVec`] by value. [`Buffer`] implements it; a
//! clone is a cheap handle on the same bytes, so to read them after a transfer,
//! pass `buffer.clone()` and keep the original.
//!
//! Dropping an operation's future cancels and detaches it: the reactor keeps
//! the buffer and descriptor alive until the kernel finishes. For a
//! [`File`] this is safe — re-read from the same offset. For a [`Stream`],
//! [`StreamSocket`] or [`DatagramSocket`], bytes the kernel already consumed
//! are lost. Use `readable`
//! to wait for readability, then read synchronously, when cancellation safety
//! matters.
//!
//! # Platform notes
//!
//! - Linux `io_uring` requires kernel 5.11 or newer ([`StreamSocket::hangup`]
//!   requires 5.13+). On 6.15+, the `completion-backend/iowait` feature
//!   re-enables counting idle ring waits as system iowait.
//! - On Windows with the IOCP backend, handles passed to [`File::new`] and
//!   [`Stream::new`] must be opened with `FILE_FLAG_OVERLAPPED` (use
//!   [`OpenOptions`] and [`Stream::pipe`], which set it automatically).

#[cfg(feature = "compat")]
mod compat;
mod executor;
mod io;
mod memory;
mod net;
mod reactor;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "compat")]
pub use compat::PollSocket;
pub use completion_backend::{
	AsDescriptor, BackendKind, BorrowedDescriptor, Config, Error, ErrorKind, IoVec, Op,
	OwnedDescriptor, OwnedSocket, Response, Result,
};
pub use executor::{block_on, init, spawn, timeout, yield_now};
pub use io::{File, OpenOptions};
pub use memory::{AsIoVec, Buffer};
pub use net::{DatagramSocket, Listener, Stream, StreamSocket, ToSockAddr};
pub use reactor::init_reactor;

pub(crate) enum Slot<D: Send + 'static, M: Send + 'static> {
	Local(executor::LocalSlot<D, M>),
	Remote(reactor::RemoteSlot<D, M>),
}

impl<D: Send + 'static, M: Send + 'static> Future for Slot<D, M> {
	type Output = (Result<Response>, D, M);

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		// SAFETY: `local` is structurally pinned inside `Slot`.
		match unsafe { self.get_unchecked_mut() } {
			Slot::Local(local) => unsafe { Pin::new_unchecked(local) }.poll(cx),
			Slot::Remote(remote) => Pin::new(remote).poll(cx),
		}
	}
}

pub(crate) fn submit<D: AsDescriptor + 'static, M: Send + 'static>(
	fd: D,
	op: Op,
	memory: M,
) -> Result<Slot<D, M>> {
	if reactor::is_installed() {
		Ok(Slot::Remote(reactor::submit(fd, op, memory)))
	} else {
		executor::submit(fd, op, memory).map(Slot::Local)
	}
}
