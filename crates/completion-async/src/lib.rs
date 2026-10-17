//! Async I/O runtime built on OS completion backends (`io_uring`, IOCP, and
//! `polling`).
//!
//! Run futures on the current thread's default executor with [`block_on`] and
//! [`spawn`], or create a dedicated [`Executor`].
//!
//! Wrap an I/O resource in [`Facade`] to issue async operations. Each operation
//! returns a future ([`Transfer`], [`Receive`], [`Op`], or [`Accept`]) whose
//! available methods depend on the resource type:
//!
//! | Resource | Operations |
//! |---|---|
//! | `std::fs::File` ([`File`]) | `read_at`, `read_exact_at`, `write_at`, `write_all_at`, `sync`, `flush`, `allocate`, `punch` |
//! | Pipes, stdio, `OwnedFd`, `OwnedHandle` | `read_at`, `read_exact_at`, `write_at`, `write_all_at` |
//! | `UdpSocket`, `UnixDatagram` ([`SocketHandle`]) | `recv`, `recv_with_fds`, `send`, `send_with_fds`, `take_error` |
//! | `TcpStream`, `UnixStream` ([`StreamHandle`]) | All [`SocketHandle`] methods, plus `recv_exact`, `recv_exact_with_fds`, `send_all`, `send_all_with_fds`, `shutdown`, `hangup` |
//! | `TcpListener`, `UnixListener` ([`ListenerHandle`]) | `accept` |
//! | [`Signal`] | `wait_readable`, `wait_writable` |
//!
//! Custom socket types can implement [`SocketHandle`], [`StreamHandle`], or
//! [`ListenerHandle`], or convert into a supported resource via [`Into`] when
//! calling [`Facade::new`] / [`Facade::with_executor`].
//!
//! Data transfers operate on [`Span`]s sliced from a [`Buffer`] (use
//! [`Buffer::gather`] for scatter/gather I/O). A [`BufferPool`] hands out
//! pre-faulted, page-aligned buffers for repeated transfers.
//!
//! Attach a [`Cancel`] token to any operation or [`Timeout`] with `.until(&cancel)`
//! to abort it and wait until the kernel releases its buffers:
//!
//! ```no_run
//! # use completion_async::{Buffer, Cancel, Facade};
//! # async fn example(socket: Facade<std::net::TcpStream>, buffer: Buffer, cancel: Cancel) {
//! let outcome = socket.send_all(buffer.span(0, 512)).until(&cancel).await;
//! # }
//! ```

mod completion;
mod executor;
mod global;
mod io;
mod net;
mod op;
mod reactor;
mod signal;
mod timer;

pub use completion::Cancel;
pub use completion_backend::{
	AsDescriptor, BorrowedDescriptor, Buffer, BufferPool, Config, Error, ErrorKind, Extent, IoVec,
	Memory, OwnedDescriptor, OwnedSocket, Span, Status,
};
pub use executor::{Executor, Spawner, Task};
pub use global::{block_on, spawn, try_spawn};
pub use io::{File, OpenOptions};
pub use net::{Accept, ListenerHandle, SocketHandle, StreamHandle};
pub use op::{Facade, Op, Outcome, Receive, Received, Transfer};
pub use signal::Signal;
pub use timer::Timeout;
