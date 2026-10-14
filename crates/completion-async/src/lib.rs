//! Completion-model async I/O, experimental.
//!
//! [`Executor`] is the start point: use [`block_on`] from the global executor to
//! run a root async task.
//!
//! A [`Facade`] provides a set of methods over owned resource in a [`Handle`].
//! Each method returns a future: [`Transfer`], [`Receive`], [`Op`], or [`Accept`],
//!  which resolves to [`Outcome`], [`Received`], plain `Result`, whatever.
//!
//! Which methods a facade has follows from its resource's [`Kind`]:
//!
//! | Kind | Resources | Operations |
//! |---|---|---|
//! | [`kind::File`] | `File` | `read_at`, `read_vectored_at`, `read_exact_at`, `read_exact_vectored_at`, `write_at`, `write_vectored_at`, `write_all_at`, `write_all_vectored_at`, `sync`, `flush`, `allocate`, `punch` |
//! | [`kind::Descriptor`] | pipes, stdio, `OwnedFd` | the eight read and write methods of a file |
//! | [`kind::Datagram`] | `UdpSocket`, `UnixDatagram` | `recv`, `recv_vectored`, `recv_with_fds`, `send`, `send_vectored`, `send_with_fds`, `take_error` |
//! | [`kind::Stream`] | `TcpStream`, `UnixStream` | a datagram's seven, plus `recv_exact`, `recv_exact_vectored`, `recv_exact_with_fds`, `send_all`, `send_all_vectored`, `send_all_with_fds`, `shutdown` |
//! | [`kind::Listener`] | `TcpListener`, `UnixListener` | `accept` |
//! | [`kind::Signal`] | [`Signal`] | `wait_readable`, `wait_writable` |
//!
//! The eight a file and a descriptor share are the four reads — `read_at`,
//! `read_vectored_at`, `read_exact_at`, `read_exact_vectored_at` — and the four
//! writes, `write_at`, `write_vectored_at`, `write_all_at`,
//! `write_all_vectored_at`.
//!
//! A wait is only [`Signal`]'s. A resource with a transfer has none to offer:
//! the backend arms the readiness itself when a call would block, and the
//! completion reports how much moved.
//!
//! An operation is given a cancel with the [`until`](Transfer::until) on its
//! future, which then yields [`ErrorKind::Cancelled`] if the cancel wins:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use completion_async::{Cancel, Facade, Memory};
//! # async fn example(socket: Facade<std::net::TcpStream>, mem: Arc<dyn Memory>, cancel: Cancel) {
//! let outcome = socket.send_all(mem).until(&cancel).await;
//! # }
//! ```

mod completion;
mod error;
mod executor;
mod global;
mod io;
pub mod kind;
mod net;
mod outcome;
mod signal;
mod timer;

// The low layer's data types and its descriptor trait, re-exported so a user of
// this crate needs one name for them: the memory, the extents and spans, the
// descriptors, and the cancellation.
pub use completion::Cancel;
pub use completion_rt::{
	AsDescriptor, BorrowedDescriptor, Extent, Handle, Memory, OwnedDescriptor, OwnedSocket, Span,
};
pub use error::{Error, ErrorKind, Status};
pub use executor::{Config, Executor, Scope, ScopeTask, Spawner, Task};
pub use global::{block_on, scope, spawn, try_spawn};
pub use io::{Facade, File, IntoMemory, Lent, OpenOptions};
pub use kind::{Kind, ListenerHandle, Positional, Resource, SocketKind};
pub use net::{Accept, Op, Receive, Received, Transfer};
pub use outcome::Outcome;
pub use signal::Signal;
pub use timer::Timeout;
