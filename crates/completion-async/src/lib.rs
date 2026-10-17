//! Completion-model async I/O, experimental.
//!
//! [`Executor`] is the start point: use [`block_on`] from the global executor to
//! run a root async task.
//!
//! A [`Facade`] provides a set of methods over an owned resource behind an
//! `Arc`. Each method returns a future: [`Transfer`], [`Receive`], [`Op`], or [`Accept`],
//!  which resolves to [`Outcome`], [`Received`], plain `Result`, whatever.
//!
//! Which methods a facade has follows from its resource type:
//!
//! | Resource | Operations |
//! |---|---|
//! | `File` | `read_at`, `read_exact_at`, `write_at`, `write_all_at`, `sync`, `flush`, `allocate`, `punch` |
//! | pipes, stdio, `OwnedFd` | the four read and write methods of a file, and no more |
//! | `UdpSocket`, `UnixDatagram` | `recv`, `recv_with_fds`, `send`, `send_with_fds`, `take_error` |
//! | `TcpStream`, `UnixStream` | a datagram's five, plus `recv_exact`, `recv_exact_with_fds`, `send_all`, `send_all_with_fds`, `shutdown`, `hangup` |
//! | `TcpListener`, `UnixListener` | `accept` |
//! | [`Signal`] | `wait_readable`, `wait_writable` |
//!
//! The four a file and a descriptor share are the reads `read_at` and
//! `read_exact_at`, and the writes `write_at` and `write_all_at`.
//!
//! A transfer names its memory as one [`Span`]: ranges of a [`Space`].
//! [`Space::gather`] makes one span of several ranges.
//!
//! A readiness wait is only [`Signal`]'s. A resource with a transfer has none
//! to offer: the backend arms the readiness itself when a call would block, and
//! the completion reports how much moved. A stream's `hangup` is not readiness
//! for a transfer but the end of the connection, which no transfer reports
//! while the stream is idle.
//!
//! A resource type of the caller's own gets the same methods by converting into
//! one of these: [`Facade::new`] takes a value that converts into the resource
//! the facade names.
//!
//! An operation is given a cancel with the [`until`](Transfer::until) on its
//! future, which then yields [`ErrorKind::Cancelled`] if the cancel wins:
//!
//! ```no_run
//! # use completion_async::{Cancel, Facade, Space};
//! # async fn example(socket: Facade<std::net::TcpStream>, space: Space, cancel: Cancel) {
//! let outcome = socket.send_all(space.span(0, 512)).until(&cancel).await;
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

// The low layer's data types and its descriptor trait, re-exported so a user of
// this crate needs one name for them: the memory, the extents and iovecs, and
// the descriptors.
pub use completion::Cancel;
pub use completion_rt::{
	AsDescriptor, BorrowedDescriptor, Config, Error, ErrorKind, Extent, IoVec, Memory,
	OwnedDescriptor, OwnedSocket, Space, Span, Status,
};
pub use executor::{Executor, Spawner, Task};
pub use global::{block_on, spawn, try_spawn};
pub use io::{File, OpenOptions};
pub use net::{Accept, ListenerHandle, SocketHandle, StreamHandle};
pub use op::{Facade, Op, Outcome, Receive, Received, Transfer};
pub use signal::Signal;
pub use timer::Timeout;
