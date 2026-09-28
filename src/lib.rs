//! Completion-model async I/O, experimental.
//!
//! A [`Reactor`] owns one completion backend and hands out a [`Submitter`] that
//! any thread submits an operation through — `read`, `send`, `accept`, and the
//! rest — and the returned [`Completion`] resolves when the completion arrives,
//! yielding what that operation produces. Dropping a `Completion` without
//! awaiting cancels the operation and returns only once the backend has let go
//! of the memory it was given. An operation still in flight that named the
//! caller's memory is a bug: the drop has to wait on it, panicking in a debug
//! build and logging in a release one.
//!
//! A [`Facade`] is the same operations over one resource: it holds the resource
//! in a [`Handle`], so an operation that is still in flight keeps it open, and
//! it names the operation as a method. Each method returns a future — a
//! [`Transfer`], a [`Receive`], an [`Op`], or an [`Accept`] — which resolves to
//! what the operation got through, and whether it finished: an [`Outcome`] for a
//! transfer, a [`Received`] for a receive that asked for control data, a plain
//! `Result` for the rest. Awaiting a `Transfer` that the kernel answered short
//! gives the count; the loop over the calls — `read_exact_at`, `send_all` and
//! their like — gives what the loop moved before it stopped, so a failure or a
//! cancel does not hide the progress behind it.
//!
//! An operation is given a cancel with the [`until`](Transfer::until) on its
//! future, which then yields [`Error::Cancelled`] if the cancel wins:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use completion_rt::{Cancel, Facade, Memory};
//! # async fn example(socket: Facade<std::net::TcpStream>, mem: Arc<dyn Memory>, cancel: Cancel) {
//! let outcome = socket.send_all(mem).until(&cancel).await;
//! # }
//! ```
//!
//! Nothing polls the backend by itself: an [`Executor`] runs the ready tasks
//! and then waits on the reactor, or a caller drives it with [`Reactor::poll`]
//! instead, directly or when its [`Reactor::poll_fd`] becomes ready. A
//! [`Submitter::timeout`] is a timer in the reactor's own table, and a wait is
//! capped at the nearest one.

mod backend;
mod channel;
mod core;
mod desc;
mod event;
mod executor;
mod flow;
mod io;
mod memory;
mod net;
mod outcome;
mod tasks;

pub use backend::{Config, PollMode};
pub use channel::{Closed, Receiver, Sender};
pub use core::{Cancel, Cancelled, Completion, Handle, Reactor, Response, Submitter, Until};
pub use desc::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
pub use event::Event;
pub use executor::{Executor, Spawner};
pub use flow::{Accept, Op, Receive, Transfer};
pub use io::{Facade, File, OpenOptions};
pub use memory::{Extent, Extents, Memory, Span};
pub use net::{ListenerHandle, SocketHandle, StreamHandle};
pub use outcome::{Error, Outcome, Received};
pub use tasks::Tasks;
