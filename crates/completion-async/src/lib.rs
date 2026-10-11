//! Completion-model async I/O, experimental.
//!
//! [`Executor`] is the start point: use [`Executor::block_on`] to run a
//! root async task.
//!
//! A [`Facade`] is a set of operations over one resource: it holds the resource
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
//! # use completion_async::{Cancel, Facade, Memory};
//! # async fn example(socket: Facade<std::net::TcpStream>, mem: Arc<dyn Memory>, cancel: Cancel) {
//! let outcome = socket.send_all(mem).until(&cancel).await;
//! # }
//! ```

mod error;
mod executor;
mod global;
mod io;
mod net;
mod outcome;
mod submitter;

// The low layer's data types and its descriptor trait, re-exported so a user of
// this crate needs one name for them: the memory, the extents and spans, the
// descriptors, and the cancellation.
pub use completion_rt::{
	AsDescriptor, BorrowedDescriptor, Cancel, Extent, Handle, Memory, OwnedDescriptor, OwnedSocket,
	Span,
};
pub use error::{Error, ErrorKind, Status};
pub use executor::{Config, Executor, Scope, ScopeTask, Spawner, Task};
pub use global::{block_on, scope, spawn, submitter, try_spawn};
pub use io::{Facade, File, IntoMemory, Lent, OpenOptions};
pub use net::{
	Accept, ListenerHandle, Op, Receive, Received, SocketHandle, StreamHandle, Transfer,
};
pub use outcome::Outcome;
pub use submitter::{Submitter, Timeout};
