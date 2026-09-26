//! The completion mechanism of the target.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use enum_dispatch::enum_dispatch;

use crate::core::Response;
use crate::desc::RawHandle;
use crate::event::Event;
use crate::{Extents, Memory, OwnedDescriptor};

#[cfg(not(any(unix, windows)))]
compile_error!("the completion crate has no backend for this target");

#[cfg(windows)]
mod iocp;
#[cfg(all(unix, not(target_os = "linux")))]
mod portable;
#[cfg(target_os = "linux")]
mod uring;

/// How completions are taken out of the backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PollMode {
	/// The caller waits on [`crate::Reactor::poll_fd`] with its own poller.
	Fd,
	/// The caller calls [`crate::Reactor::poll`] itself.
	Poll,
}

/// Configure the backend.
#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub entries: u32,
	/// Completions appear only while polling (`io_uring` IOPOLL).
	pub iopoll: bool,
	/// A kernel thread consumes submissions (`io_uring` SQPOLL).
	pub sqpoll: bool,
	pub wakeup: PollMode,
}

impl Default for Config {
	fn default() -> Self {
		Config {
			entries: 256,
			iopoll: false,
			sqpoll: false,
			wakeup: PollMode::Poll,
		}
	}
}

impl Config {
	pub(super) fn validate(&self) -> io::Result<()> {
		if self.iopoll && self.wakeup == PollMode::Fd {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"IOPOLL only completes while polling, so it has no poll_fd",
			));
		}
		Ok(())
	}
}

/// What the core needs from a platform's completion mechanism.
///
/// Every operation is a method of its own: its arguments are its own, and no
/// single type stands in for all of them. `id` is the reactor's token — the
/// method keeps it, `poll` reports it back when the operation completes, and
/// `take` uses it to hand the result over.
#[enum_dispatch(BackendEnum)]
pub(super) trait Backend: Send {
	/// Reads the ranges of `memory` named by `extents`, from `fdoff`.
	fn read(
		&mut self,
		id: u64,
		fd: RawHandle,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()>;

	/// Writes the ranges of `memory` named by `extents`, from `fdoff`.
	fn write(
		&mut self,
		id: u64,
		fd: RawHandle,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()>;

	/// Makes everything written to `fd` durable.
	fn fsync(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	/// Receives into the ranges of `memory` named by `extents`.
	fn recv(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()>;

	/// Receives into the ranges of `memory` named by `extents`, with a control
	/// buffer that takes the descriptors that arrive with the bytes.
	fn recv_with_fds(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()>;

	/// Sends the ranges of `memory` named by `extents`.
	fn send(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()>;

	/// Sends the ranges of `memory` named by `extents`, carrying `fds` with the
	/// bytes; they leave with what the one call sends.
	fn send_with_fds(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<()>;

	/// Accepts a connection on the listening socket `fd`.
	fn accept(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	/// Waits for `fd` to become readable.
	fn readable(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	/// Waits for `fd` to become writable.
	fn writable(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	/// Drop an operation nobody is waiting for.
	fn cancel(&mut self, _id: u64) {}

	/// The id of one completed operation.
	///
	/// A backend keeps what one entry into the kernel produced: with a
	/// completion already buffered this reports it without entering again, and
	/// only an empty buffer waits. `WouldBlock` says nothing completed — a
	/// zero timeout means "do not wait for one".
	fn poll(&mut self, timeout: Option<Duration>) -> io::Result<u64>;

	/// Takes what the operation `id` produced, consuming its state.
	fn take(&mut self, id: u64) -> io::Result<Response>;

	/// The wake-up a caller waits on; see [`Reactor::poll_fd`](crate::Reactor::poll_fd).
	fn poll_fd(&self) -> io::Result<Event>;
}

/// The platform's completion mechanism.
#[enum_dispatch]
pub(super) enum BackendEnum {
	#[cfg(target_os = "linux")]
	Uring(uring::Uring),
	#[cfg(windows)]
	Iocp(iocp::Iocp),
	#[cfg(all(unix, not(target_os = "linux")))]
	Portable(portable::Portable),
}

/// The backend the target builds, and the handle its submissions are announced
/// with.
pub(super) fn create(config: &Config) -> io::Result<(BackendEnum, Event)> {
	#[cfg(windows)]
	use iocp::Iocp as BackendImpl;
	#[cfg(all(unix, not(target_os = "linux")))]
	use portable::Portable as BackendImpl;
	#[cfg(target_os = "linux")]
	use uring::Uring as BackendImpl;
	let (backend, event) = BackendImpl::new(config)?;
	Ok((backend.into(), event))
}

/// How this target's backend reports a cancellation: the error an operation
/// stopped on request produces, as against work it did. A racing
/// [`Completion::until`](crate::Completion::until) asks this, so a response that
/// landed anyway keeps its result.
///
/// A plain function rather than a backend method: the reader runs while the
/// core lock may be held by the pass that woke the future.
pub(super) fn cancelled() -> fn(&io::Error) -> bool {
	#[cfg(windows)]
	{
		return iocp::cancelled;
	}
	#[cfg(all(unix, not(target_os = "linux")))]
	{
		return portable::cancelled;
	}
	#[cfg(target_os = "linux")]
	{
		return uring::cancelled;
	}
}
