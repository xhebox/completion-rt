//! The completion mechanism of the target.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use enum_dispatch::enum_dispatch;

use crate::OwnedDescriptor;
use crate::desc::OwnedSocket;
use crate::desc::RawHandle;
use crate::error::Error;
use crate::memory::IoVecs;

#[cfg(not(any(unix, windows)))]
compile_error!("the completion crate has no backend for this target");

mod park;

use park::Doorbell;
pub(crate) use park::Park;

#[cfg(windows)]
mod iocp;
#[cfg(all(unix, not(target_os = "linux")))]
mod portable;
#[cfg(target_os = "linux")]
mod uring;

/// Configure the backend.
#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub entries: u32,
	/// Completions appear only while polling (`io_uring` IOPOLL).
	pub iopoll: bool,
	/// A kernel thread consumes submissions (`io_uring` SQPOLL).
	pub sqpoll: bool,
}

impl Default for Config {
	fn default() -> Self {
		Config {
			entries: 256,
			iopoll: false,
			sqpoll: false,
		}
	}
}

/// Completed operation result.
#[derive(Debug)]
pub enum Response {
	/// The bytes a transfer moved.
	Count(usize),
	/// A receive that also brought descriptors.
	CountWithFds(usize, Vec<OwnedDescriptor>),
	/// The socket an accept produced; a response that is never taken closes it.
	Accepted(OwnedSocket),
	/// A sync or a wait: the completion itself is all of it.
	Done,
}

impl Response {
	/// Returns the transferred byte count, closing any received descriptors; other response variants return an error.
	pub fn count(self) -> Result<usize, Error> {
		match self {
			Response::Count(count) | Response::CountWithFds(count, _) => Ok(count),
			Response::Accepted(_) => Err(Error::from(io::Error::other(
				"an accept reported a socket, not a count",
			))),
			Response::Done => Err(Error::from(io::Error::other(
				"a transfer reported no count",
			))),
		}
	}
}

#[enum_dispatch(BackendEnum)]
pub(super) trait Backend {
	fn read(&mut self, id: u64, fd: RawHandle, fdoff: u64, buffers: IoVecs) -> io::Result<()>;

	fn write(&mut self, id: u64, fd: RawHandle, fdoff: u64, buffers: IoVecs) -> io::Result<()>;

	fn fsync(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	fn flush(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	fn allocate(&mut self, id: u64, fd: RawHandle, offset: u64, len: u64) -> io::Result<()>;

	fn punch(&mut self, id: u64, fd: RawHandle, offset: u64, len: u64) -> io::Result<()>;

	fn recv(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()>;

	fn recv_with_fds(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()>;

	fn send(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()>;

	fn send_with_fds(
		&mut self,
		id: u64,
		fd: RawHandle,
		buffers: IoVecs,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<()>;

	fn accept(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	fn readable(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	fn writable(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	fn hangup(&mut self, id: u64, fd: RawHandle) -> io::Result<()>;

	fn cancel(&mut self, _id: u64) {}

	/// The id of one completed operation.
	///
	/// A backend keeps what one entry into the kernel produced: with a
	/// completion already buffered this reports it without entering again, and
	/// only an empty buffer waits. `WouldBlock` says nothing completed — a
	/// zero timeout means "do not wait for one".
	///
	/// `park` carries the reactor's wake state; a backend that spins rather than
	/// waits (IOPOLL) uses it to see a cross-thread wake.
	fn poll(&mut self, timeout: Option<Duration>, park: &Park) -> io::Result<u64>;

	fn take(&mut self, id: u64) -> Result<Response, Error>;

	/// What wakes a park asleep in this backend's [`poll`](Backend::poll).
	fn doorbell(&self) -> Arc<dyn Doorbell>;
}

#[enum_dispatch]
pub(super) enum BackendEnum {
	#[cfg(target_os = "linux")]
	Uring(uring::Uring),
	#[cfg(windows)]
	Iocp(iocp::Iocp),
	#[cfg(all(unix, not(target_os = "linux")))]
	Portable(portable::Portable),
}

pub(super) fn create(config: &Config) -> io::Result<BackendEnum> {
	#[cfg(windows)]
	use iocp::Iocp as BackendImpl;
	#[cfg(all(unix, not(target_os = "linux")))]
	use portable::Portable as BackendImpl;
	#[cfg(target_os = "linux")]
	use uring::Uring as BackendImpl;
	Ok(BackendImpl::new(config)?.into())
}
