use std::sync::Arc;
use std::time::Duration;

use enum_dispatch::enum_dispatch;
use socket2::SockAddr;

use crate::desc::{BorrowedDescriptor, OwnedSocket};
use crate::error::Result;

#[cfg(not(any(unix, windows)))]
compile_error!("completion-backend does not support this target");

#[cfg(windows)]
mod iocp;
mod portable;
#[cfg(target_os = "linux")]
mod uring;

/// Configuration for [`Backend::new`].
#[derive(Clone, Copy, Debug)]
pub struct Config {
	/// Submission queue capacity (`io_uring` only).
	pub entries: u32,
	/// Poll the kernel for completions instead of sleeping on interrupts (`io_uring` `IORING_SETUP_IOPOLL`).
	pub iopoll: bool,
	/// Let a kernel thread consume the submission ring (`io_uring` `IORING_SETUP_SQPOLL`).
	pub sqpoll: bool,
	/// Completion mechanism to use; [`BackendKind::default`] selects the
	/// platform default.
	pub backend: BackendKind,
}

impl Default for Config {
	fn default() -> Self {
		Config {
			entries: 256,
			iopoll: false,
			sqpoll: false,
			backend: BackendKind::default(),
		}
	}
}

/// Resolved pointer and length for a byte transfer.
#[derive(Clone, Copy, Debug)]
pub struct IoVec {
	/// Pointer to the first byte of the range.
	pub ptr: *mut u8,
	/// Byte length of the range.
	pub len: usize,
}

// SAFETY: `IoVec` is a raw pointer into memory kept alive by the caller for
// the duration of an I/O operation.
unsafe impl Send for IoVec {}

/// An I/O operation submitted to a [`Backend`].
pub enum Op {
	/// Reads into `buf` at `offset` (or at the descriptor's current position when `None`).
	Read { offset: Option<u64>, buf: IoVec },
	/// Writes from `buf` at `offset` (or at the descriptor's current position when `None`).
	Write { offset: Option<u64>, buf: IoVec },
	/// Flushes all modified data and metadata to disk (`fsync`).
	Fsync,
	/// Checks for deferred write-back errors without forcing data to disk.
	Flush,
	/// Reserves `len` bytes from `offset`, extending the file when the range ends
	/// past its length.
	Allocate { offset: u64, len: u64 },
	/// Deallocates `len` bytes from `offset`, zeroing the range and keeping the
	/// file's length.
	Punch { offset: u64, len: u64 },
	/// Receives into `buf`.
	Recv { buf: IoVec },
	/// Sends from `buf`.
	Send { buf: IoVec },
	/// Accepts the next connection on a listening socket.
	Accept,
	/// Connects a socket to `addr`.
	Connect { addr: SockAddr },
	/// Waits until the descriptor becomes readable.
	Readable,
	/// Waits until the descriptor becomes writable.
	Writable,
	/// Waits until the peer closes both directions or the connection resets.
	Hangup,
}

/// Output of a completed [`Op`], reported by [`Backend::poll`].
#[derive(Debug)]
pub enum Response {
	/// Number of bytes transferred.
	Count(usize),
	/// Accepted connection socket.
	Accepted(OwnedSocket),
	/// Operation completed without a return value (`Fsync`, `Flush`, `Allocate`, `Punch`, `Connect`, `Readable`, `Writable`, `Hangup`).
	Done,
}

/// Thread-safe handle that interrupts a blocking [`Backend::poll`].
#[derive(Clone)]
pub struct Waker(WakerInner);

#[derive(Clone)]
enum WakerInner {
	Portable(Arc<portable::PollerWake>),
	#[cfg(target_os = "linux")]
	Uring(Arc<uring::Signal>),
	#[cfg(windows)]
	Iocp(Arc<iocp::Port>),
}

impl Waker {
	/// Wakes a blocking [`Backend::poll`].
	pub fn wake(&self) {
		match &self.0 {
			WakerInner::Portable(poller) => poller.wake(),
			#[cfg(target_os = "linux")]
			WakerInner::Uring(signal) => signal.wake(),
			#[cfg(windows)]
			WakerInner::Iocp(port) => port.wake(),
		}
	}
}

#[enum_dispatch]
trait Driver<T> {
	fn submit(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64>;
	fn get_mut(&mut self, id: u64) -> Option<&mut T>;
	fn cancel(&mut self, id: u64);
	fn cancel_all(&mut self);
	fn is_idle(&self) -> bool;
	fn poll(
		&mut self,
		timeout: Option<Duration>,
		complete: impl FnMut(u64, T, Result<Response>),
	) -> Result<()>;
	fn waker(&self) -> Waker;
}

/// Platform completion driver.
///
/// Once [`Backend::submit`] returns `Ok(id)`, the caller must keep the
/// descriptor and any [`IoVec`] memory referenced by the operation alive until
/// [`Backend::poll`] yields a completion for `id`. Operation IDs carry a
/// generation, so cancelling an ID whose operation already completed is a
/// no-op.
#[enum_dispatch(Driver<T>)]
#[derive(strum::EnumDiscriminants)]
#[strum_discriminants(
	name(BackendKind),
	doc = "Completion mechanism used by a [`Backend`].",
	derive(
		Default,
		Hash,
		strum::EnumString,
		strum::Display,
		strum::IntoStaticStr,
		strum::VariantArray,
	),
	strum(serialize_all = "lowercase", ascii_case_insensitive)
)]
pub enum Backend<T = ()> {
	/// Readiness polling ([`polling`](https://github.com/smol-rs/polling)) with
	/// file I/O on a [`blocking`](https://github.com/smol-rs/blocking) thread pool.
	#[strum_discriminants(cfg_attr(all(unix, not(target_os = "linux")), default))]
	Portable(portable::Portable<T>),
	/// Linux `io_uring`.
	#[cfg(target_os = "linux")]
	#[strum_discriminants(default)]
	Uring(uring::Uring<T>),
	/// Windows I/O completion port (IOCP).
	#[cfg(windows)]
	#[strum_discriminants(default)]
	Iocp(iocp::Iocp<T>),
}

impl<T> Backend<T> {
	/// Creates a new completion backend with `config`.
	pub fn new(config: &Config) -> Result<Self> {
		let backend = match config.backend {
			BackendKind::Portable => portable::Portable::new(config)?.into(),
			#[cfg(target_os = "linux")]
			BackendKind::Uring => uring::Uring::new(config)?.into(),
			#[cfg(windows)]
			BackendKind::Iocp => iocp::Iocp::new(config)?.into(),
		};
		Ok(backend)
	}

	/// Returns the completion mechanism this backend runs on.
	pub fn kind(&self) -> BackendKind {
		self.into()
	}

	/// Submits `op` on `fd`, and returns the id its completion reports under.
	///
	/// If this returns `Err`, the operation was not queued and no completion
	/// will be produced for it.
	pub fn submit(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64> {
		Driver::submit(self, fd, op, data)
	}

	/// Returns a mutable reference to the caller data associated with `id` if
	/// the operation is still tracked by the backend.
	pub fn get_mut(&mut self, id: u64) -> Option<&mut T> {
		Driver::get_mut(self, id)
	}

	/// Requests asynchronous cancellation of the in-flight operation `id`.
	///
	/// The operation's final result (cancelled or completed) is still delivered
	/// by [`Backend::poll`].
	pub fn cancel(&mut self, id: u64) {
		Driver::cancel(self, id)
	}

	/// Requests cancellation of every operation currently in flight.
	///
	/// As with [`Backend::cancel`], each operation's final result is still
	/// delivered by [`Backend::poll`].
	pub fn cancel_all(&mut self) {
		Driver::cancel_all(self)
	}

	/// Returns `true` if no operation is in flight and no completion is waiting
	/// to be handed over.
	pub fn is_idle(&self) -> bool {
		Driver::is_idle(self)
	}

	/// Polls for completed operations, waiting up to `timeout` if none are ready
	/// (`Some(Duration::ZERO)` never blocks; `None` waits indefinitely), and invokes
	/// `complete(id, data, result)` for each completion.
	pub fn poll(
		&mut self,
		timeout: Option<Duration>,
		complete: impl FnMut(u64, T, Result<Response>),
	) -> Result<()> {
		Driver::poll(self, timeout, complete)
	}

	/// Returns a thread-safe [`Waker`] that wakes a blocking [`Backend::poll`].
	pub fn waker(&self) -> Waker {
		Driver::waker(self)
	}
}

#[cfg(test)]
mod tests {
	use assert2::assert;

	use super::{Backend, BackendKind, Config};

	/// The default is the target's own completion mechanism: the ring where there
	/// is one, IOCP on Windows, the portable backend on the other unix targets.
	#[test]
	fn a_kind_defaults_to_the_targets_own_backend() {
		#[cfg(target_os = "linux")]
		assert!(BackendKind::default() == BackendKind::Uring);
		#[cfg(all(unix, not(target_os = "linux")))]
		assert!(BackendKind::default() == BackendKind::Portable);
		#[cfg(windows)]
		assert!(BackendKind::default() == BackendKind::Iocp);

		let backend: Backend = Backend::new(&Config::default()).unwrap();
		assert!(backend.kind() == BackendKind::default());
	}

	/// Every variant a target offers round-trips through its name, whatever case
	/// the name arrives in.
	#[test]
	fn a_kind_round_trips_through_its_name() {
		use strum::VariantArray;

		for &kind in BackendKind::VARIANTS {
			let name = kind.to_string();
			assert!(name.parse::<BackendKind>().unwrap() == kind);
			assert!(name.to_uppercase().parse::<BackendKind>().unwrap() == kind);
			let static_name: &'static str = kind.into();
			assert!(static_name == name);
		}

		assert!("portableish".parse::<BackendKind>().is_err());
	}

	/// Every target offers the portable backend, whether or not it is the default:
	/// on Linux it swaps the ring for `polling`'s readiness and `blocking`'s pool,
	/// and on Windows it swaps IOCP for them.
	#[test]
	fn every_target_offers_the_portable_backend() {
		let config = Config {
			backend: "portable".parse().unwrap(),
			..Config::default()
		};
		assert!(Backend::<()>::new(&config).unwrap().kind() == BackendKind::Portable);
		#[cfg(any(target_os = "linux", windows))]
		assert!(BackendKind::default() != BackendKind::Portable);
	}

	/// A backend hands its mechanism to whichever thread drives it: the reactor
	/// moves one between threads, so every mechanism it can pick is `Send`.
	#[test]
	fn a_backend_is_send() {
		fn assert_send<T: Send>() {}
		assert_send::<Backend>();
	}
}
