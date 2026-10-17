//! What the completion-backend tests share.
//!
//! Which helpers a binary uses depends on which target's tests it builds —
//! windows compiles the socket tests away — so the module as a whole opts out
//! of the dead-code lint.
#![allow(dead_code)]

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Once};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

/// Runs a future to completion on this thread, woken by whoever completes it.
///
/// These tests drive their reactor on a thread of its own, so the crate's
/// executor — which polls a reactor it owns — is not what can wait for them:
/// this parks until the completion's waker unparks it.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
	init_logging();

	struct Unpark(Thread);

	impl Wake for Unpark {
		fn wake(self: Arc<Self>) {
			self.0.unpark();
		}
	}

	let waker = Waker::from(Arc::new(Unpark(thread::current())));
	let mut context = Context::from_waker(&waker);
	let mut future = pin!(future);
	loop {
		if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
			return value;
		}
		thread::park();
	}
}

/// Installs the logger, filtered by `RUST_LOG`.
///
/// A test binary that never installs one drops every record: the library
/// depends on the `log` facade alone. Idempotent, and a logger installed by
/// something else is left alone. The helpers below reach it, so a test that
/// waits on a completion has its records whether or not it asks.
pub(crate) fn init_logging() {
	static INIT: Once = Once::new();
	INIT.call_once(|| {
		let _ = env_logger::builder().is_test(true).try_init();
	});
}

/// A unique scratch directory for one test, below the crate's
/// `target/test-tmp` (`COMPLETION_RT_TEST_TMP` overrides the root). `tag` names
/// the call site and becomes the directory name prefix; tempfile appends a
/// random suffix, so parallel and repeated runs never collide.
pub(crate) struct Scratch {
	/// Owns the directory: dropping the `Scratch` removes it.
	dir: tempfile::TempDir,
	path: PathBuf,
}

impl Scratch {
	/// A path to the directory, relative to the crate root when the scratch
	/// root is — which is where cargo runs a test binary. tempfile makes the
	/// root absolute on its way to creating the directory, and that absolute
	/// path would carry the depth of the checkout (and of
	/// `target/package/<crate>-<version>` when the packed crate is tested) into
	/// every path a test hands the kernel; a unix socket has to fit
	/// `sockaddr_un::sun_path`, 104 bytes including the NUL on macOS.
	pub(crate) fn path(&self) -> &Path {
		&self.path
	}
}

/// Opens `path` read-only, in the shape the backend needs: a completion port
/// only ever owns a handle opened for overlapped I/O, and `File::open` does not
/// ask for that on Windows.
pub(crate) fn open(path: impl AsRef<Path>) -> io::Result<std::fs::File> {
	let path = path.as_ref();
	#[cfg(windows)]
	{
		use std::os::windows::fs::OpenOptionsExt;
		use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;
		std::fs::OpenOptions::new()
			.read(true)
			.custom_flags(FILE_FLAG_OVERLAPPED)
			.open(path)
	}
	#[cfg(not(windows))]
	{
		std::fs::File::open(path)
	}
}

/// Holds both of a socket's buffers to `bytes`, so a payload larger than them
/// cannot all leave at once whatever the platform's default buffering is. A
/// test that needs a send still in front of the kernel sets it on both ends,
/// because either end's buffer would otherwise take the whole payload.
pub(crate) fn cap_buffers(socket: &std::net::TcpStream, bytes: usize) {
	#[cfg(unix)]
	{
		let _ = rustix::net::sockopt::set_socket_send_buffer_size(socket, bytes);
		let _ = rustix::net::sockopt::set_socket_recv_buffer_size(socket, bytes);
	}
	#[cfg(windows)]
	{
		use std::mem::size_of;
		use std::os::windows::io::AsRawSocket;
		use windows_sys::Win32::Networking::WinSock::{
			SO_RCVBUF, SO_SNDBUF, SOCKET, SOL_SOCKET, setsockopt,
		};
		let value = bytes as i32;
		let socket = socket.as_raw_socket() as SOCKET;
		for option in [SO_SNDBUF, SO_RCVBUF] {
			// SAFETY: `value` is the `int` both options take, and its size is
			// what the call is told to read.
			unsafe {
				setsockopt(
					socket,
					SOL_SOCKET,
					option,
					&value as *const i32 as *const u8,
					size_of::<i32>() as i32,
				);
			}
		}
	}
}

/// Makes a scratch directory below `target/test-tmp`. Remove it by dropping the
/// returned `Scratch`, so bind it for the lifetime of the test.
pub(crate) fn tempdir(tag: &str) -> io::Result<Scratch> {
	init_logging();
	let base = std::env::var_os("COMPLETION_RT_TEST_TMP")
		.map(PathBuf::from)
		.unwrap_or_else(|| PathBuf::from("target/test-tmp"));
	std::fs::create_dir_all(&base)?;
	let dir = tempfile::Builder::new().prefix(tag).tempdir_in(&base)?;
	let name = dir
		.path()
		.file_name()
		.expect("tempfile names the directory");
	Ok(Scratch {
		path: base.join(name),
		dir,
	})
}

/// Bytes a test binds to a space. Atomic, so the backend may write them through
/// an iovec while the buffer is shared.
pub(crate) struct Buffer(std::sync::Arc<[std::sync::atomic::AtomicU8]>);

impl Buffer {
	/// `len` zeroed bytes.
	pub(crate) fn new(len: usize) -> Buffer {
		Buffer::from_slice(&vec![0; len])
	}

	pub(crate) fn from_slice(bytes: &[u8]) -> Buffer {
		Buffer(
			bytes
				.iter()
				.map(|&byte| std::sync::atomic::AtomicU8::new(byte))
				.collect(),
		)
	}

	pub(crate) fn len(&self) -> usize {
		self.0.len()
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Copies `out.len()` bytes at `offset` out of the buffer.
	pub(crate) fn copy_to(&self, offset: usize, out: &mut [u8]) {
		for (index, byte) in out.iter_mut().enumerate() {
			*byte = self.0[offset + index].load(Relaxed);
		}
	}

	/// Copies `bytes` into the buffer at `offset`.
	pub(crate) fn copy_from(&self, offset: usize, bytes: &[u8]) {
		for (index, byte) in bytes.iter().enumerate() {
			self.0[offset + index].store(*byte, Relaxed);
		}
	}

	pub(crate) fn to_vec(&self) -> Vec<u8> {
		self.0.iter().map(|byte| byte.load(Relaxed)).collect()
	}
}

// SAFETY: the bytes are atomics, so a write through the pointer races on bytes
// only, never on a reference; `iovec` checks the extent against the length.
unsafe impl completion_async::Memory for Buffer {
	fn len(&self) -> usize {
		self.0.len()
	}

	fn iovec(&self, extent: completion_async::Extent) -> io::Result<completion_async::IoVec> {
		let end = usize::try_from(extent.offset)
			.ok()
			.and_then(|offset| offset.checked_add(extent.len));
		if !end.is_some_and(|end| end <= self.0.len()) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"range outside the buffer",
			));
		}
		let base = self.0.as_ptr() as *mut u8;
		// SAFETY: in bounds, checked above.
		Ok(unsafe { completion_async::IoVec::new(base.add(extent.offset as usize), extent.len) })
	}
}
