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
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Once};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use completion_async::{AsIoVec, IoVec};

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

/// Zeroed bytes whose memory reports when the last handle to them drops.
///
/// A test that watches a reactor let go of a buffer cannot count a clone it no
/// longer holds; the memory's own drop is the same fact, seen from the other
/// side.
pub(crate) fn tracked(len: usize) -> (TrackedBuffer, Tracked) {
	let alive = Arc::new(AtomicBool::new(true));
	let inner = TrackedInner {
		bytes: vec![0; len],
		alive: Arc::clone(&alive),
	};
	(TrackedBuffer(Arc::new(inner)), Tracked(alive))
}

/// Whether the memory behind a [`tracked`] buffer is still alive.
pub(crate) struct Tracked(Arc<AtomicBool>);

impl Tracked {
	/// True while any handle to the memory is alive.
	pub(crate) fn alive(&self) -> bool {
		self.0.load(Relaxed)
	}
}

/// A clone-sharing handle on the tracked bytes.
#[derive(Clone)]
pub(crate) struct TrackedBuffer(Arc<TrackedInner>);

/// Plain bytes that note their own end: the last handle dropping is what a test
/// watches to see the reactor let go.
struct TrackedInner {
	bytes: Vec<u8>,
	alive: Arc<AtomicBool>,
}

impl Drop for TrackedInner {
	fn drop(&mut self) {
		self.alive.store(false, Relaxed);
	}
}

// SAFETY: the bytes are owned outright and stay in place for the lifetime of
// the value, so the whole range stays valid and writable.
unsafe impl AsIoVec for TrackedBuffer {
	fn as_iovec(&self) -> IoVec {
		IoVec {
			ptr: self.0.bytes.as_ptr().cast_mut(),
			len: self.0.bytes.len(),
		}
	}
}
