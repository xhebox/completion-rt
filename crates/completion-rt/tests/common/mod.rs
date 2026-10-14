//! What the completion-rt tests share.
//!
//! Which helpers a binary uses depends on which target's tests it builds —
//! windows compiles the socket tests away — so the module as a whole opts out
//! of the dead-code lint.
#![allow(dead_code)]

use std::future::Future;
use std::io;
use std::panic::Location;
use std::path::{Path, PathBuf};
use std::pin::{Pin, pin};
use std::sync::Once;
use std::task::{Context, Poll, Waker};

use completion_rt::{AsDescriptor, Handle, InFlightState, Op, Reactor, Response, Token};

/// Runs a future to completion on this thread, driving `reactor` while it waits.
///
/// The completions the future waits on land through `reactor`, so `poll(None)`
/// returns once one does, and the future is polled again.
pub(crate) fn block_on<F: Future>(reactor: &Reactor, future: F) -> F::Output {
	init_logging();
	let mut context = Context::from_waker(Waker::noop());
	let mut future = pin!(future);
	loop {
		if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
			return value;
		}
		reactor.poll(None).unwrap();
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
	/// path would carry the depth of the checkout into every path a test hands
	/// the kernel; a unix socket has to fit `sockaddr_un::sun_path`, 104 bytes
	/// including the NUL on macOS.
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

/// A submitted operation, as the tests hold it: resolves to its userdata and
/// its response.
pub(crate) struct Completion<U = ()> {
	token: Token,
	userdata: Option<U>,
}

impl<U> Completion<U> {
	pub(crate) fn tag<V>(self, userdata: V) -> Completion<V> {
		Completion {
			token: self.token,
			userdata: Some(userdata),
		}
	}

	pub(crate) fn cancel(&self) {
		self.token.cancel();
	}
}

impl<U: Unpin> Future for Completion<U> {
	type Output = (U, io::Result<Response>);

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		this.token.poll(cx.waker()).map(|response| {
			let userdata = this
				.userdata
				.take()
				.expect("a completion is not polled after it resolved");
			(userdata, response)
		})
	}
}

/// Submits `op` on `handle`; the completion is attributed to the caller's line.
#[track_caller]
pub(crate) fn submit<S: AsDescriptor + Send + Sync + 'static>(
	reactor: &Reactor,
	handle: &Handle<S>,
	op: Op,
) -> io::Result<Completion> {
	let token = reactor.submit(handle, op, Location::caller(), None)?;
	Ok(Completion {
		token,
		userdata: Some(()),
	})
}

/// Waits on the reactor's event; the completion is attributed to the caller's line.
#[track_caller]
pub(crate) fn wait_event(reactor: &Reactor) -> io::Result<Completion> {
	let token = reactor.wait_event(Location::caller())?;
	Ok(Completion {
		token,
		userdata: Some(()),
	})
}

/// Whether the backend holds nothing: every operation the reactor retains has landed.
pub(crate) fn idle(reactor: &Reactor) -> bool {
	reactor
		.in_flight()
		.iter()
		.all(|entry| entry.state == InFlightState::Done)
}
