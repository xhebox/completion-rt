//! What the completion-rt tests share.
//!
//! Which helpers a binary uses depends on which target's tests it builds —
//! windows compiles the socket tests away — so the module as a whole opts out
//! of the dead-code lint.
#![allow(dead_code)]

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Once};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

/// Runs a future to completion on this thread, woken by whoever completes it.
///
/// These tests drive their reactor on a thread of its own, so the crate's
/// executor — which polls a reactor it owns — is not what can wait for them:
/// this parks until the completion's waker unparks it.
pub fn block_on<F: Future>(future: F) -> F::Output {
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
pub fn init_logging() {
	static INIT: Once = Once::new();
	INIT.call_once(|| {
		let _ = env_logger::builder().is_test(true).try_init();
	});
}

/// A unique scratch directory for one test, below the crate's
/// `target/test-tmp` (`COMPLETION_RT_TEST_TMP` overrides the root). `tag` names
/// the call site and becomes the directory name prefix; tempfile appends a
/// random suffix, so parallel and repeated runs never collide.
pub struct Scratch {
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
	pub fn path(&self) -> &Path {
		&self.path
	}
}

/// Makes a scratch directory below `target/test-tmp`. Remove it by dropping the
/// returned `Scratch`, so bind it for the lifetime of the test.
pub fn tempdir(tag: &str) -> io::Result<Scratch> {
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
