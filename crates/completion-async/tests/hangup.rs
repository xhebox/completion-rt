//! A stream's hangup wait, through the facade: it outlasts the peer's
//! half-close and resolves once the peer is gone.

#![cfg(target_os = "linux")]

mod common;

use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use completion_async::{Cancel, Config, ErrorKind, Executor, Facade};

fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

#[test]
fn a_hangup_waits_out_the_half_close_for_the_peer_to_go() {
	let (peer, stream) = UnixStream::pair().unwrap();
	let mut executor = executor();
	let stream: Facade<UnixStream> = Facade::with_executor(stream, &executor);
	let gone = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&gone);
	let peer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		peer.shutdown(Shutdown::Write).unwrap();
		std::thread::sleep(Duration::from_millis(150));
		flag.store(true, Ordering::Release);
		drop(peer);
	});

	let cancel = Cancel::new();
	executor
		.block_on(stream.hangup().until(&cancel))
		.unwrap()
		.unwrap();
	assert!(
		gone.load(Ordering::Acquire),
		"the hangup resolved on the half-close"
	);
	peer.join().unwrap();
}

#[test]
fn a_cancelled_hangup_reports_the_cancel() {
	let (_peer, stream) = UnixStream::pair().unwrap();
	let mut executor = executor();
	let stream: Facade<UnixStream> = Facade::with_executor(stream, &executor);
	let cancel = Cancel::new();
	let raiser = cancel.clone();
	let thread = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		raiser.cancel();
	});

	let error = executor
		.block_on(stream.hangup().until(&cancel))
		.unwrap()
		.unwrap_err();
	assert_eq!(error.kind(), ErrorKind::Cancelled);
	thread.join().unwrap();
}
