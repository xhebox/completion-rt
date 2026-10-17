//! A stream's hangup wait, through the facade: it outlasts the peer's
//! half-close and resolves once the peer is gone.

#![cfg(target_os = "linux")]

mod common;

use std::future::Future;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Waker};
use std::time::Duration;

use completion_async::{Buffer, Cancel, Config, ErrorKind, Executor, Facade, File};

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

/// The kernel ends a hangup's multishot poll on its own when the completion
/// queue it would report to is full. That is not the peer going away, so the
/// facade arms the wait again instead of failing one.
#[test]
fn a_hangup_survives_a_poll_the_kernel_ends_early() {
	// A ring whose completion queue holds four, so the burst below overflows it.
	let mut executor = Executor::new(Config {
		entries: 2,
		..Config::default()
	})
	.unwrap();
	let (peer, stream) = UnixStream::pair().unwrap();
	let stream: Facade<UnixStream> = Facade::with_executor(stream, &executor);
	let mut cx = Context::from_waker(Waker::noop());

	// The wait, and then a burst of reads: nothing drives the reactor here, so
	// the completions pile up unreaped while the poll is armed.
	let mut hangup = Box::pin(stream.hangup());
	assert!(hangup.as_mut().poll(&mut cx).is_pending());
	let scratch = common::tempdir("completion-hangup-ended").unwrap();
	let buf: Buffer = Buffer::zeroed(24);
	let mut reads = Vec::new();
	for index in 0..24 {
		let path = scratch.path().join(format!("data{index}"));
		std::fs::write(&path, b"hello").unwrap();
		let file: File = Facade::with_executor(common::open(&path).unwrap(), &executor);
		let mut read = Box::pin(file.read_at(buf.span(index, 1), 0));
		assert!(read.as_mut().poll(&mut cx).is_pending());
		// Held: dropping a transfer releases the operation behind it.
		reads.push(read);
	}
	std::thread::sleep(Duration::from_millis(50));
	// The half-close reaches the armed poll with the queue full: the kernel ends
	// the multishot rather than queue a report it has no room for.
	peer.shutdown(Shutdown::Write).unwrap();

	let gone = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&gone);
	let peer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(150));
		flag.store(true, Ordering::Release);
		drop(peer);
	});
	executor.block_on(hangup).unwrap().unwrap();
	assert!(
		gone.load(Ordering::Acquire),
		"the hangup resolved before the peer went"
	);
	peer.join().unwrap();
}
