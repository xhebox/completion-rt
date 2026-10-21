//! A stream's hangup wait: it outlasts the peer's
//! half-close and resolves once the peer is gone.

#![cfg(target_os = "linux")]

mod common;

use std::future::Future;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use completion_async::{Buffer, Config, ErrorKind, Executor, File, Stream};

fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

#[test]
fn a_hangup_waits_out_the_half_close_for_the_peer_to_go() {
	let (peer, stream) = UnixStream::pair().unwrap();
	let mut executor = executor();
	let stream: Stream = Stream::new(stream);
	let gone = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&gone);
	let peer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		peer.shutdown(Shutdown::Write).unwrap();
		std::thread::sleep(Duration::from_millis(150));
		flag.store(true, Ordering::Release);
		drop(peer);
	});

	// No scope of its own: `block_on` already runs the wait in the executor's
	// root scope, and nothing here gives it up.
	executor.block_on(stream.hangup()).unwrap().unwrap();
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
	let stream: Stream = Stream::new(stream);
	let scope = executor.scope();
	let raiser = scope.clone();
	let thread = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		drop(raiser.cancel());
	});

	let error = executor
		.block_on(async { scope.spawn(stream.hangup()).await })
		.unwrap()
		.unwrap_err();
	assert_eq!(error.kind(), ErrorKind::Cancelled);
	thread.join().unwrap();
}

/// The kernel ends a hangup's multishot poll on its own when the completion
/// queue it would report to is full. That is not the peer going away, so the
/// handle arms the wait again instead of failing one.
#[test]
fn a_hangup_survives_a_poll_the_kernel_ends_early() {
	// A ring whose completion queue holds four, so the burst below overflows it.
	let mut executor = Executor::new(Config {
		entries: 2,
		..Config::default()
	})
	.unwrap();
	let (peer, stream) = UnixStream::pair().unwrap();
	let stream: Stream = Stream::new(stream);
	// The wait, and then a burst of reads: nothing drives the reactor here, so
	// the completions pile up unreaped while the poll is armed.
	let mut hangup = Box::pin(stream.hangup());
	let scratch = common::tempdir("completion-hangup-ended").unwrap();
	let buf: Buffer = Buffer::zeroed(24);
	let mut reads = Vec::new();
	for index in 0..24usize {
		let path = scratch.path().join(format!("data{index}"));
		std::fs::write(&path, b"hello").unwrap();
		let file = File::new(common::open(&path).unwrap());
		reads.push(Box::pin(file.read_at(buf.span(index, 1), 0)));
	}
	executor
		.block_on(std::future::poll_fn(|cx| {
			assert!(hangup.as_mut().poll(cx).is_pending());
			for read in &mut reads {
				assert!(read.as_mut().poll(cx).is_pending());
			}
			std::task::Poll::Ready(())
		}))
		.unwrap();
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
