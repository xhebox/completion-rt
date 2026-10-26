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

use assert2::assert;
use completion_async::{BackendKind, Buffer, Config, File, StreamSocket, block_on, init};
use rstest::rstest;

/// The wait outlasts the peer's half-close on every backend: the ring's own
/// hangup poll and the portable backend's poller interrupt both hold until the
/// peer goes away.
#[rstest]
fn a_hangup_waits_out_the_half_close_for_the_peer_to_go(
	#[values(BackendKind::default(), BackendKind::Portable)] backend: BackendKind,
) {
	init(Config {
		backend,
		..Config::default()
	})
	.unwrap();
	let (peer, stream) = UnixStream::pair().unwrap();
	common::init_logging();
	let stream: StreamSocket = StreamSocket::new(stream);
	let gone = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&gone);
	let peer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		peer.shutdown(Shutdown::Write).unwrap();
		std::thread::sleep(Duration::from_millis(150));
		flag.store(true, Ordering::Release);
		drop(peer);
	});

	block_on(stream.hangup()).unwrap().unwrap();
	assert!(
		gone.load(Ordering::Acquire),
		"the hangup resolved on the half-close"
	);
	peer.join().unwrap();
}

/// The kernel ends a hangup's multishot poll on its own when the completion
/// queue it would report to is full. That is not the peer going away, so the
/// handle arms the wait again instead of failing one.
#[test]
fn a_hangup_survives_a_poll_the_kernel_ends_early() {
	// A ring whose completion queue holds four, so the burst below overflows it.
	init(Config {
		entries: 2,
		..Config::default()
	})
	.unwrap();
	let (peer, stream) = UnixStream::pair().unwrap();
	let stream: StreamSocket = StreamSocket::new(stream);
	// The wait, and then a burst of reads: nothing drives the reactor here, so
	// the completions pile up unreaped while the poll is armed.
	let mut hangup = Box::pin(stream.hangup());
	let scratch = common::tempdir("completion-hangup-ended").unwrap();
	let mut reads = Vec::new();
	for index in 0..24usize {
		let path = scratch.path().join(format!("data{index}"));
		std::fs::write(&path, b"hello").unwrap();
		let file = File::new(common::open(&path).unwrap());
		reads.push(Box::pin(file.read_at(Buffer::zeroed(1), 0)));
	}
	block_on(std::future::poll_fn(|cx| {
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
	block_on(hangup).unwrap().unwrap();
	assert!(
		gone.load(Ordering::Acquire),
		"the hangup resolved before the peer went"
	);
	peer.join().unwrap();
}
