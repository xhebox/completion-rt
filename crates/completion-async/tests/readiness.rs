//! Readiness on a descriptor that carries no transfer.
//!
//! [`Readiness`] is a descriptor whose only operation is the wait: an eventfd,
//! a pidfd, a vhost-user endpoint. Nothing moves through one, so a wait is the
//! whole operation. A resource with a transfer has no wait to offer — the
//! backend arms the readiness itself when a call would block — which the
//! compile-fail cases on the handles hold to.
//!
//! The waits run on a pipe: unix's portable descriptor with two ends, and the
//! one both ends of which are plain descriptors rather than a socket.
#![cfg(unix)]

mod common;

use std::io::Write;
use std::os::fd::OwnedFd;
use std::time::{Duration, Instant};

use completion_async::{Config, Executor, Readiness, timeout};

/// The async driver the waits run on: it owns the reactor, so `block_on` both
/// polls the wait and drives the backend under it.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// A wait resolves on the readiness, not before it: an empty pipe has nothing
/// to read, and the peer's write is what ends the wait.
#[test]
fn a_wait_resolves_when_the_descriptor_becomes_ready() {
	const DELAY: Duration = Duration::from_millis(50);

	let (reader, mut peer) = std::io::pipe().unwrap();
	let mut executor = executor();
	let readiness = Readiness::new(OwnedFd::from(reader));

	let writer = std::thread::spawn(move || {
		std::thread::sleep(DELAY);
		peer.write_all(b"x").unwrap();
	});

	let started = Instant::now();
	executor.block_on(readiness.readable()).unwrap().unwrap();
	let waited = started.elapsed();
	writer.join().unwrap();
	assert!(
		waited >= DELAY,
		"the wait resolved in {waited:?}, before the peer wrote"
	);
	assert!(
		waited < Duration::from_secs(5),
		"the wait did not end on the peer's write"
	);
}

/// The other direction resolves on a descriptor that can take a write: a
/// wait is not a read, and a resource only ever waited on offers both.
#[test]
fn a_writable_descriptor_ends_a_write_wait() {
	let (_reader, writer) = std::io::pipe().unwrap();
	let mut executor = executor();
	let readiness = Readiness::new(OwnedFd::from(writer));

	executor.block_on(readiness.writable()).unwrap().unwrap();
}

/// A wait is an operation like any other, so a cancel ends it: nothing is owed
/// on a pipe that never filled, and the wait yields having moved nothing.
#[test]
fn a_dropped_wait_gives_the_descriptor_back() {
	let (reader, _peer) = std::io::pipe().unwrap();
	let mut executor = executor();
	let readiness = Readiness::new(OwnedFd::from(reader));

	executor
		.block_on(async {
			let mut wait = std::pin::pin!(readiness.readable());
			std::future::poll_fn(|cx| {
				let _ = wait.as_mut().poll(cx);
				std::task::Poll::Ready(())
			})
			.await;
		})
		.unwrap();
	executor
		.block_on(timeout(Duration::from_millis(50)))
		.unwrap()
		.unwrap();
	assert!(
		readiness.try_take().is_some(),
		"the dropped wait still holds the descriptor"
	);
}
