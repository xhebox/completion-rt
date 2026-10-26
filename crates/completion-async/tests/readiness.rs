//! Readiness waits on [`Stream`], [`StreamSocket`], and [`DatagramSocket`].
//!
//! The waits run on a pipe, a socket, and on Windows a waitable event handle.

mod common;

use assert2::assert;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(unix)]
use std::os::fd::OwnedFd;
use std::time::{Duration, Instant};

#[cfg(unix)]
use completion_async::AsDescriptor;
use completion_async::{Stream, StreamSocket, block_on, timeout};

/// A connected loopback pair: the portable way to two real sockets.
fn pair() -> (TcpStream, TcpStream) {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (server, _) = listener.accept().unwrap();
	(client, server)
}

/// A wait resolves on the readiness, not before it: an empty pipe has nothing
/// to read, and the peer's write is what ends the wait.
#[cfg(unix)]
#[test]
fn a_wait_resolves_when_the_descriptor_becomes_ready() {
	const DELAY: Duration = Duration::from_millis(50);

	let (reader, mut peer) = std::io::pipe().unwrap();
	common::init_logging();
	let readiness = Stream::new(OwnedFd::from(reader));

	let writer = std::thread::spawn(move || {
		std::thread::sleep(DELAY);
		peer.write_all(b"x").unwrap();
	});

	let started = Instant::now();
	block_on(readiness.readable()).unwrap().unwrap();
	let waited = started.elapsed();
	writer.join().unwrap();
	let mut byte = [0u8; 1];
	assert!(rustix::io::read(readiness.as_descriptor(), &mut byte).unwrap() == 1);
	assert!(&byte == b"x");
	assert!(waited >= DELAY, "the wait resolved before the peer wrote");
	assert!(
		waited < Duration::from_secs(5),
		"the wait did not end on the peer's write"
	);
}

/// The other direction resolves on a descriptor that can take a write: a
/// wait is not a read, and a resource only ever waited on offers both.
#[cfg(unix)]
#[test]
fn a_writable_descriptor_ends_a_write_wait() {
	let (_reader, writer) = std::io::pipe().unwrap();
	common::init_logging();
	let readiness = Stream::new(OwnedFd::from(writer));

	block_on(readiness.writable()).unwrap().unwrap();
}

/// A wait is an operation like any other, so a cancel ends it: nothing is owed
/// on a pipe that never filled, and the wait yields having moved nothing.
#[cfg(unix)]
#[test]
fn a_dropped_wait_gives_the_descriptor_back() {
	let (reader, _peer) = std::io::pipe().unwrap();
	common::init_logging();
	let readiness = Stream::new(OwnedFd::from(reader));

	block_on(async {
		let mut wait = std::pin::pin!(readiness.readable());
		std::future::poll_fn(|cx| {
			drop(wait.as_mut().poll(cx));
			std::task::Poll::Ready(())
		})
		.await;
	})
	.unwrap();
	block_on(timeout(Duration::from_millis(50)))
		.unwrap()
		.unwrap();
	assert!(
		readiness.try_take().is_some(),
		"the dropped wait still holds the descriptor"
	);
}

/// A socket is a readiness source on every target: the wait resolves on the
/// peer's write and not before it, and the descriptor comes back with the bytes
/// still in it.
#[test]
fn a_socket_readiness_wait_resolves_when_the_peer_writes() {
	const DELAY: Duration = Duration::from_millis(50);

	// A socket just connected has room for a write, so that wait is answered
	// here and now.
	let (client, _server) = pair();
	common::init_logging();
	let writable = StreamSocket::new(client);
	block_on(writable.writable()).unwrap().unwrap();

	let (mut client, server) = pair();
	let mut probe = server
		.try_clone()
		.expect("a loopback socket clones its handle");
	let readiness = StreamSocket::new(server);

	let sender = std::thread::spawn(move || {
		std::thread::sleep(DELAY);
		client.write_all(b"x").unwrap();
	});

	let started = Instant::now();
	block_on(readiness.readable()).unwrap().unwrap();
	let waited = started.elapsed();
	sender.join().unwrap();
	assert!(waited >= DELAY, "the wait resolved before the peer wrote");
	assert!(
		waited < Duration::from_secs(5),
		"the wait did not end on the peer's write"
	);

	// The wait left the socket readable, not drained: the byte the peer sent is
	// still there for the caller to read itself.
	probe.set_nonblocking(false).unwrap();
	let mut byte = [0u8; 1];
	assert!(probe.read(&mut byte).unwrap() == 1);
	assert!(&byte == b"x");
	assert!(
		readiness.try_take().is_some(),
		"the settled wait still holds the socket"
	);
}

/// On Windows, a waitable kernel object such as an event handle resolves a
/// readiness wait once signaled, and a dropped wait releases the handle.
#[cfg(windows)]
#[test]
fn a_waitable_event_handle_resolves_and_cancels_cleanly() {
	use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
	use windows_sys::Win32::System::Threading::{CreateEventW, SetEvent};

	const DELAY: Duration = Duration::from_millis(50);

	common::init_logging();
	let raw = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
	assert!(!raw.is_null(), "CreateEventW failed");
	let event = unsafe { OwnedHandle::from_raw_handle(raw) };
	let signal_target = event.as_raw_handle() as usize;
	let readiness = Stream::new(event);

	let signaller = std::thread::spawn(move || {
		std::thread::sleep(DELAY);
		assert!(unsafe { SetEvent(signal_target as _) } != 0);
	});

	let started = Instant::now();
	block_on(readiness.readable()).unwrap().unwrap();
	let waited = started.elapsed();
	signaller.join().unwrap();
	assert!(
		waited >= DELAY,
		"the wait resolved before the event was set"
	);

	// An unsignaled event whose wait is dropped in flight gives the handle back.
	let raw = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
	assert!(!raw.is_null(), "CreateEventW failed");
	let event = unsafe { OwnedHandle::from_raw_handle(raw) };
	let readiness = Stream::new(event);
	block_on(async {
		let mut wait = std::pin::pin!(readiness.readable());
		std::future::poll_fn(|cx| {
			drop(wait.as_mut().poll(cx));
			std::task::Poll::Ready(())
		})
		.await;
	})
	.unwrap();
	block_on(timeout(Duration::from_millis(50)))
		.unwrap()
		.unwrap();
	assert!(
		readiness.try_take().is_some(),
		"the dropped wait still holds the event handle"
	);
}
