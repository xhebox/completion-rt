//! Socket operations that need the reactor driven by hand: an accept whose
//! landed result is never taken, a receive or take observed between passes, and
//! the origin a submission reports.
//!
//! These drive `Reactor::poll` themselves — a bounded pass to put an operation
//! in flight, a `ZERO` poll to hand one to the kernel, an `is_idle` to know it
//! settled — none of which the async layer, whose executor owns the reactor,
//! exposes. The tests that only move bytes through the facade live with the
//! high crate.

mod common;

#[cfg(unix)]
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Waker};
use std::time::Duration;

use completion_rt::{Config, Extent, Handle, Memory, Reactor};

/// A connected pair over loopback: the portable way to two real sockets.
fn pair() -> (TcpStream, TcpStream) {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (server, _) = listener.accept().unwrap();
	(client, server)
}

fn memory(buf: &Arc<Vec<u8>>) -> Arc<dyn Memory> {
	let memory: Arc<dyn Memory> = buf.clone();
	memory
}

fn whole(buf: &Arc<Vec<u8>>) -> [Extent; 1] {
	[Extent {
		offset: 0,
		len: buf.len(),
	}]
}

/// A listener to connect to inside `directory`, and the path its socket lives
/// at.
#[cfg(unix)]
fn listener(directory: &std::path::Path) -> (std::os::unix::net::UnixListener, std::path::PathBuf) {
	let path = directory.join("sock");
	assert!(
		path.as_os_str().len() < 104,
		"the socket path does not fit sun_path: {}",
		path.display()
	);
	let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
	listener.set_nonblocking(true).unwrap();
	(listener, path)
}

/// Drives the reactor until the accept's response is in the entry that holds
/// it.
#[cfg(unix)]
fn drive_until_settled(reactor: &mut Reactor) {
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
}

/// The peer's read once the accepted end is gone: zero bytes is the
/// connection ending, and a leaked descriptor would leave it waiting.
#[cfg(unix)]
fn assert_the_connection_ended(client: &mut std::os::unix::net::UnixStream) {
	client.set_nonblocking(true).unwrap();
	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	let mut byte = [0u8; 1];
	loop {
		match client.read(&mut byte) {
			Ok(0) => return,
			Ok(read) => panic!("the peer read {read} bytes, not the connection ending"),
			Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
				assert!(
					std::time::Instant::now() < deadline,
					"the peer never saw the connection end"
				);
				std::thread::sleep(Duration::from_millis(1));
			}
			Err(error) => panic!("the peer's read failed: {error}"),
		}
	}
}

/// Drives the reactor by hand until the peer sees the connection end. An entry
/// whose result was given up on no longer counts as outstanding, so nothing
/// else reaps for it, and a socket left open keeps the peer waiting.
#[cfg(unix)]
fn drive_until_the_connection_ends(
	reactor: &mut Reactor,
	client: &mut std::os::unix::net::UnixStream,
) {
	client.set_nonblocking(true).unwrap();
	let mut byte = [0u8; 1];
	for _ in 0..500 {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
		match client.read(&mut byte) {
			Ok(0) => return,
			Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
			other => panic!("the peer read {other:?}"),
		}
	}
	panic!("the accepted socket was never closed");
}

/// An accept whose landed result is never taken closes the socket the kernel
/// created: the peer sees the connection end.
#[test]
#[cfg(unix)]
fn a_dropped_accept_closes_the_new_socket() {
	use std::os::unix::net::UnixStream;

	let directory = common::tempdir("completion-accept-drop").unwrap();
	let (listener, path) = listener(directory.path());
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let listener = Handle::new(listener);
	let mut client;
	{
		let mut accept = pin!(submitter.accept(&listener).unwrap());
		let mut cx = Context::from_waker(Waker::noop());
		// Hand the accept to the kernel.
		assert!(accept.as_mut().poll(&mut cx).is_pending());
		client = UnixStream::connect(&path).unwrap();
		drive_until_settled(&mut reactor);
		// The socket exists and the response holds it, so this is not a
		// request saying nobody wanted it yet.
	}

	assert_the_connection_ended(&mut client);
}

/// An accept dropped while the kernel still has it, and which succeeds
/// anyway, closes the socket that comes back with nobody waiting for it.
#[test]
#[cfg(unix)]
fn an_abandoned_accept_that_succeeds_closes_its_socket() {
	use std::os::unix::net::UnixStream;

	let directory = common::tempdir("completion-accept-abandoned").unwrap();
	let (listener, path) = listener(directory.path());
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let listener = Handle::new(listener);
	{
		let mut accept = pin!(submitter.accept(&listener).unwrap());
		let mut cx = Context::from_waker(Waker::noop());
		// Hand the accept to the kernel with nothing to accept yet.
		assert!(accept.as_mut().poll(&mut cx).is_pending());
		reactor.poll(Some(Duration::ZERO)).unwrap();
	}

	// The client arrives after the result was given up on, and the kernel
	// reports the connection all the same.
	let mut client = UnixStream::connect(&path).unwrap();
	drive_until_the_connection_ends(&mut reactor, &mut client);
}

/// An until future dropped after its cancel fired, whose accept raced to
/// success anyway, leaves no socket behind: the caller never takes the result,
/// and the peer still sees the connection end.
#[test]
#[cfg(unix)]
fn a_cancelled_accept_that_succeeded_closes_its_socket() {
	use std::os::unix::net::UnixStream;

	use completion_rt::Cancel;

	let directory = common::tempdir("completion-accept-raced").unwrap();
	let (listener, path) = listener(directory.path());
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let listener = Handle::new(listener);

	let cancel = Cancel::new();
	let mut client;
	{
		let mut accept = pin!(submitter.accept(&listener).unwrap().until(&cancel));
		let mut cx = Context::from_waker(Waker::noop());
		// Hand the accept to the kernel with nothing to accept yet.
		assert!(accept.as_mut().poll(&mut cx).is_pending());
		reactor.poll(Some(Duration::ZERO)).unwrap();

		client = UnixStream::connect(&path).unwrap();
		// The kernel reports the connection without the reactor being driven,
		// so the accept is done before the cancel fires.
		std::thread::sleep(Duration::from_millis(50));
		cancel.cancel();
		assert!(
			accept.as_mut().poll(&mut cx).is_pending(),
			"the caller asked for the accept to stop"
		);
		// The ask reaches the backend, which the accept has already outrun.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		// The caller goes without the result, and the socket it never took
		// goes with the future.
	}

	drive_until_the_connection_ends(&mut reactor, &mut client);
}

/// The origin a reactor reports names the call that asked for the operation.
///
/// A facade takes the location where the operation was asked for, because the
/// frame that reaches the submitter through the loop is the loop's own; a
/// report that points at `flow.rs` names code that cannot stop asking.
#[test]
fn an_operation_reports_the_line_that_asked_for_it() {
	let (client, server) = pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let client = Handle::new(client);
	let buffer = Arc::new(vec![0u8; 16]);

	let before = line!();
	let mut receive = pin!(
		submitter
			.recv(&client, memory(&buffer), whole(&buffer))
			.unwrap()
	);
	let after = line!();
	let mut cx = Context::from_waker(Waker::noop());
	assert!(
		std::future::Future::poll(receive.as_mut(), &mut cx).is_pending(),
		"the peer sends nothing, so the receive waits"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();

	let entry = submitter
		.in_flight()
		.into_iter()
		.find(|entry| entry.op == "recv")
		.expect("the receive is in the reactor");
	assert_eq!(
		entry.origin.file(),
		file!(),
		"the origin names another file than the caller's"
	);
	assert!(
		(before..=after).contains(&entry.origin.line()),
		"the origin names line {}, outside the call at {before}..={after}",
		entry.origin.line()
	);

	// The peer going ends the receive, so the future is dropped with nothing in
	// flight: the fallback is not what this test is about.
	drop(server);
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
}
