//! A hangup wait resolves once the peer is gone, not when it only half-closes:
//! a stream whose peer shut its write side is still open the other way.

#![cfg(target_os = "linux")]

mod common;

use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::pin::pin;
use std::task::{Context, Waker};
use std::time::Duration;

use common::{block_on, submit};
use completion_rt::{Config, ErrorKind, Handle, Op, Reactor, Response};

fn pair() -> (UnixStream, Handle<UnixStream>) {
	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	(left, Handle::new(right))
}

#[test]
fn a_half_close_is_not_a_hangup_but_the_peer_going_is() {
	let (left, right) = pair();
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut hangup = pin!(submit(&reactor, &right, Op::Hangup).unwrap());
	let mut cx = Context::from_waker(Waker::noop());
	assert!(hangup.as_mut().poll(&mut cx).is_pending());

	left.shutdown(Shutdown::Write).unwrap();
	for _ in 0..10 {
		reactor.poll(Some(Duration::from_millis(10))).unwrap();
	}
	assert!(
		hangup.as_mut().poll(&mut cx).is_pending(),
		"a peer that only shut its write side is still there"
	);

	drop(left);
	let ((), result) = block_on(&reactor, hangup);
	assert!(matches!(result.unwrap(), Response::Done));

	// The poll the hangup rode is withdrawn once reported; its last completion
	// lands on nothing.
	for _ in 0..5 {
		reactor.poll(Some(Duration::from_millis(5))).unwrap();
	}
	assert!(common::idle(&reactor), "the reported hangup left an entry");
}

#[test]
fn a_peer_already_gone_is_a_hangup_at_once() {
	let (left, right) = pair();
	drop(left);
	let reactor = Reactor::new(Config::default()).unwrap();
	let hangup = submit(&reactor, &right, Op::Hangup).unwrap();
	let ((), result) = block_on(&reactor, hangup);
	assert!(matches!(result.unwrap(), Response::Done));
}

/// TCP reads a peer's close as a FIN, the same half-close a `shutdown(Write)`
/// sends: the connection hangs up once this side shuts its own write side too.
#[test]
fn a_tcp_peer_close_hangs_up_once_this_side_shuts_too() {
	use std::net::{TcpListener, TcpStream};

	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (stream, _) = listener.accept().unwrap();
	stream.set_nonblocking(true).unwrap();
	let stream = Handle::new(stream);
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut hangup = pin!(submit(&reactor, &stream, Op::Hangup).unwrap());
	let mut cx = Context::from_waker(Waker::noop());
	assert!(hangup.as_mut().poll(&mut cx).is_pending());

	drop(peer);
	for _ in 0..10 {
		reactor.poll(Some(Duration::from_millis(10))).unwrap();
	}
	assert!(
		hangup.as_mut().poll(&mut cx).is_pending(),
		"a FIN alone is a half-close"
	);

	stream.shutdown(Shutdown::Write).unwrap();
	let ((), result) = block_on(&reactor, hangup);
	assert!(matches!(result.unwrap(), Response::Done));
}

#[test]
fn a_hangup_cancelled_before_the_peer_goes_reports_a_cancel() {
	let (_left, right) = pair();
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut hangup = pin!(submit(&reactor, &right, Op::Hangup).unwrap());
	let mut cx = Context::from_waker(Waker::noop());
	assert!(hangup.as_mut().poll(&mut cx).is_pending());
	reactor.poll(Some(Duration::ZERO)).unwrap();

	hangup.cancel();
	let ((), result) = block_on(&reactor, hangup);
	assert_eq!(result.unwrap_err().kind(), ErrorKind::Cancelled);
}
