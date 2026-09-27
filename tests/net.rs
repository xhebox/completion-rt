//! Socket operations: the descriptor they name, before a kernel sees them.
mod common;

use common::block_on;

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
#[cfg(unix)]
use std::task::{Context, Waker};
use std::time::Duration;

use completion_rt::{Config, Error, Facade, Reactor};

/// A connected pair over loopback: the portable way to two real sockets.
fn pair() -> (TcpStream, TcpStream) {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (server, _) = listener.accept().unwrap();
	(client, server)
}

/// A listener to connect to inside `directory`, and the path its socket lives
/// at.
///
/// A `sockaddr_un` path is bounded — 104 bytes including the NUL on macOS, the
/// shortest of the two — which the scratch directory is named to fit (see
/// `common::Scratch::path`); this holds the bound to what the kernel actually
/// gets.
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
///
/// The wait is a loop over a non-blocking read rather than a read timeout:
/// `setsockopt(SO_RCVTIMEO)` is refused on a unix socket whose peer has already
/// closed (macOS answers EINVAL), and this is called after that point.
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
	use std::os::unix::net::{UnixListener, UnixStream};

	let directory = common::tempdir("completion-accept-drop").unwrap();
	let (listener, path) = listener(directory.path());
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let listener: Facade<UnixListener> = Facade::new(listener, &submitter);
	let mut client;
	{
		let mut accept = std::pin::pin!(listener.accept());
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
	use std::os::unix::net::{UnixListener, UnixStream};

	let directory = common::tempdir("completion-accept-abandoned").unwrap();
	let (listener, path) = listener(directory.path());
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let listener: Facade<UnixListener> = Facade::new(listener, &submitter);
	{
		let mut accept = std::pin::pin!(listener.accept());
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
	use std::future::Future;
	use std::os::unix::net::{UnixListener, UnixStream};

	use completion_rt::Cancel;

	let directory = common::tempdir("completion-accept-raced").unwrap();
	let (listener, path) = listener(directory.path());
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let listener: Facade<UnixListener> = Facade::new(listener, &submitter);

	let cancel = Cancel::new();
	let mut client;
	{
		let mut accept = std::pin::pin!(listener.accept().until(&cancel));
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

/// A receive given up on its `Cancel` comes back cancelled, with
/// nothing falling back to a blocking drop and the buffer left alone.
#[test]
fn a_given_up_receive_reports_the_cancel() {
	use std::future::Future;
	use std::pin::Pin;
	use std::task::{Context, Poll, Waker};

	use completion_rt::{Cancel, Error, Extent};

	/// Polls `future` while driving the reactor, bounded: an operation nothing
	/// ends fails the test instead of parking it.
	fn drive<T>(reactor: &mut Reactor, future: &mut Pin<&mut impl Future<Output = T>>) -> T {
		let mut cx = Context::from_waker(Waker::noop());
		for _ in 0..500 {
			if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
				return value;
			}
			reactor.poll(Some(Duration::from_millis(1))).unwrap();
		}
		panic!("the operation never landed");
	}

	let (client, _server) = pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);
	let buffer = Arc::new(vec![0u8; 16]);
	let payload: Arc<dyn completion_rt::Memory> =
		Arc::clone(&buffer) as Arc<dyn completion_rt::Memory>;
	let cancel = Cancel::new();
	let mut receive = std::pin::pin!(
		socket
			.recv_vectored(payload, [Extent { offset: 0, len: 16 }])
			.until(&cancel)
	);
	let mut cx = Context::from_waker(Waker::noop());
	// The peer sends nothing, so the kernel has the receive and only the cancel
	// can end it.
	assert!(receive.as_mut().poll(&mut cx).is_pending());
	reactor.poll(Some(Duration::ZERO)).unwrap();
	let drops = reactor.fallback_drops();
	cancel.cancel();

	let outcome = drive(&mut reactor, &mut receive);
	assert!(
		matches!(outcome.result, Err(Error::Cancelled)),
		"a given-up receive is not the receive's own result: {:?}",
		outcome.result
	);
	assert_eq!(outcome.value, 0, "nothing arrived for the receive to count");
	assert_eq!(
		reactor.fallback_drops(),
		drops,
		"the drop fallback answered the cancel, not the until race"
	);
	assert!(
		buffer.iter().all(|byte| *byte == 0),
		"the receive wrote the buffer it was supposed to leave alone"
	);
}

/// `send_all` moves a payload larger than one call: eight mebibytes do not fit
/// the socket's buffers, the peer drains while the send goes, and only a loop
/// puts all of it on the wire.
#[test]
fn send_all_moves_a_payload_larger_than_one_call() {
	use std::io::Read;

	let (client, mut server) = pair();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
		reactor
	});

	// The peer drains, so the send buffer never holds the whole payload. The
	// timeout is not part of the contract: it is here so a send that stops
	// short fails this test instead of parking it.
	let len = 8 << 20;
	let drain = std::thread::spawn(move || {
		server
			.set_read_timeout(Some(Duration::from_secs(10)))
			.unwrap();
		let mut got = vec![0u8; len];
		server.read_exact(&mut got).map(|()| got)
	});

	let payload: Arc<Vec<u8>> = Arc::new((0..len).map(|i| i as u8).collect());
	block_on(socket.send_all(payload.clone()))
		.into_result()
		.unwrap();

	let got = drain
		.join()
		.unwrap()
		.expect("the peer never saw the whole payload");
	assert_eq!(got, *payload, "the payload did not arrive whole");
	submitter.stop();
	driven.join().unwrap();
}

/// `recv_exact` fills a buffer larger than one call: the peer sends more than
/// the socket's buffers hold, and only a loop fills the buffer.
#[test]
fn recv_exact_fills_a_buffer_larger_than_one_call() {
	use std::io::Write;

	let (mut client, server) = pair();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(server, &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
		reactor
	});

	// The write timeout bounds the sender, so a receive that stops short fails
	// this test instead of parking the sender thread.
	let len = 8 << 20;
	let payload: Arc<Vec<u8>> = Arc::new((0..len).map(|i| i as u8).collect());
	let sender = payload.clone();
	let sending = std::thread::spawn(move || {
		client
			.set_write_timeout(Some(Duration::from_secs(10)))
			.unwrap();
		client.write_all(&sender)
	});

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; len]);
	block_on(socket.recv_exact(buffer.clone()))
		.into_result()
		.unwrap();
	sending
		.join()
		.unwrap()
		.expect("the sender never got the whole payload out");

	assert_eq!(*buffer, *payload, "the payload did not arrive whole");
	submitter.stop();
	driven.join().unwrap();
}

/// A cancel covers the whole `recv_exact` loop: a cancel that lands after the
/// first call has moved bytes, but before the memory is full, ends the loop
/// there — the bytes it took are its progress, and the cancel is why it stopped.
#[test]
#[cfg(unix)]
fn a_cancel_ends_a_recv_exact_loop_mid_way() {
	use std::future::Future;
	use std::io::Write;
	use std::os::unix::net::UnixStream;
	use std::task::{Context, Poll, Waker};

	use completion_rt::{Cancel, Extent, Memory};

	let (mut left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<UnixStream> = Facade::new(right, &submitter);

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0u8; 8]);
	let memory: Arc<dyn Memory> = Arc::clone(&buffer) as Arc<dyn Memory>;
	let cancel = Cancel::new();
	let mut receive = std::pin::pin!(
		socket
			.recv_exact_vectored(memory, [Extent { offset: 0, len: 8 }])
			.until(&cancel)
	);
	let mut cx = Context::from_waker(Waker::noop());
	assert!(receive.as_mut().poll(&mut cx).is_pending());

	// Four bytes are one call: the loop takes them and comes back for the
	// rest, which the peer never sends.
	left.write_all(b"ABCD").unwrap();
	let mut first = false;
	for _ in 0..500 {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
		if buffer[..4] == *b"ABCD" {
			first = true;
			break;
		}
	}
	assert!(first, "the first call never landed");

	cancel.cancel();
	let mut landed = None;
	for _ in 0..500 {
		if let Poll::Ready(value) = receive.as_mut().poll(&mut cx) {
			landed = Some(value);
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	let outcome = landed.expect("the loop never landed");
	assert_eq!(outcome.value, 4, "the loop kept the bytes it had taken");
	assert!(
		matches!(outcome.result, Err(Error::Cancelled)),
		"a given-up loop is not the loop's own result: {:?}",
		outcome.result
	);
	assert_eq!(&buffer[..4], b"ABCD", "the first call's bytes stay");
	assert_eq!(
		&buffer[4..],
		&[0u8; 4],
		"the loop wrote past what it was given"
	);
}

/// A socket type of the caller's own: the socket traits are plain markers, so a
/// wrapper this crate has never seen gets the same receive and send — and, for
/// a stream, the loops over them.
///
/// [`SocketHandle`]: completion_rt::SocketHandle
struct Wrapper(std::net::TcpStream);

impl completion_rt::AsDescriptor for Wrapper {
	fn as_descriptor(&self) -> completion_rt::BorrowedDescriptor<'_> {
		completion_rt::AsDescriptor::as_descriptor(&self.0)
	}
}

impl completion_rt::SocketHandle for Wrapper {}
impl completion_rt::StreamHandle for Wrapper {}

#[test]
fn a_caller_socket_type_gets_recv_and_send() {
	use std::io::Write;

	let (client, mut server) = pair();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<Wrapper> = Facade::new(Wrapper(client), &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
		reactor
	});

	let payload: Arc<Vec<u8>> = Arc::new(b"hello".to_vec());
	assert_eq!(block_on(socket.send(payload)).into_result().unwrap(), 5);
	server
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut got = [0u8; 5];
	server.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

	server.write_all(b"world").unwrap();
	let buffer: Arc<Vec<u8>> = Arc::new(vec![0u8; 5]);
	assert_eq!(
		block_on(socket.recv(buffer.clone())).into_result().unwrap(),
		5
	);
	assert_eq!(&buffer[..], b"world");

	submitter.stop();
	driven.join().unwrap();
}

/// A cancel that lands in a `send_all` loop reports the bytes the loop had put
/// out: the progress is not thrown away with the cancel.
#[test]
fn a_cancel_ends_a_send_all_loop_with_its_progress() {
	use std::future::Future;
	use std::task::{Context, Poll, Waker};

	use completion_rt::Cancel;

	// The peer is held, and never reads: the socket's own buffers take a few
	// calls' worth and the loop is left with the kernel. Eight mebibytes are
	// more than any default socket buffer holds, so the loop cannot finish.
	let (client, _server) = pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let len = 8 << 20;
	let payload: Arc<Vec<u8>> = Arc::new((0..len).map(|i| i as u8).collect());
	let cancel = Cancel::new();
	let mut send = std::pin::pin!(socket.send_all(payload.clone()).until(&cancel));
	let mut cx = Context::from_waker(Waker::noop());
	assert!(send.as_mut().poll(&mut cx).is_pending());

	let mut done = 0;
	for _ in 0..500 {
		if let Poll::Ready(outcome) = send.as_mut().poll(&mut cx) {
			done = outcome.value;
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	assert_eq!(
		done, 0,
		"the payload went out with the peer never reading it"
	);

	cancel.cancel();
	let mut landed = None;
	for _ in 0..500 {
		if let Poll::Ready(outcome) = send.as_mut().poll(&mut cx) {
			landed = Some(outcome);
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	let outcome = landed.expect("the send never landed");
	assert!(
		outcome.value > 0,
		"the cancel threw away the bytes the loop had sent"
	);
	assert!(
		matches!(outcome.result, Err(Error::Cancelled)),
		"a given-up loop is not the loop's own result: {:?}",
		outcome.result
	);
}

/// A `send_all` loop that fails keeps the progress it made before the call that
/// failed: the peer going away is the call's error, not a reason to forget the
/// bytes that left.
#[test]
fn a_send_all_loop_reports_what_it_sent_before_an_io_error() {
	use std::future::Future;
	use std::task::{Context, Poll, Waker};

	// The peer reads a little and is gone: the send fills what is left of the
	// socket's buffers, and the connection it is going down with it.
	let (client, server) = pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let len = 8 << 20;
	let payload: Arc<Vec<u8>> = Arc::new((0..len).map(|i| i as u8).collect());
	let mut send = std::pin::pin!(socket.send_all(payload.clone()));
	let mut cx = Context::from_waker(Waker::noop());
	assert!(send.as_mut().poll(&mut cx).is_pending());

	let mut done = 0;
	for _ in 0..500 {
		if let Poll::Ready(outcome) = send.as_mut().poll(&mut cx) {
			done = outcome.value;
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	assert_eq!(done, 0, "the loop finished with the peer reading nothing");

	// The peer goes away, and the reset reaches the send: what it had taken off
	// the memory is still the progress it reports.
	drop(server);
	let mut landed = None;
	for _ in 0..500 {
		if let Poll::Ready(outcome) = send.as_mut().poll(&mut cx) {
			landed = Some(outcome);
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	let outcome = landed.expect("the send never landed");
	assert!(outcome.value > 0, "the failed send dropped its progress");
	assert!(
		outcome.result.is_err(),
		"a send whose peer went away reports an error"
	);
}

/// A wait is an operation of its own, with nothing to count: `wait_writable`
/// resolves once the socket can take bytes, `wait_readable` once it has some.
#[test]
fn waits_resolve_when_the_socket_is_ready() {
	use std::io::Write;

	let (client, mut server) = pair();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	block_on(socket.wait_writable()).unwrap();

	server.write_all(b"x").unwrap();
	block_on(socket.wait_readable()).unwrap();

	submitter.stop();
	driven.join().unwrap();
}

/// A datagram socket is not a byte stream: each call is one message, and the
/// loops over the calls — `recv_exact`, `send_all` — are not on it.
#[test]
fn a_datagram_socket_takes_a_message_a_call() {
	use std::net::UdpSocket;

	let left = UdpSocket::bind("127.0.0.1:0").unwrap();
	let right = UdpSocket::bind("127.0.0.1:0").unwrap();
	right.connect(left.local_addr().unwrap()).unwrap();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let receiver: Facade<UdpSocket> = Facade::new(left, &submitter);
	let sender: Facade<UdpSocket> = Facade::new(right, &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	let payload: Arc<Vec<u8>> = Arc::new(b"hello".to_vec());
	assert_eq!(block_on(sender.send(payload)).into_result().unwrap(), 5);

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0u8; 8]);
	assert_eq!(
		block_on(receiver.recv(buffer.clone()))
			.into_result()
			.unwrap(),
		5
	);
	assert_eq!(&buffer[..5], b"hello");

	submitter.stop();
	driven.join().unwrap();
}

/// An accept hands back the connection's own facade: the listener says what its
/// streams are, and the caller gets that stream type, not a bare descriptor.
/// The connection is live both ways: the accepted stream sends to the peer and
/// receives from it.
#[test]
#[cfg(unix)]
fn an_accept_gives_a_stream_facade_of_its_own() {
	use std::io::Write;
	use std::os::unix::net::{UnixListener, UnixStream};

	let directory = common::tempdir("completion-accept-facade").unwrap();
	let (listener, path) = listener(directory.path());
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let listener: Facade<UnixListener> = Facade::new(listener, &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	let mut client = UnixStream::connect(&path).unwrap();
	let stream: Facade<UnixStream> = block_on(listener.accept()).unwrap();
	let payload: Arc<Vec<u8>> = Arc::new(b"hello".to_vec());
	assert_eq!(block_on(stream.send(payload)).into_result().unwrap(), 5);

	client
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut got = [0u8; 5];
	client.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

	// The connection reaches the other way too: the client's bytes come out of
	// the accepted stream.
	client.write_all(b"world").unwrap();
	let received: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let payload: Arc<dyn completion_rt::Memory> =
		Arc::clone(&received) as Arc<dyn completion_rt::Memory>;
	assert_eq!(block_on(stream.recv(payload)).into_result().unwrap(), 5);
	assert_eq!(&received[..], b"world");

	submitter.stop();
	driven.join().unwrap();
}

/// A receive that asked for control data hands the descriptors back with the
/// bytes: the caller owns what the kernel passed over the socket.
#[test]
#[cfg(unix)]
fn recv_with_fds_hands_the_descriptors_back() {
	use std::os::unix::net::UnixStream;

	let directory = common::tempdir("completion-fds").unwrap();
	let path = directory.path().join("payload");
	std::fs::write(&path, b"hello").unwrap();

	let (left, right) = UnixStream::pair().unwrap();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let sender: Facade<UnixStream> = Facade::new(left, &submitter);
	let receiver: Facade<UnixStream> = Facade::new(right, &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	let file = std::fs::File::open(&path).unwrap();
	let payload: Arc<Vec<u8>> = Arc::new(b"hello".to_vec());
	let sent = block_on(sender.send_all_with_fds(payload, vec![file.into()]));
	assert!(sent.result.is_ok(), "the send failed: {:?}", sent.result);

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0u8; 5]);
	let received = block_on(receiver.recv_exact_with_fds(buffer.clone()))
		.into_result()
		.expect("the receive failed");
	assert_eq!(received.bytes, 5);
	assert_eq!(&buffer[..], b"hello");
	assert_eq!(received.fds.len(), 1, "the descriptor did not come across");
	let sent = std::fs::File::from(received.fds.into_iter().next().unwrap());
	assert_eq!(sent.metadata().unwrap().len(), 5);

	submitter.stop();
	driven.join().unwrap();
}

/// A take waits for an operation that still names the socket: the receive's
/// future holds its own clone of the handle, so the socket comes back only
/// once that future is dropped.
#[test]
fn a_take_waits_for_a_receive_in_flight() {
	use std::future::Future;
	use std::io::Write;
	use std::task::{Context, Poll, Waker};

	use completion_rt::Memory;

	let (client, mut server) = pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0u8; 5]);
	let payload: Arc<dyn Memory> = Arc::clone(&buffer) as Arc<dyn Memory>;
	let mut receive = Box::pin(socket.clone().recv(payload));
	let mut take = Box::pin(socket.take());
	let mut cx = Context::from_waker(Waker::noop());

	// The peer has sent nothing, so the receive sits with the kernel and names
	// the socket.
	assert!(receive.as_mut().poll(&mut cx).is_pending());
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(
		take.as_mut().poll(&mut cx).is_pending(),
		"the receive still names the socket"
	);

	// The peer's bytes settle the receive, but the settled future keeps its
	// clone until it is dropped: the take stays pending.
	server.write_all(b"hello").unwrap();
	let mut settled = false;
	for _ in 0..500 {
		if receive.as_mut().poll(&mut cx).is_ready() {
			settled = true;
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	assert!(settled, "the receive never landed");
	assert_eq!(&buffer[..], b"hello");
	assert!(
		take.as_mut().poll(&mut cx).is_pending(),
		"the settled receive still holds the socket"
	);

	// Dropping the settled future lets the take through.
	drop(receive);
	let mut took = false;
	for _ in 0..500 {
		if let Poll::Ready(resource) = take.as_mut().poll(&mut cx) {
			assert!(resource.is_some(), "the take was already claimed");
			took = true;
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	assert!(took, "the take did not follow the dropped receive");
}

/// A take that cannot wait reports what still names the socket, and the
/// receive it was waiting on still lands on that same socket.
#[test]
fn a_take_without_waiting_reports_a_receive_in_flight() {
	use std::future::Future;
	use std::io::Write;
	use std::task::{Context, Waker};

	use completion_rt::Memory;

	let (client, mut server) = pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0u8; 5]);
	let payload: Arc<dyn Memory> = Arc::clone(&buffer) as Arc<dyn Memory>;
	let mut receive = Box::pin(socket.clone().recv(payload));
	let mut cx = Context::from_waker(Waker::noop());
	assert!(receive.as_mut().poll(&mut cx).is_pending());
	reactor.poll(Some(Duration::ZERO)).unwrap();

	let socket = match socket.try_take() {
		Ok(_) => panic!("the receive still names the socket"),
		Err(socket) => socket,
	};

	// The receive still lands on the socket it named.
	server.write_all(b"hello").unwrap();
	let mut landed = false;
	for _ in 0..500 {
		if receive.as_mut().poll(&mut cx).is_ready() {
			landed = true;
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	assert!(landed, "the receive never landed");
	assert_eq!(&buffer[..], b"hello");

	// The settled receive, once dropped, names the socket no longer.
	drop(receive);
	assert!(
		socket.try_take().is_ok(),
		"the settled receive let the socket go"
	);
}

/// A `shutdown(Write)` half-closes the connection: the peer reads end of file,
/// and what it sends afterwards still arrives — the receive side was not
/// touched.
#[test]
fn a_shutdown_write_half_closes_the_connection() {
	use std::io::Write;
	use std::net::Shutdown;

	let (client, mut server) = pair();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	socket.shutdown(Shutdown::Write).unwrap();

	// The peer's read reaches the FIN: zero bytes, not a read left waiting.
	server
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut byte = [0u8; 1];
	assert_eq!(
		server.read(&mut byte).unwrap(),
		0,
		"the peer did not read the FIN"
	);

	// The receive side is open still: the peer's bytes arrive.
	server.write_all(b"world").unwrap();
	let buffer: Arc<Vec<u8>> = Arc::new(vec![0u8; 5]);
	assert_eq!(
		block_on(socket.recv(buffer.clone())).into_result().unwrap(),
		5
	);
	assert_eq!(&buffer[..], b"world");

	submitter.stop();
	driven.join().unwrap();
}

/// A receive already in front of the kernel ends when the read side is shut
/// down: it comes back, with the zero bytes the kernel reports, instead of
/// waiting for a peer that never sends.
#[test]
fn a_shutdown_read_ends_a_receive_in_flight() {
	use std::future::Future;
	use std::net::Shutdown;
	use std::task::{Context, Poll, Waker};

	use completion_rt::Memory;

	let (client, _server) = pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0u8; 16]);
	let payload: Arc<dyn Memory> = Arc::clone(&buffer) as Arc<dyn Memory>;
	let mut receive = std::pin::pin!(socket.recv(payload));
	let mut cx = Context::from_waker(Waker::noop());
	// The peer sends nothing, so the receive sits with the kernel and only a
	// shutdown can end it.
	assert!(receive.as_mut().poll(&mut cx).is_pending());
	reactor.poll(Some(Duration::ZERO)).unwrap();

	socket.shutdown(Shutdown::Read).unwrap();

	let mut landed = None;
	for _ in 0..500 {
		if let Poll::Ready(outcome) = receive.as_mut().poll(&mut cx) {
			landed = Some(outcome);
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	let outcome = landed.expect("the shut-down receive never came back");
	assert!(
		outcome.result.is_ok(),
		"the receive ended with an error: {:?}",
		outcome.result
	);
	assert_eq!(outcome.value, 0, "the receive counted bytes nobody sent");
}

/// `take_error` hands over the socket's pending error and clears it: a refused
/// datagram shows up once, and the second call reports a clean socket.
#[test]
fn take_error_takes_a_pending_error_off_the_socket() {
	use std::net::UdpSocket;
	use std::time::Instant;

	// A port nothing holds: the kernel picks one for a bind, and the socket
	// that held it goes.
	let held = UdpSocket::bind("127.0.0.1:0").unwrap();
	let unreachable = held.local_addr().unwrap();
	drop(held);

	let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
	sender.connect(unreachable).unwrap();
	// The datagram reaches a port with no receiver, and the port-unreachable
	// comes back to the connected socket that sent it.
	sender.send(b"x").unwrap();

	let (_reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<UdpSocket> = Facade::new(sender, &submitter);

	let deadline = Instant::now() + Duration::from_secs(5);
	let pending = loop {
		if let Some(error) = socket.take_error().unwrap() {
			break error;
		}
		assert!(
			Instant::now() < deadline,
			"the refused datagram never reached the socket"
		);
		std::thread::sleep(Duration::from_millis(1));
	};
	assert_eq!(
		pending.kind(),
		std::io::ErrorKind::ConnectionRefused,
		"the pending error was not the refusal: {pending:?}"
	);
	assert!(
		socket.take_error().unwrap().is_none(),
		"the error was read without being taken away"
	);
}

/// A send already in front of the kernel ends when the write side is shut
/// down: the peer never reads, the send waits on the socket's buffers, and the
/// shutdown is what turns it into the error a closed send side gives — not a
/// write left waiting.
#[test]
fn a_shutdown_write_ends_a_send_in_flight() {
	use std::future::Future;
	use std::net::Shutdown;
	use std::task::{Context, Poll, Waker};

	let (client, _server) = pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<TcpStream> = Facade::new(client, &submitter);

	let len = 8 << 20;
	let payload: Arc<Vec<u8>> = Arc::new((0..len).map(|i| i as u8).collect());
	let mut send = std::pin::pin!(socket.send_all(payload.clone()));
	let mut cx = Context::from_waker(Waker::noop());
	assert!(send.as_mut().poll(&mut cx).is_pending());

	// Eight mebibytes never fit the socket's buffers with the peer not reading:
	// the send is still pending however many times the reactor is driven.
	for _ in 0..50 {
		assert!(
			send.as_mut().poll(&mut cx).is_pending(),
			"the payload went out with the peer never reading it"
		);
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}

	socket.shutdown(Shutdown::Write).unwrap();

	let mut landed = None;
	for _ in 0..500 {
		if let Poll::Ready(outcome) = send.as_mut().poll(&mut cx) {
			landed = Some(outcome);
			break;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	let outcome = landed.expect("the shut-down send never came back");
	assert!(
		outcome.value > 0,
		"the shut-down send threw away the bytes it had put out"
	);
	assert!(
		outcome.result.is_err(),
		"a send whose write side was shut down reports an error, not a write left waiting"
	);
}
