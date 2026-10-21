//! Socket operations through the handles, driven by the executor.
//!
//! The operations that need the reactor polled by hand — an accept whose landed
//! result is never taken, the origin a submission reports — live with the low
//! crate, whose tests reach the reactor directly.
mod common;

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use completion_async::{
	BackendKind, Buffer, Config, ErrorKind, Executor, Listener, OwnedDescriptor, Socket, Span,
	Stream,
};

/// The async driver the socket operations run on.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// The whole of `buf` in a span: the span holds its own handle on the buffer,
/// so it outlives the operation even when the caller's handle goes.
fn span(buf: &Buffer) -> Span {
	buf.whole()
}

/// A connected pair over loopback: the portable way to two real sockets.
fn pair() -> (TcpStream, TcpStream) {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (server, _) = listener.accept().unwrap();
	(client, server)
}

/// A connected pair whose buffers are held to `bytes` from before anything
/// flows, so a send larger than them is left waiting in the kernel instead of
/// being buffered whole — which is what the platform's defaults do here.
///
/// Unix only: windows completes a loopback send to a peer that never reads
/// whole, whatever the buffers are held to, so there is no send left in flight
/// there for the tests below to act on.
#[cfg(unix)]
fn held_pair(bytes: usize) -> (TcpStream, TcpStream) {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	cap_buffers(&listener, bytes);
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	cap_buffers(&client, bytes);
	let (server, _) = listener.accept().unwrap();
	cap_buffers(&server, bytes);
	(client, server)
}

/// Holds `socket`'s send and receive buffers to `bytes`.
#[cfg(unix)]
fn cap_buffers(socket: &impl std::os::fd::AsFd, bytes: usize) {
	use rustix::net::sockopt::{set_socket_recv_buffer_size, set_socket_send_buffer_size};

	assert!(set_socket_send_buffer_size(socket, bytes).is_ok());
	assert!(set_socket_recv_buffer_size(socket, bytes).is_ok());
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

/// `send_all` moves a payload larger than one call: eight mebibytes do not fit
/// the socket's buffers, the peer drains while the send goes, and only a loop
/// puts all of it on the wire.
#[test]
fn send_all_moves_a_payload_larger_than_one_call() {
	let (client, mut server) = pair();
	let mut executor = executor();
	let socket: Stream = Stream::new(client);

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

	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Buffer = Buffer::from(bytes.as_slice());
	executor
		.block_on(socket.send_all(span(&payload)))
		.unwrap()
		.into_result()
		.unwrap();

	let got = drain
		.join()
		.unwrap()
		.expect("the peer never saw the whole payload");
	assert_eq!(got, bytes, "the payload did not arrive whole");
}

/// `recv_exact` fills a buffer larger than one call: the peer sends more than
/// the socket's buffers hold, and only a loop fills the buffer.
#[test]
fn recv_exact_fills_a_buffer_larger_than_one_call() {
	use std::io::Write;

	let (mut client, server) = pair();
	let mut executor = executor();
	let socket: Stream = Stream::new(server);

	// The write timeout bounds the sender, so a receive that stops short fails
	// this test instead of parking the sender thread.
	let len = 8 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let sender = bytes.clone();
	let sending = std::thread::spawn(move || {
		client
			.set_write_timeout(Some(Duration::from_secs(10)))
			.unwrap();
		client.write_all(&sender)
	});

	let buffer: Buffer = Buffer::zeroed(len);
	executor
		.block_on(socket.recv_exact(span(&buffer)))
		.unwrap()
		.into_result()
		.unwrap();
	sending
		.join()
		.unwrap()
		.expect("the sender never got the whole payload out");

	assert_eq!(
		buffer.whole().to_vec(),
		bytes,
		"the payload did not arrive whole"
	);
}

/// A stream type of the caller's own, which this crate has never seen: it
/// reaches the socket operations by converting into the descriptor an operation
/// names, so no trait has to name it.
struct MyStream(TcpStream);

impl From<MyStream> for OwnedDescriptor {
	fn from(stream: MyStream) -> OwnedDescriptor {
		stream.0.into()
	}
}

/// The caller's type gets the sends, the receives, and the loops over them.
#[test]
fn a_caller_socket_type_gets_the_socket_operations() {
	use std::io::Write;

	let (stream, mut peer) = pair();
	let mut executor = executor();
	let socket = Stream::new(MyStream(stream));

	let payload: Buffer = Buffer::from(b"hello");
	assert_eq!(
		executor
			.block_on(socket.send_all(span(&payload)))
			.unwrap()
			.into_result()
			.unwrap(),
		5
	);
	peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

	peer.write_all(b"world").unwrap();
	let buffer: Buffer = Buffer::zeroed(5);
	assert_eq!(
		executor
			.block_on(socket.recv_exact(span(&buffer)))
			.unwrap()
			.into_result()
			.unwrap(),
		5
	);
	assert_eq!(buffer.whole().to_vec(), b"world");
}

/// A datagram socket is not a byte stream: each call is one message, and the
/// loops over the calls — `recv_exact`, `send_all` — are not on it.
#[test]
fn a_datagram_socket_takes_a_message_a_call() {
	use std::net::UdpSocket;

	let left = UdpSocket::bind("127.0.0.1:0").unwrap();
	let right = UdpSocket::bind("127.0.0.1:0").unwrap();
	right.connect(left.local_addr().unwrap()).unwrap();
	let mut executor = executor();
	let receiver: Socket = Socket::new(left);
	let sender: Socket = Socket::new(right);

	let payload: Buffer = Buffer::from(b"hello");
	assert_eq!(
		executor
			.block_on(sender.send(span(&payload)))
			.unwrap()
			.unwrap(),
		5
	);

	let buffer: Buffer = Buffer::zeroed(8);
	assert_eq!(
		executor
			.block_on(receiver.recv(span(&buffer)))
			.unwrap()
			.unwrap(),
		5
	);
	assert_eq!(&buffer.whole().to_vec()[..5], b"hello");
}

/// An accept hands back the connection as a stream of its own. The connection
/// is live both ways: the accepted stream sends to the peer and receives from
/// it.
#[test]
#[cfg(unix)]
fn an_accept_gives_a_stream_of_its_own() {
	use std::io::Write;
	use std::os::unix::net::UnixStream;

	let directory = common::tempdir("completion-accept-stream").unwrap();
	let (listener, path) = listener(directory.path());
	let mut executor = executor();
	let listener: Listener = Listener::new(listener);

	let mut client = UnixStream::connect(&path).unwrap();
	let stream: Stream = executor
		.block_on(listener.accept())
		.unwrap()
		.expect("the accept landed");
	let payload: Buffer = Buffer::from(b"hello");
	assert_eq!(
		executor
			.block_on(stream.send(span(&payload)))
			.unwrap()
			.unwrap(),
		5
	);

	client
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut got = [0u8; 5];
	client.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

	// The connection reaches the other way too: the client's bytes come out of
	// the accepted stream.
	client.write_all(b"world").unwrap();
	let received: Buffer = Buffer::zeroed(5);
	assert_eq!(
		executor
			.block_on(stream.recv(span(&received)))
			.unwrap()
			.unwrap(),
		5
	);
	assert_eq!(received.whole().to_vec(), b"world");
}

/// The same accept over TCP, on every platform, and the connection is live both
/// ways.
#[test]
fn a_tcp_accept_gives_a_stream_of_its_own() {
	use std::io::Write;

	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	listener.set_nonblocking(true).unwrap();
	let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let mut executor = executor();
	let listener: Listener = Listener::new(listener);

	let stream: Stream = executor
		.block_on(listener.accept())
		.unwrap()
		.expect("the accept landed");
	let payload: Buffer = Buffer::from(b"hello");
	assert_eq!(
		executor
			.block_on(stream.send(span(&payload)))
			.unwrap()
			.unwrap(),
		5
	);
	client
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut got = [0u8; 5];
	client.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

	// The connection reaches the other way too: the client's bytes come out of
	// the accepted stream.
	client.write_all(b"world").unwrap();
	let received: Buffer = Buffer::zeroed(5);
	assert_eq!(
		executor
			.block_on(stream.recv(span(&received)))
			.unwrap()
			.unwrap(),
		5
	);
	assert_eq!(received.whole().to_vec(), b"world");
}

/// A connect reaches a listener over TCP, and the stream it hands back is live
/// both ways: it sends to the peer and receives from it.
#[test]
fn a_tcp_connect_gives_a_live_stream() {
	use std::io::Write;

	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let mut executor = executor();
	let connecting = Stream::connect(listener.local_addr().unwrap());
	let stream = executor
		.block_on(connecting)
		.unwrap()
		.expect("the connect failed");
	let (mut peer, _) = listener.accept().unwrap();

	let payload: Buffer = Buffer::from(b"hello");
	assert_eq!(
		executor
			.block_on(stream.send(span(&payload)))
			.unwrap()
			.unwrap(),
		5
	);
	peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

	peer.write_all(b"world").unwrap();
	let received: Buffer = Buffer::zeroed(5);
	assert_eq!(
		executor
			.block_on(stream.recv(span(&received)))
			.unwrap()
			.unwrap(),
		5
	);
	assert_eq!(received.whole().to_vec(), b"world");
}

/// A unix socket's path is an address the connect takes as it is, and the
/// connection it makes is live both ways.
#[test]
#[cfg(unix)]
fn a_unix_connect_gives_a_live_stream() {
	use std::io::Write;

	let directory = common::tempdir("completion-connect-unix").unwrap();
	let (listener, path) = listener(directory.path());
	let mut executor = executor();
	let connecting = Stream::connect(path.as_path());
	let stream = executor
		.block_on(connecting)
		.unwrap()
		.expect("the connect failed");
	let (mut peer, _) = listener.accept().unwrap();

	let payload: Buffer = Buffer::from(b"hello");
	assert_eq!(
		executor
			.block_on(stream.send(span(&payload)))
			.unwrap()
			.unwrap(),
		5
	);
	peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

	peer.write_all(b"world").unwrap();
	let received: Buffer = Buffer::zeroed(5);
	assert_eq!(
		executor
			.block_on(stream.recv(span(&received)))
			.unwrap()
			.unwrap(),
		5
	);
	assert_eq!(received.whole().to_vec(), b"world");
}

/// The portable backend connects by readiness: the handshake it left in flight
/// is answered by the socket turning writable, and the stream it hands back is
/// live both ways.
#[test]
fn a_portable_connect_gives_a_live_stream() {
	use std::io::Write;

	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	common::init_logging();
	let mut executor = Executor::new(Config {
		backend: BackendKind::Portable,
		..Config::default()
	})
	.unwrap();
	let connecting = Stream::connect(listener.local_addr().unwrap());
	let stream = executor
		.block_on(connecting)
		.unwrap()
		.expect("the connect failed");
	let (mut peer, _) = listener.accept().unwrap();

	let payload: Buffer = Buffer::from(b"hello");
	assert_eq!(
		executor
			.block_on(stream.send(span(&payload)))
			.unwrap()
			.unwrap(),
		5
	);
	peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

	peer.write_all(b"world").unwrap();
	let received: Buffer = Buffer::zeroed(5);
	assert_eq!(
		executor
			.block_on(stream.recv(span(&received)))
			.unwrap()
			.unwrap(),
		5
	);
	assert_eq!(received.whole().to_vec(), b"world");
}

/// A connect given up on its scope reports the cancel: the socket it made is
/// dropped with it, and nothing of the connection reaches the kernel.
#[test]
fn a_given_up_connect_reports_the_cancel() {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let mut executor = executor();
	let scope = executor.scope();
	// The scope fired before the connect was asked for, so the first poll gives
	// it up without ever reaching the kernel.
	drop(scope.cancel());
	let connecting = Stream::connect(listener.local_addr().unwrap());
	let error = executor
		.block_on(async { scope.spawn(connecting).await })
		.unwrap()
		.err()
		.expect("a given-up connect resolved");
	assert_eq!(error.kind(), ErrorKind::Cancelled);
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
	let mut executor = executor();
	let sender: Stream = Stream::new(left);
	let receiver: Stream = Stream::new(right);

	let file = std::fs::File::open(&path).unwrap();
	let payload: Buffer = Buffer::from(b"hello");
	let sent = executor
		.block_on(sender.send_all_with_fds(span(&payload), vec![file.into()]))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(sent, 5, "the send moved a different count");

	let buffer: Buffer = Buffer::zeroed(5);
	let received = executor
		.block_on(receiver.recv_exact_with_fds(span(&buffer)))
		.unwrap()
		.into_result()
		.expect("the receive failed");
	assert_eq!(received.bytes, 5);
	assert_eq!(buffer.whole().to_vec(), b"hello");
	assert_eq!(received.fds.len(), 1, "the descriptor did not come across");
	let sent = std::fs::File::from(received.fds.into_iter().next().unwrap());
	assert_eq!(sent.metadata().unwrap().len(), 5);
}

/// `take_error` hands over the socket's pending error and clears it: a refused
/// datagram shows up once, and the second call reports a clean socket.
///
/// Windows: the refused datagram is not reported through `SO_ERROR`; it
/// surfaces as the error of the socket's next operation instead.
#[cfg(unix)]
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

	let socket: Socket = Socket::new(sender);

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

/// A `shutdown(Write)` half-closes the connection: the peer reads end of file,
/// and what it sends afterwards still arrives — the receive side was not
/// touched.
#[test]
fn a_shutdown_write_half_closes_the_connection() {
	use std::io::Write;
	use std::net::Shutdown;

	let (client, mut server) = pair();
	let mut executor = executor();
	let socket: Stream = Stream::new(client);

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
	let buffer: Buffer = Buffer::zeroed(5);
	assert_eq!(
		executor
			.block_on(socket.recv(span(&buffer)))
			.unwrap()
			.unwrap(),
		5
	);
	assert_eq!(buffer.whole().to_vec(), b"world");
}

/// A cancel covers the whole `recv_exact` loop: a cancel that lands after the
/// first call has moved bytes, but before the memory is full, ends the loop
/// there — the bytes it took are its progress, and the cancel is why it stopped.
#[test]
#[cfg(unix)]
fn a_cancel_ends_a_recv_exact_loop_mid_way() {
	use std::io::Write;
	use std::os::unix::net::UnixStream;

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let mut executor = executor();
	let socket: Stream = Stream::new(right);

	let buffer: Buffer = Buffer::zeroed(8);
	// Four bytes are one call: the loop takes them and comes back for the rest,
	// which the peer never sends.
	(&left).write_all(b"ABCD").unwrap();
	let scope = executor.scope();
	let raiser = scope.clone();
	let thread = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(100));
		drop(raiser.cancel());
	});

	let outcome = executor
		.block_on(async { scope.spawn(socket.recv_exact(span(&buffer))).await })
		.unwrap();
	thread.join().unwrap();
	assert_eq!(outcome.value, 4, "the loop kept the bytes it had taken");
	assert!(
		outcome
			.result
			.as_ref()
			.is_err_and(|error| error.kind() == ErrorKind::Cancelled),
		"a given-up loop is not the loop's own result: {:?}",
		outcome.result
	);
	assert_eq!(
		&buffer.whole().to_vec()[..4],
		b"ABCD",
		"the first call's bytes stay"
	);
	assert_eq!(
		&buffer.whole().to_vec()[4..],
		&[0u8; 4],
		"the loop wrote past what it was given"
	);
}

/// A cancel that lands in a `send_all` loop reports the bytes the loop had put
/// out: the progress is not thrown away with the cancel.
///
/// Windows: a loopback send to a peer that never reads completes whole, so the
/// loop is never left in front of the kernel for the cancel to end.
#[cfg(unix)]
#[test]
fn a_cancel_ends_a_send_all_loop_with_its_progress() {
	// The peer is held, and never reads: the buffers take a few calls' worth
	// and the loop is left with the kernel. They are held to a slice of the
	// payload from before the connection is made, so eight mebibytes cannot all
	// leave whatever the platform's defaults would buffer.
	let (client, _server) = held_pair(64 << 10);
	let mut executor = executor();
	let socket: Stream = Stream::new(client);

	let len = 8 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Buffer = Buffer::from(bytes.as_slice());
	let scope = executor.scope();
	let raiser = scope.clone();
	let thread = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(100));
		drop(raiser.cancel());
	});

	let outcome = executor
		.block_on(async { scope.spawn(socket.send_all(span(&payload))).await })
		.unwrap();
	thread.join().unwrap();
	assert!(
		outcome.value > 0,
		"the cancel threw away the bytes the loop had sent"
	);
	assert!(
		outcome
			.result
			.as_ref()
			.is_err_and(|error| error.kind() == ErrorKind::Cancelled),
		"a given-up loop is not the loop's own result: {:?}",
		outcome.result
	);
}

/// A `send_all` loop that fails keeps the progress it made before the call that
/// failed: the peer going away is the call's error, not a reason to forget the
/// bytes that left.
///
/// Windows: a loopback send to a peer that never reads completes whole, so the
/// peer going away leaves no send for the reset to fail.
#[cfg(unix)]
#[test]
fn a_send_all_loop_reports_what_it_sent_before_an_io_error() {
	// The peer reads nothing and is gone: buffers held to a slice of the
	// payload are filled, the rest is left waiting, and the connection goes
	// down with the peer.
	let (client, server) = held_pair(64 << 10);
	let mut executor = executor();
	let socket: Stream = Stream::new(client);

	let len = 8 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Buffer = Buffer::from(bytes.as_slice());
	let gone = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(100));
		drop(server);
	});

	// The peer goes away, and the reset reaches the send: what it had taken off
	// the memory is still the progress it reports.
	let outcome = executor.block_on(socket.send_all(span(&payload))).unwrap();
	gone.join().unwrap();
	assert!(outcome.value > 0, "the failed send dropped its progress");
	assert!(
		outcome.result.is_err(),
		"a send whose peer went away reports an error"
	);
}

/// A receive already in front of the kernel ends when the read side is shut
/// down: it comes back, with the zero bytes the kernel reports, instead of
/// waiting for a peer that never sends.
///
/// Windows: shutting down the read side leaves a receive already in front of
/// the kernel waiting, so there is nothing to come back.
#[cfg(unix)]
#[test]
fn a_shutdown_read_ends_a_receive_in_flight() {
	use completion_async::timeout;
	use std::net::Shutdown;

	let (client, _server) = pair();
	let mut executor = executor();
	let socket: Stream = Stream::new(client);

	let buffer: Buffer = Buffer::zeroed(16);
	// The peer sends nothing, so the receive sits with the kernel and only a
	// shutdown can end it.
	let shutdown = socket.clone();
	let timeout = timeout(Duration::from_millis(50));
	let task = executor.spawn(async move {
		timeout.await.unwrap();
		shutdown.shutdown(Shutdown::Read).unwrap();
	});

	let received = executor.block_on(socket.recv(span(&buffer))).unwrap();
	executor.block_on(task).unwrap();
	assert_eq!(
		received.expect("the receive ended with an error"),
		0,
		"the receive counted bytes nobody sent"
	);
}

/// A send already in front of the kernel ends when the write side is shut
/// down: the peer never reads, the send waits on the socket's buffers, and the
/// shutdown is what turns it into the error a closed send side gives — not a
/// write left waiting.
///
/// Windows: a loopback send to a peer that never reads completes whole, so
/// there is no send in front of the kernel for the shutdown to end.
#[cfg(unix)]
#[test]
fn a_shutdown_write_ends_a_send_in_flight() {
	use completion_async::timeout;
	use std::net::Shutdown;

	let (client, _server) = held_pair(64 << 10);
	let mut executor = executor();
	let socket: Stream = Stream::new(client);

	let len = 8 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Buffer = Buffer::from(bytes.as_slice());
	// Buffers held to a slice of the payload with the peer not reading: the
	// send is still pending when the write side goes.
	let shutdown = socket.clone();
	let timeout = timeout(Duration::from_millis(50));
	let task = executor.spawn(async move {
		timeout.await.unwrap();
		shutdown.shutdown(Shutdown::Write).unwrap();
	});

	let outcome = executor.block_on(socket.send_all(span(&payload))).unwrap();
	executor.block_on(task).unwrap();
	assert!(
		outcome.value > 0,
		"the shut-down send threw away the bytes it had put out"
	);
	assert!(
		outcome.result.is_err(),
		"a send whose write side was shut down reports an error, not a write left waiting"
	);
}

/// A take that cannot wait reports what still names the socket, and the
/// receive it was waiting on still lands on that same socket.
#[test]
fn a_take_without_waiting_reports_a_receive_in_flight() {
	use std::io::Write;

	let (client, mut server) = pair();
	let mut executor = executor();
	let socket: Stream = Stream::new(client);

	let buffer: Buffer = Buffer::zeroed(5);
	let mut receive = Box::pin(socket.clone().recv(span(&buffer)));
	executor
		.block_on(std::future::poll_fn(|cx| {
			assert!(receive.as_mut().poll(cx).is_pending());
			std::task::Poll::Ready(())
		}))
		.unwrap();

	// `try_take` consumes the handle, so probe with a clone: the receive holds
	// its own clone of the descriptor and the probe cannot take it out.
	let probe = socket.clone();
	assert!(
		probe.try_take().is_none(),
		"the receive still names the socket"
	);

	// The receive still lands on the socket it named.
	let writer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		server.write_all(b"hello").unwrap();
	});
	{
		let receive = &mut receive;
		executor
			.block_on(std::future::poll_fn(|cx| receive.as_mut().poll(cx)))
			.unwrap()
			.unwrap();
	}
	writer.join().unwrap();
	assert_eq!(buffer.whole().to_vec(), b"hello");

	// The settled receive, once dropped, names the socket no longer.
	drop(receive);
	assert!(
		socket.try_take().is_some(),
		"the settled receive let the socket go"
	);
}
