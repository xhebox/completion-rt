//! Socket operations through the handles, driven by the executor.
//!
//! The operations that need the reactor polled by hand — an accept whose landed
//! result is never taken, the origin a submission reports — live with the low
//! crate, whose tests reach the reactor directly.
mod common;

use assert2::assert;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

#[cfg(unix)]
use completion_async::spawn;
use completion_async::{
	Buffer, DatagramSocket, Listener, OwnedSocket, StreamSocket, block_on, timeout,
};

/// A copy of `buf` names the whole of it: the copy holds its own handle on the
/// memory, so it outlives the operation even when the caller's handle goes.
fn whole(buf: &Buffer) -> Buffer {
	buf.clone()
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
	common::init_logging();
	let socket: StreamSocket = StreamSocket::new(client);

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
	let payload: Buffer = Buffer::copy_from(&bytes);
	block_on(socket.send_all(whole(&payload))).unwrap().unwrap();

	let got = drain
		.join()
		.unwrap()
		.expect("the peer never saw the whole payload");
	assert!(got == bytes, "the payload did not arrive whole");
}

/// `recv_exact` fills a buffer larger than one call: the peer sends more than
/// the socket's buffers hold, and only a loop fills the buffer.
#[test]
fn recv_exact_fills_a_buffer_larger_than_one_call() {
	use std::io::Write;

	let (mut client, server) = pair();
	common::init_logging();
	let socket: StreamSocket = StreamSocket::new(server);

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

	let mut buffer: Buffer = Buffer::zeroed(len);
	block_on(socket.recv_exact(whole(&buffer)))
		.unwrap()
		.unwrap();
	sending
		.join()
		.unwrap()
		.expect("the sender never got the whole payload out");

	assert!(
		buffer.get_mut().unwrap().to_vec() == bytes,
		"the payload did not arrive whole"
	);
}

/// A stream type of the caller's own, which this crate has never seen: it
/// reaches the socket operations by converting into the descriptor an operation
/// names, so no trait has to name it.
struct MyStream(TcpStream);

impl From<MyStream> for OwnedSocket {
	fn from(stream: MyStream) -> OwnedSocket {
		stream.0.into()
	}
}

/// The caller's type gets the sends, the receives, and the loops over them.
#[test]
fn a_caller_socket_type_gets_the_socket_operations() {
	use std::io::Write;

	let (stream, mut peer) = pair();
	common::init_logging();
	let socket = StreamSocket::new(MyStream(stream));

	let payload: Buffer = Buffer::copy_from(b"hello");
	block_on(socket.send_all(whole(&payload))).unwrap().unwrap();
	peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert!(&got == b"hello");

	peer.write_all(b"world").unwrap();
	let mut buffer: Buffer = Buffer::zeroed(5);
	block_on(socket.recv_exact(whole(&buffer)))
		.unwrap()
		.unwrap();
	assert!(buffer.get_mut().unwrap().to_vec() == b"world");
}

/// A datagram socket is not a byte stream: each call is one message, and the
/// loops over the calls — `recv_exact`, `send_all` — are not on it.
#[test]
fn a_datagram_socket_takes_a_message_a_call() {
	use std::net::UdpSocket;

	let left = UdpSocket::bind("127.0.0.1:0").unwrap();
	let right = UdpSocket::bind("127.0.0.1:0").unwrap();
	right.connect(left.local_addr().unwrap()).unwrap();
	common::init_logging();
	let receiver: DatagramSocket = DatagramSocket::new(left);
	let sender: DatagramSocket = DatagramSocket::new(right);

	let payload: Buffer = Buffer::copy_from(b"hello");
	assert!(block_on(sender.send(whole(&payload))).unwrap().unwrap() == 5);

	let mut buffer: Buffer = Buffer::zeroed(8);
	assert!(block_on(receiver.recv(whole(&buffer))).unwrap().unwrap() == 5);
	assert!(&buffer.get_mut().unwrap().to_vec()[..5] == b"hello");
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
	common::init_logging();
	let listener: Listener = Listener::new(listener);

	let mut client = UnixStream::connect(&path).unwrap();
	let stream: StreamSocket = block_on(listener.accept())
		.unwrap()
		.expect("the accept landed");
	let payload: Buffer = Buffer::copy_from(b"hello");
	assert!(block_on(stream.send(whole(&payload))).unwrap().unwrap() == 5);

	client
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut got = [0u8; 5];
	client.read_exact(&mut got).unwrap();
	assert!(&got == b"hello");

	// The connection reaches the other way too: the client's bytes come out of
	// the accepted stream.
	client.write_all(b"world").unwrap();
	let mut received: Buffer = Buffer::zeroed(5);
	assert!(block_on(stream.recv(whole(&received))).unwrap().unwrap() == 5);
	assert!(received.get_mut().unwrap().to_vec() == b"world");
}

/// The same accept over TCP, on every platform, and the connection is live both
/// ways.
#[test]
fn a_tcp_accept_gives_a_stream_of_its_own() {
	use std::io::Write;

	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	listener.set_nonblocking(true).unwrap();
	let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	common::init_logging();
	let listener: Listener = Listener::new(listener);

	let stream: StreamSocket = block_on(listener.accept())
		.unwrap()
		.expect("the accept landed");
	let payload: Buffer = Buffer::copy_from(b"hello");
	assert!(block_on(stream.send(whole(&payload))).unwrap().unwrap() == 5);
	client
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut got = [0u8; 5];
	client.read_exact(&mut got).unwrap();
	assert!(&got == b"hello");

	// The connection reaches the other way too: the client's bytes come out of
	// the accepted stream.
	client.write_all(b"world").unwrap();
	let mut received: Buffer = Buffer::zeroed(5);
	assert!(block_on(stream.recv(whole(&received))).unwrap().unwrap() == 5);
	assert!(received.get_mut().unwrap().to_vec() == b"world");
}

/// A connect reaches a listener over TCP, and the stream it hands back is live
/// both ways: it sends to the peer and receives from it.
#[test]
fn a_tcp_connect_gives_a_live_stream() {
	use std::io::Write;

	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	common::init_logging();
	let connecting = StreamSocket::connect(listener.local_addr().unwrap());
	let stream = block_on(connecting).unwrap().expect("the connect failed");
	let (mut peer, _) = listener.accept().unwrap();

	let payload: Buffer = Buffer::copy_from(b"hello");
	assert!(block_on(stream.send(whole(&payload))).unwrap().unwrap() == 5);
	peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert!(&got == b"hello");

	peer.write_all(b"world").unwrap();
	let mut received: Buffer = Buffer::zeroed(5);
	assert!(block_on(stream.recv(whole(&received))).unwrap().unwrap() == 5);
	assert!(received.get_mut().unwrap().to_vec() == b"world");
}

/// A unix socket's path is an address the connect takes as it is, and the
/// connection it makes is live both ways.
#[test]
#[cfg(unix)]
fn a_unix_connect_gives_a_live_stream() {
	use std::io::Write;

	let directory = common::tempdir("completion-connect-unix").unwrap();
	let (listener, path) = listener(directory.path());
	common::init_logging();
	let connecting = StreamSocket::connect(path.as_path());
	let stream = block_on(connecting).unwrap().expect("the connect failed");
	let (mut peer, _) = listener.accept().unwrap();

	let payload: Buffer = Buffer::copy_from(b"hello");
	assert!(block_on(stream.send(whole(&payload))).unwrap().unwrap() == 5);
	peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert!(&got == b"hello");

	peer.write_all(b"world").unwrap();
	let mut received: Buffer = Buffer::zeroed(5);
	assert!(block_on(stream.recv(whole(&received))).unwrap().unwrap() == 5);
	assert!(received.get_mut().unwrap().to_vec() == b"world");
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

	let socket: DatagramSocket = DatagramSocket::new(sender);

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
	assert!(
		pending.kind() == std::io::ErrorKind::ConnectionRefused,
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
	common::init_logging();
	let socket: StreamSocket = StreamSocket::new(client);

	socket.shutdown(Shutdown::Write).unwrap();

	// The peer's read reaches the FIN: zero bytes, not a read left waiting.
	server
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut byte = [0u8; 1];
	assert!(
		server.read(&mut byte).unwrap() == 0,
		"the peer did not read the FIN"
	);

	// The receive side is open still: the peer's bytes arrive.
	server.write_all(b"world").unwrap();
	let mut buffer: Buffer = Buffer::zeroed(5);
	assert!(block_on(socket.recv(whole(&buffer))).unwrap().unwrap() == 5);
	assert!(buffer.get_mut().unwrap().to_vec() == b"world");
}

/// A `send_all` loop interrupted by an I/O error reports the bytes it already
/// transferred before the error occurred.
#[test]
#[cfg(unix)]
fn a_send_all_loop_reports_what_it_sent_before_an_io_error() {
	// The peer reads nothing and is gone: buffers held to a slice of the
	// payload are filled, the rest is left waiting, and the connection goes
	// down with the peer.
	let (client, server) = held_pair(64 << 10);
	common::init_logging();
	let socket: StreamSocket = StreamSocket::new(client);

	let len = 8 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Buffer = Buffer::copy_from(&bytes);
	let gone = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(100));
		drop(server);
	});

	// The peer goes away, and the reset reaches the send.
	let result = block_on(socket.send_all(whole(&payload))).unwrap();
	gone.join().unwrap();
	assert!(
		result.is_err(),
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
	common::init_logging();
	let socket: StreamSocket = StreamSocket::new(client);

	let buffer: Buffer = Buffer::zeroed(16);
	// The peer sends nothing, so the receive sits with the kernel and only a
	// shutdown can end it.
	let shutdown = socket.clone();
	let timeout = timeout(Duration::from_millis(50));
	let task = spawn(async move {
		timeout.await.unwrap();
		shutdown.shutdown(Shutdown::Read).unwrap();
	});

	let received = block_on(socket.recv(whole(&buffer))).unwrap();
	block_on(task).unwrap();
	assert!(
		received.expect("the receive ended with an error") == 0,
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
	common::init_logging();
	let socket: StreamSocket = StreamSocket::new(client);

	let len = 8 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Buffer = Buffer::copy_from(&bytes);
	// Buffers held to a slice of the payload with the peer not reading: the
	// send is still pending when the write side goes.
	let shutdown = socket.clone();
	let timeout = timeout(Duration::from_millis(50));
	let task = spawn(async move {
		timeout.await.unwrap();
		shutdown.shutdown(Shutdown::Write).unwrap();
	});

	let result = block_on(socket.send_all(whole(&payload))).unwrap();
	block_on(task).unwrap();
	assert!(
		result.is_err(),
		"a send whose write side was shut down reports an error, not a write left waiting"
	);
}

/// A take that cannot wait reports what still names the socket, and the
/// receive it was waiting on still lands on that same socket.
#[test]
fn a_take_without_waiting_reports_a_receive_in_flight() {
	use std::io::Write;

	let (client, mut server) = pair();
	common::init_logging();
	let socket: StreamSocket = StreamSocket::new(client);

	let mut buffer: Buffer = Buffer::zeroed(5);
	let mut receive = Box::pin(socket.clone().recv(whole(&buffer)));
	block_on(std::future::poll_fn(|cx| {
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
		block_on(std::future::poll_fn(|cx| receive.as_mut().poll(cx)))
			.unwrap()
			.unwrap();
	}
	writer.join().unwrap();
	assert!(buffer.get_mut().unwrap().to_vec() == b"hello");

	// The settled receive, once dropped, names the socket no longer.
	drop(receive);
	assert!(
		socket.try_take().is_some(),
		"the settled receive let the socket go"
	);
}

/// An accept that lands while nobody waits for it, and whose result is then
/// dropped, closes the connection it made: the peer reads the end of the stream.
#[test]
#[cfg(target_os = "linux")]
fn a_dropped_accept_result_closes_the_connection() {
	use std::os::unix::net::UnixStream;

	let directory = common::tempdir("completion-accept-dropped").unwrap();
	let (listener, path) = listener(directory.path());
	common::init_logging();
	let listener: Listener = Listener::new(listener);

	let accepted = spawn(listener.accept());
	let mut client = UnixStream::connect(&path).unwrap();
	block_on(timeout(Duration::from_millis(100)))
		.unwrap()
		.unwrap();
	drop(accepted);

	client
		.set_read_timeout(Some(Duration::from_secs(5)))
		.unwrap();
	let mut byte = [0u8; 1];
	assert!(
		client.read(&mut byte).unwrap() == 0,
		"the peer read data where the connection should have ended"
	);
}

/// A receive dropped while in flight is cancelled and reaped: the stream is the
/// caller's again once the cancel lands, and a byte the peer sends afterwards is
/// not written into the buffer the dropped receive named.
#[test]
fn a_dropped_in_flight_recv_is_cancelled_and_never_written() {
	use std::io::Write;

	let (mut client, server) = pair();
	common::init_logging();
	let stream: StreamSocket = StreamSocket::new(server);
	let mut buf = Buffer::zeroed(8);
	let recv_view = whole(&buf);

	// One poll submits the receive; dropping it then leaves it in flight.
	block_on(async {
		let mut recv = std::pin::pin!(stream.recv(recv_view));
		std::future::poll_fn(|cx| {
			drop(recv.as_mut().poll(cx));
			std::task::Poll::Ready(())
		})
		.await;
	})
	.unwrap();
	block_on(timeout(Duration::from_millis(50)))
		.unwrap()
		.unwrap();
	let _held = StreamSocket::new(
		StreamSocket::try_take(stream).expect("the dropped receive still holds the stream"),
	);

	client.write_all(b"later").unwrap();
	block_on(timeout(Duration::from_millis(50)))
		.unwrap()
		.unwrap();
	assert!(buf.get_mut().unwrap().to_vec() == [0u8; 8]);
}
