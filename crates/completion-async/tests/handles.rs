//! A resource handle names its resource and nothing else: it binds no reactor,
//! so it is `Send + Sync` and an operation uses whichever executor polls it.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use completion_async::{
	Buffer, Config, Executor, File, Listener, Readiness, Socket, Span, Stream, timeout,
};

/// Every handle the crate exposes is `Send + Sync`: none of them names a
/// reactor, so one may be made on any thread and used from another. A `Scope`
/// is what crosses threads in the other direction: it carries a token and no
/// thread-local state.
#[test]
fn handles_are_send_and_sync() {
	fn assert_send_sync<T: Send + Sync>() {}
	assert_send_sync::<File>();
	assert_send_sync::<Stream>();
	assert_send_sync::<Socket>();
	assert_send_sync::<Listener>();
	assert_send_sync::<Readiness>();
}

/// Every operation over a handle is `Send + 'static`: the reactor it submits to
/// is a handle of its own, so an operation can be polled on one thread and
/// dropped or polled again on another.
#[test]
fn operations_are_send_and_static() {
	fn assert_send<F: Send + 'static>(future: F) {
		drop(future);
	}

	let directory = common::tempdir("completion-send").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"x").unwrap();
	let file = File::new(common::open(&path).unwrap());
	let buffer = Buffer::zeroed(1);
	let span: Span = buffer.whole();
	assert_send(file.read_at(span.clone(), 0));
	assert_send(file.write_all_at(span.clone(), 0));
	assert_send(file.sync());

	let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
	let stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let stream = Stream::new(stream);
	assert_send(stream.recv(span.clone()));
	assert_send(stream.send_all(span.clone()));
	assert_send(stream.hangup());
	assert_send(Stream::connect(SocketAddr::from(([127, 0, 0, 1], 9))));

	let socket = Socket::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
	assert_send(socket.recv(span.clone()));

	let (reader, _writer) = std::io::pipe().unwrap();
	let readiness = Readiness::new(reader);
	assert_send(readiness.readable());

	assert_send(timeout(Duration::from_secs(1)));
}

/// A file handle made on a thread that has no executor reads on one built on
/// another thread: the handle holds the resource, and the poll resolves the
/// reactor.
#[test]
fn a_file_made_on_one_thread_reads_on_another() {
	let directory = common::tempdir("completion-handle-thread").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let file = File::new(common::open(&path).unwrap());
	let buffer: Buffer = Buffer::zeroed(5);

	let (read, bytes) = std::thread::spawn(move || {
		let mut executor = Executor::new(Config::default()).unwrap();
		let read = executor
			.block_on(file.read_at(buffer.whole(), 0))
			.unwrap()
			.unwrap();
		(read, buffer.whole().to_vec())
	})
	.join()
	.unwrap();
	assert_eq!(read, 5);
	assert_eq!(bytes, b"hello");
}
