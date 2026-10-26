//! A resource handle names its resource and nothing else: it binds no reactor,
//! so it is `Send + Sync` and an operation uses whichever executor polls it.

mod common;

use assert2::assert;
use std::net::SocketAddr;
use std::time::Duration;

use completion_async::{
	AsDescriptor, Buffer, DatagramSocket, File, Listener, Stream, StreamSocket, block_on, timeout,
};

/// Every handle the crate exposes is `Send + Sync`: none of them names a
/// reactor, so one may be made on any thread and used from another.
#[test]
fn handles_are_send_and_sync() {
	fn assert_send_sync<T: AsDescriptor + Send + Sync>() {}
	assert_send_sync::<File>();
	assert_send_sync::<Stream>();
	assert_send_sync::<StreamSocket>();
	assert_send_sync::<DatagramSocket>();
	assert_send_sync::<Listener>();
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
	let whole: Buffer = buffer.clone();
	assert_send(file.read_at(whole.clone(), 0));
	assert_send(file.write_all_at(whole.clone(), 0));
	assert_send(file.sync());

	let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
	let stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let stream = StreamSocket::new(stream);
	assert_send(stream.recv(whole.clone()));
	assert_send(stream.send_all(whole.clone()));
	assert_send(stream.hangup());
	assert_send(StreamSocket::connect(SocketAddr::from(([127, 0, 0, 1], 9))));

	let socket = DatagramSocket::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
	assert_send(socket.recv(whole.clone()));

	let (reader, _writer) = std::io::pipe().unwrap();
	let stream = Stream::new(reader);
	assert_send(stream.readable());
	assert_send(stream.writable());

	assert_send(timeout(Duration::from_secs(1)));
}

/// A file handle made on a thread that has no runtime reads on one built on
/// another thread: the handle holds the resource, and the poll resolves the
/// reactor.
#[test]
fn a_file_made_on_one_thread_reads_on_another() {
	let directory = common::tempdir("completion-handle-thread").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let file = File::new(common::open(&path).unwrap());
	let mut buffer: Buffer = Buffer::zeroed(5);

	let (read, bytes) = std::thread::spawn(move || {
		let read = block_on(file.read_at(buffer.clone(), 0)).unwrap().unwrap();
		(read, buffer.get_mut().unwrap().to_vec())
	})
	.join()
	.unwrap();
	assert!(read == 5);
	assert!(bytes == b"hello");
}
