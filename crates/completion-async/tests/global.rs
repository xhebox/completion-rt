//! The thread's global executor, as seen from outside the crate.
//!
//! The harness runs each test on its own thread, so every test starts with a
//! fresh executor.

mod common;

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, Instant};

use completion_async::{Buffer, Stream, block_on, spawn, timeout};

#[test]
fn a_task_can_spawn_a_child_and_await_it() {
	let parent = spawn(async {
		let child = spawn(async { 21 });
		child.await * 2
	});
	let result = block_on(parent);
	assert!(matches!(result, Ok(42)), "{result:?}");
}

#[test]
fn calling_block_on_inside_a_running_block_on_panics_as_nested() {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let _ = block_on(async {
			let _ = block_on(async {});
		});
	}));
	let payload = result.expect_err("a nested call must panic");
	let message = payload
		.downcast_ref::<&str>()
		.map(|s| s.to_string())
		.or_else(|| payload.downcast_ref::<String>().cloned())
		.unwrap_or_default();
	assert!(message.contains("nested"), "{message}");
}

#[test]
fn a_handle_made_inside_block_on_submits_to_the_global_executor() {
	common::init_logging();
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (mut server, _) = listener.accept().unwrap();

	let payload = Buffer::from(b"hello").whole();
	let sent = block_on(async move {
		let socket: Stream = Stream::new(client);
		socket.send_all(payload).await
	})
	.unwrap();
	assert_eq!(sent.into_result().unwrap(), 5);

	let mut got = [0u8; 5];
	server.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");
}

/// `Stream::connect` reaches the thread's default executor on its own: the
/// connection is made there, and the stream it hands back runs on the same one.
#[test]
fn a_connect_made_inside_block_on_uses_the_global_executor() {
	common::init_logging();
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let addr = listener.local_addr().unwrap();

	let payload = Buffer::from(b"hello").whole();
	let sent = block_on(async move {
		let stream = Stream::connect(addr).await.expect("the connect failed");
		stream.send_all(payload).await
	})
	.unwrap();
	assert_eq!(sent.into_result().unwrap(), 5);

	let (mut peer, _) = listener.accept().unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");
}

#[test]
fn a_timeout_made_inside_a_spawned_task_fires_under_block_on() {
	common::init_logging();
	let start = Instant::now();
	let task = spawn(async { timeout(Duration::from_millis(20)).await });
	block_on(task).unwrap().unwrap();
	assert!(start.elapsed() >= Duration::from_millis(20));
}
