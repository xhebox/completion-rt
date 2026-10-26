//! Running `completion-async` I/O futures directly inside foreign executors
//! (`tokio` current-thread, `tokio` multi-threaded work-stealing, and `smol`).

mod common;

use std::io;
use std::net::TcpListener;
use std::sync::Once;
use std::time::{Duration, Instant};

use assert2::assert;
use completion_async::{Buffer, Config, File, Listener, OpenOptions, StreamSocket, init_reactor};

fn ensure_reactor() {
	static INIT: Once = Once::new();
	INIT.call_once(|| {
		init_reactor(Config::default()).unwrap();
	});
}

#[test]
fn init_reactor_rejects_a_second_call_with_already_exists() {
	common::init_logging();
	ensure_reactor();
	assert!(let Err(error) = init_reactor(Config::default()));
	assert!(let Ok(io_error) = io::Error::try_from(error));
	assert!(io_error.kind() == io::ErrorKind::AlreadyExists);
}

#[test]
fn tokio_multi_thread_drives_completion_file_and_socket_operations() {
	common::init_logging();
	ensure_reactor();
	let rt = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(4)
		.enable_all()
		.build()
		.unwrap();

	let scratch = common::tempdir("foreign-tokio").unwrap();
	let path = scratch.path().join("data");
	let file = File::new(
		OpenOptions::new()
			.create(true)
			.truncate(true)
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);

	rt.block_on(async {
		// 1. Concurrent positional writes and reads across Tokio worker threads.
		let mut tasks = Vec::new();
		for i in 0..16usize {
			let file = file.clone();
			tasks.push(tokio::spawn(async move {
				let offset = (i * 256) as u64;
				let write_buf = Buffer::from(vec![i as u8; 256]);
				file.write_all_at(write_buf, offset).await.unwrap();

				let mut read_buf = Buffer::zeroed(256);
				file.read_exact_at(read_buf.clone(), offset).await.unwrap();
				assert!(read_buf.get_mut().unwrap().iter().all(|&b| b == i as u8));
			}));
		}
		for task in tasks {
			task.await.unwrap();
		}
		file.sync().await.unwrap();

		// 2. TCP accept, connect, send_all, and recv_exact inside Tokio.
		let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
		std_listener.set_nonblocking(true).unwrap();
		let addr = std_listener.local_addr().unwrap();
		let listener = Listener::new(std_listener);

		let server = tokio::spawn(async move {
			let accepted: StreamSocket = listener.accept().await.unwrap();
			let mut buf = Buffer::zeroed(11);
			accepted.recv_exact(buf.clone()).await.unwrap();
			assert!(buf.get_mut().unwrap() == b"hello tokio");
			accepted.send_all(Buffer::copy_from(b"pong")).await.unwrap();
		});

		let client = StreamSocket::connect(addr).await.unwrap();
		client
			.send_all(Buffer::copy_from(b"hello tokio"))
			.await
			.unwrap();
		let mut reply = Buffer::zeroed(4);
		client.recv_exact(reply.clone()).await.unwrap();
		assert!(reply.get_mut().unwrap() == b"pong");
		server.await.unwrap();
	});
}

#[test]
fn cancelling_in_flight_completion_io_via_tokio_timeout_reaps_and_releases_buffer() {
	common::init_logging();
	ensure_reactor();
	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let _peer = std::net::TcpStream::connect(std_listener.local_addr().unwrap()).unwrap();
	let (server, _) = std_listener.accept().unwrap();
	server.set_nonblocking(true).unwrap();
	let socket = StreamSocket::new(server);

	rt.block_on(async {
		let mut buf = Buffer::zeroed(64);
		// `tokio::time::timeout` drops the in-flight `socket.recv` after 15 ms.
		let timed = tokio::time::timeout(Duration::from_millis(15), socket.recv(buf.clone())).await;
		assert!(
			timed.is_err(),
			"recv should time out while peer stays silent"
		);

		// Wait briefly for the background reactor to cancel and reap the CQE,
		// returning exclusive ownership of `buf` and `socket`.
		let deadline = Instant::now() + Duration::from_secs(2);
		while buf.get_mut().is_none() {
			assert!(
				Instant::now() < deadline,
				"background reactor did not release cancelled buffer in time"
			);
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
		assert!(socket.try_take().is_some());
	});
}

#[test]
fn smol_drives_completion_file_operations() {
	common::init_logging();
	ensure_reactor();
	let scratch = common::tempdir("foreign-smol").unwrap();
	let path = scratch.path().join("data");
	std::fs::write(&path, b"smol-and-completion").unwrap();
	let file = File::new(OpenOptions::new().read(true).open(&path).unwrap());

	let ex = smol::LocalExecutor::new();
	smol::future::block_on(ex.run(async {
		let task = ex.spawn(async move {
			let mut buf = Buffer::zeroed(19);
			file.read_exact_at(buf.clone(), 0).await.unwrap();
			assert!(buf.get_mut().unwrap() == b"smol-and-completion");
		});
		task.await;
	}));
}
