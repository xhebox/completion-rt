//! Tests for [`PollSocket`] implementing `futures_io::{AsyncRead, AsyncWrite}`
//! and `tokio::io::{AsyncRead, AsyncWrite}`.

mod common;

use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use assert2::assert;
use completion_async::{PollSocket, StreamSocket, block_on, spawn, timeout};

fn tcp_pair() -> (PollSocket, PollSocket) {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (server, _) = listener.accept().unwrap();
	(
		StreamSocket::new(client).into_poll().unwrap(),
		StreamSocket::new(server).into_poll().unwrap(),
	)
}

#[test]
fn poll_socket_transfers_over_futures_io_traits_and_closes_cleanly() {
	use smol::io::{AsyncReadExt as _, AsyncWriteExt as _};

	common::init_logging();
	let (mut client, mut server) = tcp_pair();

	block_on(async {
		let writer = spawn(async move {
			client.write_all(b"hello futures-io").await.unwrap();
			client.flush().await.unwrap();
			client.close().await.unwrap();
			client
		});

		let mut received = Vec::new();
		server.read_to_end(&mut received).await.unwrap();
		assert!(received == b"hello futures-io");

		let client = writer.await;
		assert!(client.get_ref().take_error().unwrap().is_none());
		let _stream: StreamSocket = client.into_inner();
	})
	.unwrap();
}

#[test]
fn poll_socket_read_is_cancellation_safe_when_dropped_while_waiting() {
	use smol::io::{AsyncReadExt as _, AsyncWriteExt as _};

	common::init_logging();
	let (mut client, mut server) = tcp_pair();

	block_on(async {
		let mut buf = [0u8; 5];
		// First read waits on an empty socket and is dropped when the timer wins.
		let early = smol::future::or(
			async {
				server.read_exact(&mut buf).await.unwrap();
				true
			},
			async {
				timeout(Duration::from_millis(15)).await.unwrap();
				false
			},
		)
		.await;
		assert!(!early, "read should still be waiting before peer writes");

		// Peer writes afterwards; no bytes were lost by cancelling the readiness wait.
		client.write_all(b"world").await.unwrap();
		server.read_exact(&mut buf).await.unwrap();
		assert!(&buf == b"world");
	})
	.unwrap();
}

#[test]
fn poll_socket_transfers_over_tokio_io_traits() {
	use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

	common::init_logging();
	let (mut client, mut server) = tcp_pair();

	block_on(async {
		let writer = spawn(async move {
			client.write_all(b"hello tokio-io").await.unwrap();
			client.flush().await.unwrap();
			client.shutdown().await.unwrap();
		});

		let mut received = Vec::new();
		server.read_to_end(&mut received).await.unwrap();
		assert!(received == b"hello tokio-io");
		writer.await;
	})
	.unwrap();
}
