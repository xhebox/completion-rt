//! Side-by-side comparison of `completion-async`, `compio`, `tokio`
//! (`current_thread`), and `smol` (`LocalExecutor`) across task scheduling,
//! timer churn, positional file reads, and loopback TCP transfers.
//!
//! Runtimes, descriptors, sockets, and buffers are created outside the timed
//! loops, and `compio`'s owned buffers are handed back from each iteration to
//! the next so the allocation profiler counts only what each runtime allocates
//! per operation.

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

const BLOCK: usize = 4096;
const FILE_LEN: usize = 64 << 10;

/// Creates a 64 KiB scratch file under `target/test-tmp`.
fn scratch_path() -> (tempfile::TempDir, std::path::PathBuf) {
	std::fs::create_dir_all("target/test-tmp").unwrap();
	let directory = tempfile::Builder::new()
		.prefix("bench-compare")
		.tempdir_in("target/test-tmp")
		.unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, vec![0x5au8; FILE_LEN]).unwrap();
	(directory, path)
}

/// Connected loopback TCP pair with `TCP_NODELAY` enabled.
fn tcp_pair() -> (std::net::TcpStream, std::net::TcpStream) {
	let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
	let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (server, _) = listener.accept().unwrap();
	client.set_nodelay(true).unwrap();
	server.set_nodelay(true).unwrap();
	(client, server)
}

mod spawn_join {
	use divan::Bencher;

	#[divan::bench(args = [1, 16, 64])]
	fn completion(bencher: Bencher, tasks: usize) {
		let mut handles = Vec::with_capacity(tasks);
		bencher
			.counter(divan::counter::ItemsCount::new(tasks))
			.bench_local(|| {
				completion_async::block_on(async {
					for _ in 0..tasks {
						handles.push(completion_async::spawn(async { 1usize }));
					}
					let mut sum = 0;
					for handle in handles.drain(..) {
						sum += handle.await;
					}
					assert_eq!(sum, tasks);
				})
				.unwrap();
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn compio(bencher: Bencher, tasks: usize) {
		let rt = compio::runtime::Runtime::new().unwrap();
		let mut handles = Vec::with_capacity(tasks);
		bencher
			.counter(divan::counter::ItemsCount::new(tasks))
			.bench_local(|| {
				rt.block_on(async {
					for _ in 0..tasks {
						handles.push(compio::runtime::spawn(async { 1usize }));
					}
					let mut sum = 0;
					for handle in handles.drain(..) {
						sum += handle.await.unwrap();
					}
					assert_eq!(sum, tasks);
				});
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn tokio(bencher: Bencher, tasks: usize) {
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		let mut handles = Vec::with_capacity(tasks);
		bencher
			.counter(divan::counter::ItemsCount::new(tasks))
			.bench_local(|| {
				rt.block_on(async {
					for _ in 0..tasks {
						handles.push(tokio::spawn(async { 1usize }));
					}
					let mut sum = 0;
					for handle in handles.drain(..) {
						sum += handle.await.unwrap();
					}
					assert_eq!(sum, tasks);
				});
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn smol(bencher: Bencher, tasks: usize) {
		let ex = smol::LocalExecutor::new();
		let mut handles = Vec::with_capacity(tasks);
		bencher
			.counter(divan::counter::ItemsCount::new(tasks))
			.bench_local(|| {
				smol::future::block_on(ex.run(async {
					for _ in 0..tasks {
						handles.push(ex.spawn(async { 1usize }));
					}
					let mut sum = 0;
					for handle in handles.drain(..) {
						sum += handle.await;
					}
					assert_eq!(sum, tasks);
				}));
			});
	}
}

mod timer_churn {
	use std::future::Future;
	use std::task::{Context, Waker};
	use std::time::Duration;

	use divan::Bencher;

	#[divan::bench(args = [1, 16, 64])]
	fn completion(bencher: Bencher, count: usize) {
		completion_async::block_on(async {}).unwrap();
		bencher
			.counter(divan::counter::ItemsCount::new(count))
			.bench_local(|| {
				let mut cx = Context::from_waker(Waker::noop());
				for _ in 0..count {
					let mut timer =
						std::pin::pin!(completion_async::timeout(Duration::from_secs(60)));
					assert!(timer.as_mut().poll(&mut cx).is_pending());
				}
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn compio(bencher: Bencher, count: usize) {
		let rt = compio::runtime::Runtime::new().unwrap();
		bencher
			.counter(divan::counter::ItemsCount::new(count))
			.bench_local(|| {
				rt.block_on(async {
					let mut cx = Context::from_waker(Waker::noop());
					for _ in 0..count {
						let mut timer =
							std::pin::pin!(compio::time::sleep(Duration::from_secs(60)));
						assert!(timer.as_mut().poll(&mut cx).is_pending());
					}
				});
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn tokio(bencher: Bencher, count: usize) {
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		let _guard = rt.enter();
		bencher
			.counter(divan::counter::ItemsCount::new(count))
			.bench_local(|| {
				let mut cx = Context::from_waker(Waker::noop());
				for _ in 0..count {
					let mut timer = std::pin::pin!(tokio::time::sleep(Duration::from_secs(60)));
					assert!(timer.as_mut().poll(&mut cx).is_pending());
				}
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn smol(bencher: Bencher, count: usize) {
		bencher
			.counter(divan::counter::ItemsCount::new(count))
			.bench_local(|| {
				let mut cx = Context::from_waker(Waker::noop());
				for _ in 0..count {
					let mut timer = std::pin::pin!(smol::Timer::after(Duration::from_secs(60)));
					assert!(timer.as_mut().poll(&mut cx).is_pending());
				}
			});
	}
}

mod file_read_at {
	use std::io::SeekFrom;

	use compio::io::AsyncReadAt;
	use divan::Bencher;
	use smol::io::{AsyncReadExt as _, AsyncSeekExt as _};
	use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

	use super::{BLOCK, FILE_LEN, scratch_path};

	#[divan::bench(args = [1, 16, 64])]
	fn completion(bencher: Bencher, depth: usize) {
		let (_dir, path) = scratch_path();
		let file = completion_async::File::new(
			completion_async::OpenOptions::new()
				.read(true)
				.open(&path)
				.unwrap(),
		);
		let buffers: Vec<completion_async::Buffer> = (0..depth)
			.map(|_| completion_async::Buffer::zeroed(BLOCK))
			.collect();
		let mut handles = Vec::with_capacity(depth);

		bencher
			.counter(divan::counter::ItemsCount::new(depth))
			.bench_local(|| {
				if depth == 1 {
					let n = completion_async::block_on(file.read_at(buffers[0].clone(), 0))
						.unwrap()
						.unwrap();
					assert_eq!(n, BLOCK);
				} else {
					completion_async::block_on(async {
						for (i, buf) in buffers.iter().enumerate() {
							let offset = ((i * BLOCK) % FILE_LEN) as u64;
							handles
								.push(completion_async::spawn(file.read_at(buf.clone(), offset)));
						}
						for handle in handles.drain(..) {
							assert_eq!(handle.await.unwrap(), BLOCK);
						}
					})
					.unwrap();
				}
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn compio(bencher: Bencher, depth: usize) {
		let (_dir, path) = scratch_path();
		let rt = compio::runtime::Runtime::new().unwrap();
		let file = rt.block_on(compio::fs::File::open(&path)).unwrap();
		let mut buffers: Vec<Option<Vec<u8>>> =
			(0..depth).map(|_| Some(vec![0u8; BLOCK])).collect();
		let mut handles = Vec::with_capacity(depth);

		bencher
			.counter(divan::counter::ItemsCount::new(depth))
			.bench_local(|| {
				if depth == 1 {
					let buf = buffers[0].take().unwrap();
					let compio::BufResult(res, returned) = rt.block_on(file.read_at(buf, 0));
					assert_eq!(res.unwrap(), BLOCK);
					buffers[0] = Some(returned);
				} else {
					rt.block_on(async {
						for (i, slot) in buffers.iter_mut().enumerate() {
							let offset = ((i * BLOCK) % FILE_LEN) as u64;
							let file = file.clone();
							let buf = slot.take().unwrap();
							handles.push(compio::runtime::spawn(async move {
								let compio::BufResult(res, returned) =
									file.read_at(buf, offset).await;
								assert_eq!(res.unwrap(), BLOCK);
								returned
							}));
						}
						for (slot, handle) in buffers.iter_mut().zip(handles.drain(..)) {
							*slot = Some(handle.await.unwrap());
						}
					});
				}
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn tokio(bencher: Bencher, depth: usize) {
		let (_dir, path) = scratch_path();
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		let mut files: Vec<Option<(tokio::fs::File, Vec<u8>)>> = (0..depth)
			.map(|_| {
				let std_file = std::fs::File::open(&path).unwrap();
				Some((tokio::fs::File::from_std(std_file), vec![0u8; BLOCK]))
			})
			.collect();
		let mut handles = Vec::with_capacity(depth);

		bencher
			.counter(divan::counter::ItemsCount::new(depth))
			.bench_local(|| {
				if depth == 1 {
					let (file, buf) = files[0].as_mut().unwrap();
					rt.block_on(async {
						file.seek(SeekFrom::Start(0)).await.unwrap();
						file.read_exact(buf).await.unwrap();
					});
				} else {
					rt.block_on(async {
						for (i, slot) in files.iter_mut().enumerate() {
							let offset = ((i * BLOCK) % FILE_LEN) as u64;
							let (mut file, mut buf) = slot.take().unwrap();
							handles.push(tokio::spawn(async move {
								file.seek(SeekFrom::Start(offset)).await.unwrap();
								file.read_exact(&mut buf).await.unwrap();
								(file, buf)
							}));
						}
						for (slot, handle) in files.iter_mut().zip(handles.drain(..)) {
							*slot = Some(handle.await.unwrap());
						}
					});
				}
			});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn smol(bencher: Bencher, depth: usize) {
		let (_dir, path) = scratch_path();
		let ex = smol::LocalExecutor::new();
		let mut files: Vec<Option<(smol::fs::File, Vec<u8>)>> = (0..depth)
			.map(|_| {
				let std_file = std::fs::File::open(&path).unwrap();
				Some((smol::fs::File::from(std_file), vec![0u8; BLOCK]))
			})
			.collect();
		let mut handles = Vec::with_capacity(depth);

		bencher
			.counter(divan::counter::ItemsCount::new(depth))
			.bench_local(|| {
				if depth == 1 {
					let (file, buf) = files[0].as_mut().unwrap();
					smol::future::block_on(async {
						file.seek(SeekFrom::Start(0)).await.unwrap();
						file.read_exact(buf).await.unwrap();
					});
				} else {
					smol::future::block_on(ex.run(async {
						for (i, slot) in files.iter_mut().enumerate() {
							let offset = ((i * BLOCK) % FILE_LEN) as u64;
							let (mut file, mut buf) = slot.take().unwrap();
							handles.push(ex.spawn(async move {
								file.seek(SeekFrom::Start(offset)).await.unwrap();
								file.read_exact(&mut buf).await.unwrap();
								(file, buf)
							}));
						}
						for (slot, handle) in files.iter_mut().zip(handles.drain(..)) {
							*slot = Some(handle.await);
						}
					}));
				}
			});
	}
}

mod tcp_ping_pong {
	use compio::io::{AsyncReadExt as _, AsyncWriteExt as _};
	use divan::Bencher;
	use smol::io::{AsyncReadExt as _, AsyncWriteExt as _};
	use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

	use super::tcp_pair;

	#[divan::bench(args = [64, 4096, 65536])]
	fn completion(bencher: Bencher, size: usize) {
		let (client, server) = tcp_pair();
		let client = completion_async::StreamSocket::new(client);
		let server = completion_async::StreamSocket::new(server);
		let write_buf = completion_async::Buffer::from(vec![0x5au8; size]);
		let read_buf = completion_async::Buffer::zeroed(size);

		bencher
			.counter(divan::counter::BytesCount::new(size))
			.bench_local(|| {
				completion_async::block_on(async {
					let (sent, received) = smol::future::zip(
						client.send_all(write_buf.clone()),
						server.recv_exact(read_buf.clone()),
					)
					.await;
					sent.unwrap();
					received.unwrap();
				})
				.unwrap();
			});
	}

	#[divan::bench(args = [64, 4096, 65536])]
	fn compio(bencher: Bencher, size: usize) {
		let rt = compio::runtime::Runtime::new().unwrap();
		let (client, server) = tcp_pair();
		let (mut client, mut server) = rt.block_on(async {
			(
				compio::net::TcpStream::from_std(client).unwrap(),
				compio::net::TcpStream::from_std(server).unwrap(),
			)
		});
		let mut write_buf = Some(vec![0x5au8; size]);
		let mut read_buf = Some(vec![0u8; size]);

		bencher
			.counter(divan::counter::BytesCount::new(size))
			.bench_local(|| {
				let wbuf = write_buf.take().unwrap();
				let rbuf = read_buf.take().unwrap();
				let (wbuf, rbuf) = rt.block_on(async {
					let send = async {
						let compio::BufResult(res, returned) = client.write_all(wbuf).await;
						res.unwrap();
						returned
					};
					let recv = async {
						let compio::BufResult(res, returned) = server.read_exact(rbuf).await;
						res.unwrap();
						returned
					};
					smol::future::zip(send, recv).await
				});
				write_buf = Some(wbuf);
				read_buf = Some(rbuf);
			});
	}

	#[divan::bench(args = [64, 4096, 65536])]
	fn tokio(bencher: Bencher, size: usize) {
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		let (client, server) = tcp_pair();
		client.set_nonblocking(true).unwrap();
		server.set_nonblocking(true).unwrap();
		let (mut client, mut server) = rt.block_on(async {
			(
				tokio::net::TcpStream::from_std(client).unwrap(),
				tokio::net::TcpStream::from_std(server).unwrap(),
			)
		});
		let write_buf = vec![0x5au8; size];
		let mut read_buf = vec![0u8; size];

		bencher
			.counter(divan::counter::BytesCount::new(size))
			.bench_local(|| {
				rt.block_on(async {
					let send = client.write_all(&write_buf);
					let recv = server.read_exact(&mut read_buf);
					let (s, r) = smol::future::zip(send, recv).await;
					s.unwrap();
					r.unwrap();
				});
			});
	}

	#[divan::bench(args = [64, 4096, 65536])]
	fn smol(bencher: Bencher, size: usize) {
		let (client, server) = tcp_pair();
		let mut client = smol::Async::new(client).unwrap();
		let mut server = smol::Async::new(server).unwrap();
		let write_buf = vec![0x5au8; size];
		let mut read_buf = vec![0u8; size];

		bencher
			.counter(divan::counter::BytesCount::new(size))
			.bench_local(|| {
				smol::future::block_on(async {
					let send = client.write_all(&write_buf);
					let recv = server.read_exact(&mut read_buf);
					let (s, r) = smol::future::zip(send, recv).await;
					s.unwrap();
					r.unwrap();
				});
			});
	}
}

fn main() {
	divan::main();
}
