//! Runtime costs over the page cache and a pipe: scheduling, timer churn,
//! buffer hand-off, and cancellation. Nothing here syncs to a device, so the
//! numbers measure the runtime rather than the disk.

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

mod executor {
	use std::future::Future;
	use std::task::{Context, Poll, Waker};
	use std::time::Duration;

	use completion_async::{block_on, spawn, timeout};
	use divan::Bencher;

	#[divan::bench]
	fn block_on_ready(bencher: Bencher) {
		bencher.bench_local(|| {
			assert_eq!(block_on(async { divan::black_box(42usize) }).unwrap(), 42);
		});
	}

	#[divan::bench(args = [1, 16, 64])]
	fn spawn_join(bencher: Bencher, tasks: usize) {
		let mut handles = Vec::with_capacity(tasks);
		bencher
			.counter(divan::counter::ItemsCount::new(tasks))
			.bench_local(|| {
				block_on(async {
					for _ in 0..tasks {
						handles.push(spawn(async { 1usize }));
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
	fn yield_now(bencher: Bencher, yields: usize) {
		bencher
			.counter(divan::counter::ItemsCount::new(yields))
			.bench_local(|| {
				block_on(async {
					let mut remaining = yields;
					std::future::poll_fn(|cx| {
						if remaining == 0 {
							Poll::Ready(())
						} else {
							remaining -= 1;
							cx.waker().wake_by_ref();
							Poll::Pending
						}
					})
					.await;
				})
				.unwrap();
			});
	}

	/// Each timeout registers on its first poll and is dropped before it
	/// expires, the common case for an I/O that completes in time.
	#[divan::bench(args = [1, 16, 64])]
	fn timer_churn(bencher: Bencher, count: usize) {
		// The first `block_on` installs the thread's runtime outside the timed loop.
		block_on(async {}).unwrap();
		bencher
			.counter(divan::counter::ItemsCount::new(count))
			.bench_local(|| {
				let mut cx = Context::from_waker(Waker::noop());
				for _ in 0..count {
					let mut timer = std::pin::pin!(timeout(Duration::from_secs(60)));
					assert!(timer.as_mut().poll(&mut cx).is_pending());
				}
			});
	}
}

mod io {
	use std::future::Future;
	use std::task::Poll;

	use completion_async::{Buffer, File, OpenOptions, Stream, block_on, spawn};
	use divan::Bencher;

	const BLOCK: usize = 4096;
	const FILE_LEN: usize = 64 << 10;

	#[divan::bench]
	fn buffer_clone_and_get_mut(bencher: Bencher) {
		let mut buffer = Buffer::zeroed(BLOCK);
		bencher.bench_local(|| {
			let clone = divan::black_box(buffer.clone());
			drop(clone);
			divan::black_box(buffer.get_mut().unwrap());
		});
	}

	/// Reads `depth` blocks from a page-cached file at once: one call at
	/// depth 1, and spawned reads beyond that.
	#[divan::bench(args = [1, 16, 64])]
	fn file_read_at(bencher: Bencher, depth: usize) {
		std::fs::create_dir_all("target/test-tmp").unwrap();
		let directory = tempfile::Builder::new()
			.prefix("bench-runtime")
			.tempdir_in("target/test-tmp")
			.unwrap();
		let path = directory.path().join("data");
		std::fs::write(&path, vec![0x37u8; FILE_LEN]).unwrap();
		let file = File::new(OpenOptions::new().read(true).open(&path).unwrap());
		let buffers: Vec<Buffer> = (0..depth).map(|_| Buffer::zeroed(BLOCK)).collect();
		let mut handles = Vec::with_capacity(depth);

		bencher
			.counter(divan::counter::ItemsCount::new(depth))
			.bench_local(|| {
				if depth == 1 {
					let n = block_on(file.read_at(buffers[0].clone(), 0))
						.unwrap()
						.unwrap();
					assert_eq!(n, BLOCK);
				} else {
					block_on(async {
						for (i, buf) in buffers.iter().enumerate() {
							let offset = ((i * BLOCK) % FILE_LEN) as u64;
							handles.push(spawn(file.read_at(buf.clone(), offset)));
						}
						for handle in handles.drain(..) {
							assert_eq!(handle.await.unwrap(), BLOCK);
						}
					})
					.unwrap();
				}
			});
	}

	#[divan::bench(args = [64, 4096, 65536])]
	fn pipe_ping_pong(bencher: Bencher, size: usize) {
		let (reader, writer) = Stream::pipe().unwrap();
		let write_buf = Buffer::from(vec![0x5au8; size]);
		let read_buf = Buffer::zeroed(size);
		bencher
			.counter(divan::counter::BytesCount::new(size))
			.bench_local(|| {
				block_on(async {
					let write = spawn({
						let writer = writer.clone();
						let write_buf = write_buf.clone();
						async move { writer.write_all(write_buf).await.unwrap() }
					});
					reader.read_exact(read_buf.clone()).await.unwrap();
					write.await;
				})
				.unwrap();
			});
	}

	/// Submits a read on an empty pipe, drops it in flight, and times how long
	/// the reactor takes to hand the buffer back once the kernel reports the
	/// cancellation.
	#[divan::bench]
	fn cancel_in_flight_read(bencher: Bencher) {
		let (reader, _writer) = Stream::pipe().unwrap();
		let mut buffer = Buffer::zeroed(64);
		bencher.bench_local(|| {
			let mut read = Some(Box::pin(reader.read(buffer.clone())));
			block_on(std::future::poll_fn(|cx| {
				if let Some(mut in_flight) = read.take() {
					assert!(in_flight.as_mut().poll(cx).is_pending());
					// Dropping `in_flight` queues the cancel while keeping `cx.waker()`
					// registered on the entry, so the completion wakes this root future.
					drop(in_flight);
					return Poll::Pending;
				}
				assert!(buffer.get_mut().is_some());
				Poll::Ready(())
			}))
			.unwrap();
		});
	}
}

fn main() {
	divan::main();
}
