//! End-to-end core tests against the real backend: completions land, a
//! submission keeps its buffers alive, and a cancelled operation still lands.
//!
//! The async surface drives the reactor through the executor: `Executor::block_on`
//! owns the reactor, so one call both polls the operation and drives the backend
//! under it. The reactor's own mechanics, such as the poll timeout, live in the
//! low crate's tests.
mod common;

#[cfg(target_os = "linux")]
use std::io;

#[cfg(target_os = "linux")]
use completion_async::ErrorKind;
#[cfg(unix)]
use completion_async::Stream;
use completion_async::{Buffer, Config, Executor, File, Span};

/// The async driver the operations run on: it owns the reactor, and `block_on`
/// drives both the future and the backend beneath it.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// A file with `contents`, wrapped in a handle bound to `executor`.
fn seeded(contents: &[u8]) -> File {
	let dir = common::tempdir("completion-seed").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, contents).unwrap();
	// The returned descriptor outlives the deleted directory entry.
	File::new(common::open(path).unwrap())
}

/// The whole of `buf` in a span: the span holds its own handle on the buffer,
/// so it outlives the operation even when the caller's handle goes.
fn span(buf: &Buffer) -> Span {
	buf.whole()
}

/// A read completes with the bytes its one call moved: the file has five, the
/// buffer room for eight, and the completion is the five. A caller that wants
/// the eight loops over `File::read_exact_at`.
#[test]
fn a_read_short_of_the_buffer_completes_with_its_count() {
	let mut executor = executor();
	let file = seeded(b"hello");
	let buf: Buffer = Buffer::zeroed(8);
	let n = executor
		.block_on(file.read_at(span(&buf), 0))
		.unwrap()
		.unwrap();
	assert_eq!(n, 5, "the completion is one call's bytes");
	assert_eq!(&buf.whole().to_vec()[..5], b"hello");
}

/// An operation holds the descriptor it was submitted for. The caller's copy
/// can go while the operation is still pending: its number stays the
/// operation's, so no later `open` can take it, and the read still reaches the
/// file it named.
#[test]
#[cfg(unix)]
fn an_operation_holds_its_descriptor_past_the_callers_copy() {
	use std::os::fd::{AsRawFd, RawFd};

	let directory = common::tempdir("completion-fd-hold").unwrap();
	let first = directory.path().join("first");
	let second = directory.path().join("second");
	std::fs::write(&first, b"first").unwrap();
	std::fs::write(&second, b"second!").unwrap();
	let mut executor = executor();

	let plain = std::fs::File::open(&first).unwrap();
	let number: RawFd = plain.as_raw_fd();
	let file = File::new(plain);
	let buf: Buffer = Buffer::zeroed(5);
	let read = file.read_at(span(&buf), 0);
	// The handle goes before anything reaches the kernel; the read names the
	// file through the entry's own clone of the resource.
	drop(file);

	// `open` takes the lowest free number, so one pass over every number up to
	// the operation's own fills anything below it: were the number free, a file
	// would take it here and the read below would reach that file instead.
	let mut squatters = Vec::new();
	for _ in 0..=(number + 1).min(4096) as usize {
		let squatter = std::fs::File::open(&second).unwrap();
		assert_ne!(
			squatter.as_raw_fd(),
			number,
			"the operation's number was free while it was still pending"
		);
		squatters.push(squatter);
	}

	assert_eq!(executor.block_on(read).unwrap().unwrap(), 5);
	assert_eq!(
		buf.whole().to_vec(),
		b"first",
		"the read landed on another file"
	);
}

/// A dropped operation whose request never reached a `poll` still releases the
/// buffer, so the backend never sees it and the buffer goes with it.
#[test]
fn a_dropped_operation_releases_an_unsubmitted_request() {
	let mut executor = executor();
	let file = seeded(b"data");
	let (buf, live) = common::tracked(4);
	let read = file.read_at(buf.whole(), 0);
	drop(buf);
	assert!(live.alive(), "the request holds the buffer");
	// Nothing polled: the request is still unsubmitted, so it goes with the drop.
	drop(read);
	assert!(!live.alive(), "the request goes with it");
	executor.block_on(async {}).unwrap();
	assert!(!live.alive(), "and nothing held it after");
}

/// The pool runs a fixed number of file operations at a time and the rest
/// wait for a worker: a caller submits as many as it likes, and every one of
/// them lands.
#[test]
fn many_transfers_all_complete() {
	let mut executor = executor();
	let file = seeded(&[7u8; 4096]);
	let mut tasks = Vec::new();
	for _ in 0..4096 {
		let buf: Buffer = Buffer::zeroed(4096);
		let read = buf.clone();
		let span = span(&read);
		let file = file.clone();
		tasks.push(executor.spawn(async move {
			let n = file.read_at(span, 0).await.unwrap();
			assert_eq!(n, 4096);
			assert!(read.whole().to_vec().iter().all(|&byte| byte == 7));
		}));
	}
	executor
		.block_on(async {
			for task in tasks {
				task.await;
			}
		})
		.unwrap();
}

/// A socket op waits for data even when the descriptor says not to block:
/// the backend arms the readiness itself, so a device can keep a non-blocking
/// socket (a connect still needs one) and still transfer through the engine.
/// The completion is the bytes one call moved — four here, of the eight the
/// buffer has room for; a caller that wants the eight loops over
/// [`Stream::recv_exact`].
#[cfg(unix)]
#[test]
fn a_receive_waits_on_a_nonblocking_socket() {
	use std::io::Write;
	use std::os::unix::net::UnixStream;
	use std::time::Duration;

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let mut executor = executor();
	let right: Stream = Stream::new(right);
	let buffer: Buffer = Buffer::zeroed(8);

	// Nothing has been sent yet, so the receive stays in flight until the peer
	// writes: the non-blocking descriptor does not turn it into a short count.
	let sender = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		(&left).write_all(b"data").unwrap();
	});
	let n = executor
		.block_on(right.recv(span(&buffer)))
		.unwrap()
		.unwrap();
	sender.join().unwrap();
	assert_eq!(n, 4, "the completion is one call's bytes");
	assert_eq!(&buffer.whole().to_vec()[..4], b"data");
}

/// SQPOLL hands submissions to a kernel thread; the completions come back the
/// same way, so the caller's side of the reactor does not care.
#[test]
#[cfg(target_os = "linux")]
fn sqpoll_rings_complete_too() {
	let mut executor = match Executor::new(Config {
		sqpoll: true,
		..Config::default()
	}) {
		Ok(executor) => executor,
		// Without a kernel that grants SQPOLL there is nothing to exercise;
		// that is the host's answer, not a result to paper over.
		Err(error) => {
			let error = io::Error::from(error);
			assert!(
				matches!(
					error.kind(),
					io::ErrorKind::InvalidInput | io::ErrorKind::PermissionDenied
				),
				"unexpected SQPOLL setup error: {error}"
			);
			return;
		}
	};
	let file = seeded(b"hello");
	let buf: Buffer = Buffer::zeroed(5);
	let n = executor
		.block_on(file.read_at(span(&buf), 0))
		.unwrap()
		.unwrap();
	assert_eq!(n, 5);
	assert_eq!(buf.whole().to_vec(), b"hello");
}

/// A fsync has no overlapped form, so an IOPOLL ring refuses it instead of
/// letting the kernel answer EINVAL — and instead of silently dropping it.
#[test]
#[cfg(target_os = "linux")]
fn fsync_is_refused_on_an_iopoll_ring() {
	let mut executor = Executor::new(Config {
		iopoll: true,
		..Config::default()
	})
	.unwrap();
	let file = seeded(b"data");
	// The reactor is the backend's only caller, so the refusal lands on the
	// first poll rather than at submit time.
	match executor.block_on(file.sync()).unwrap() {
		Ok(()) => panic!("fsync on an IOPOLL ring must be refused"),
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::Unsupported)
		}
		Err(other) => panic!("fsync on an IOPOLL ring must be refused, got {other:?}"),
	}
}

/// A fallocate has no polled form either.
#[test]
#[cfg(target_os = "linux")]
fn allocate_is_refused_on_an_iopoll_ring() {
	let mut executor = Executor::new(Config {
		iopoll: true,
		..Config::default()
	})
	.unwrap();
	let file = seeded(b"data");
	match executor.block_on(file.allocate(0, 4096)).unwrap() {
		Ok(()) => panic!("allocate on an IOPOLL ring must be refused"),
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::Unsupported)
		}
		Err(other) => panic!("allocate on an IOPOLL ring must be refused, got {other:?}"),
	}
}

/// The backend is chosen at runtime, by name: on Linux `portable` swaps the
/// ring for `polling`'s readiness and `blocking`'s pool, so a regular file lands
/// on the pool and a socket on the poller, as they do on the ring.
#[test]
#[cfg(target_os = "linux")]
fn the_portable_backend_serves_a_file_and_a_socket() {
	use std::os::unix::net::UnixStream;

	use completion_async::OpenOptions;

	let mut executor = Executor::new(Config {
		backend: "portable".parse().unwrap(),
		..Config::default()
	})
	.unwrap();
	let directory = common::tempdir("completion-portable").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, [0xab; 8192]).unwrap();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);

	// A regular file has no readiness to wait on, so its transfers run on the pool.
	let buf: Buffer = Buffer::zeroed(4);
	let n = executor
		.block_on(file.read_at(span(&buf), 0))
		.unwrap()
		.unwrap();
	assert_eq!(n, 4);
	assert_eq!(buf.whole().to_vec(), [0xab; 4]);

	// A punch is a pool job too, and it cuts the hole the ring cuts.
	executor.block_on(file.punch(0, 4096)).unwrap().unwrap();
	assert_eq!(std::fs::read(&path).unwrap()[..4096], [0u8; 4096][..]);

	// A socket is a readiness source: the poller reports it, the call runs.
	let (left, right) = UnixStream::pair().unwrap();
	let left: Stream = Stream::new(left);
	let right: Stream = Stream::new(right);
	let payload: Buffer = Buffer::from(b"portable");
	assert_eq!(
		executor
			.block_on(left.send(span(&payload)))
			.unwrap()
			.unwrap(),
		8
	);
	let received: Buffer = Buffer::zeroed(8);
	assert_eq!(
		executor
			.block_on(right.recv(span(&received)))
			.unwrap()
			.unwrap(),
		8
	);
	assert_eq!(received.whole().to_vec(), b"portable");
}

/// An accept on the portable backend, on every target: the listener is a
/// readiness source, the connection comes back as a stream, and the two ends
/// are live both ways.
#[test]
fn the_portable_backend_accepts_a_connection() {
	use std::io::{Read, Write};
	use std::net::{TcpListener, TcpStream};

	use completion_async::{Listener, Stream};

	let mut executor = Executor::new(Config {
		backend: "portable".parse().unwrap(),
		..Config::default()
	})
	.unwrap();
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let address = listener.local_addr().unwrap();
	listener.set_nonblocking(true).unwrap();
	let listener: Listener = Listener::new(listener);
	let mut client = TcpStream::connect(address).unwrap();
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
	let mut got = [0u8; 5];
	client.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");

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

	// The accepted socket is the backend's to run: a unix one carries the flags
	// a watched descriptor needs, non-blocking and closed on exec.
	#[cfg(unix)]
	{
		let socket = stream
			.try_take()
			.unwrap_or_else(|| panic!("a settled operation still named the socket"));
		let flags = rustix::fs::fcntl_getfl(&socket).unwrap();
		assert!(flags.contains(rustix::fs::OFlags::NONBLOCK));
		let descriptor = rustix::io::fcntl_getfd(&socket).unwrap();
		assert!(descriptor.contains(rustix::io::FdFlags::CLOEXEC));
	}
}
