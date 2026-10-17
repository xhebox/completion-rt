//! End-to-end core tests against the real backend: completions land, a
//! submission keeps its buffers alive, and a cancelled operation still lands.
//!
//! The async surface drives the reactor through the executor: `Executor::block_on`
//! owns the reactor, so one call both polls the operation and drives the backend
//! under it. The reactor's own mechanics, such as the poll timeout, live in the
//! low crate's tests.
mod common;

use std::future::Future;
#[cfg(target_os = "linux")]
use std::io;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

#[cfg(unix)]
use completion_async::Facade;
use completion_async::{Buffer, Config, ErrorKind, Executor, File, Span};

/// The async driver the operations run on: it owns the reactor, and `block_on`
/// drives both the future and the backend beneath it.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// A file with `contents`, wrapped in a facade bound to `executor`.
fn seeded(executor: &Executor, contents: &[u8]) -> File {
	let dir = common::tempdir("completion-seed").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, contents).unwrap();
	// The returned descriptor outlives the deleted directory entry.
	File::with_executor(common::open(path).unwrap(), executor)
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
	let file = seeded(&executor, b"hello");
	let buf: Buffer = Buffer::zeroed(8);
	let n = executor
		.block_on(file.read_at(span(&buf), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(n, 5, "the completion is one call's bytes");
	assert_eq!(&buf.to_vec()[..5], b"hello");
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
	let facade = File::with_executor(plain, &executor);
	let buf: Buffer = Buffer::zeroed(5);
	let read = facade.read_at(span(&buf), 0);
	// The facade goes before anything reaches the kernel; the read names the
	// file through the entry's own clone of the resource.
	drop(facade);

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

	assert_eq!(executor.block_on(read).unwrap().into_result().unwrap(), 5);
	assert_eq!(buf.to_vec(), b"first", "the read landed on another file");
}

/// A dropped operation whose request never reached a `poll` still releases the
/// buffer, so the backend never sees it and the buffer goes with it.
#[test]
fn a_dropped_operation_releases_an_unsubmitted_request() {
	let mut executor = executor();
	let file = seeded(&executor, b"data");
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

/// `until` returns `Cancelled` only after the kernel has let go of the buffer:
/// the caller's handle is all that is left of it when it does, and a byte
/// written to the peer afterwards lands nowhere.
#[test]
#[cfg(unix)]
fn cancelling_waits_for_the_kernel() {
	use std::io::Write;
	use std::os::unix::net::UnixStream;
	use std::time::Duration;

	use completion_async::Cancel;

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let mut executor = executor();
	let right: Facade<UnixStream> = Facade::with_executor(right, &executor);
	let buf = Buffer::zeroed(8);
	let cancel = Cancel::new();
	let raiser = cancel.clone();
	std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(20));
		raiser.cancel();
	});
	let outcome = executor
		.block_on(right.recv(span(&buf)).until(&cancel))
		.unwrap();
	assert!(
		outcome
			.result
			.as_ref()
			.is_err_and(|error| error.kind() == ErrorKind::Cancelled)
	);
	// A write after the cancel lands nowhere: the kernel is no longer reading
	// into the buffer.
	(&left).write_all(b"late data").unwrap();
	std::thread::sleep(Duration::from_millis(20));
	assert_eq!(buf.to_vec(), &[0u8; 8], "a byte arrived after the cancel");
}

#[test]
fn concurrent_submits_all_complete() {
	let mut executor = executor();
	let mut tasks = Vec::new();
	for i in 0..8u8 {
		let file = seeded(&executor, &[i; 64]);
		let buf: Buffer = Buffer::zeroed(64);
		let read = buf.clone();
		let span = span(&read);
		tasks.push(executor.spawner().spawn(async move {
			let n = file.read_at(span, 0).await.into_result().unwrap();
			assert_eq!(n, 64);
			assert!(read.to_vec().iter().all(|&b| b == i));
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

/// The pool runs a fixed number of file operations at a time and the rest
/// wait for a worker: a caller submits as many as it likes, and every one of
/// them lands.
#[test]
fn many_transfers_all_complete() {
	let mut executor = executor();
	let file = seeded(&executor, &[7u8; 4096]);
	let mut tasks = Vec::new();
	for _ in 0..4096 {
		let buf: Buffer = Buffer::zeroed(4096);
		let read = buf.clone();
		let span = span(&read);
		let file = file.clone();
		tasks.push(executor.spawner().spawn(async move {
			let n = file.read_at(span, 0).await.into_result().unwrap();
			assert_eq!(n, 4096);
			assert!(read.to_vec().iter().all(|&byte| byte == 7));
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

/// The socket ops move bytes: one send on one end, one receive on the other.
#[cfg(unix)]
#[test]
fn a_socket_send_lands_in_the_receive_buffer() {
	use std::os::unix::net::UnixStream;

	let (left, right) = UnixStream::pair().unwrap();
	let mut executor = executor();
	let left: Facade<UnixStream> = Facade::with_executor(left, &executor);
	let right: Facade<UnixStream> = Facade::with_executor(right, &executor);

	let sent: Buffer = Buffer::from_slice(b"over the socket");
	assert_eq!(
		executor
			.block_on(left.send(span(&sent)))
			.unwrap()
			.into_result()
			.unwrap(),
		15
	);

	let received: Buffer = Buffer::zeroed(15);
	assert_eq!(
		executor
			.block_on(right.recv(span(&received)))
			.unwrap()
			.into_result()
			.unwrap(),
		15
	);
	assert_eq!(received.to_vec(), b"over the socket");
}

/// A socket op waits for data even when the descriptor says not to block:
/// the backend arms the readiness itself, so a device can keep a non-blocking
/// socket (a connect still needs one) and still transfer through the engine.
/// The completion is the bytes one call moved — four here, of the eight the
/// buffer has room for; a caller that wants the eight loops over
/// [`Facade::recv_exact`].
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
	let right: Facade<UnixStream> = Facade::with_executor(right, &executor);
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
		.into_result()
		.unwrap();
	sender.join().unwrap();
	assert_eq!(n, 4, "the completion is one call's bytes");
	assert_eq!(&buffer.to_vec()[..4], b"data");
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
	let file = seeded(&executor, b"hello");
	let buf: Buffer = Buffer::zeroed(5);
	let n = executor
		.block_on(file.read_at(span(&buf), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(n, 5);
	assert_eq!(buf.to_vec(), b"hello");
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
	let file = seeded(&executor, b"data");
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
	let file = seeded(&executor, b"data");
	match executor.block_on(file.allocate(0, 4096)).unwrap() {
		Ok(()) => panic!("allocate on an IOPOLL ring must be refused"),
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::Unsupported)
		}
		Err(other) => panic!("allocate on an IOPOLL ring must be refused, got {other:?}"),
	}
}

/// Dropping the executor stops the reactor it owns: a facade left over — every
/// clone a caller kept is one — is refused from then on, so the caller has an
/// error to act on instead of a completion nothing drives.
#[test]
fn a_submission_after_the_executor_drops_is_refused() {
	let executor = executor();
	let file = seeded(&executor, b"hello");
	let buf: Buffer = Buffer::zeroed(5);
	let span = span(&buf);
	drop(executor);
	let mut read = pin!(file.read_at(span, 0));
	let mut cx = Context::from_waker(Waker::noop());
	// Nothing drives this submission, so it either lands on this poll or never
	// does.
	let Poll::Ready(outcome) = Future::poll(read.as_mut(), &mut cx) else {
		panic!("the operation submitted after the executor dropped parked forever");
	};
	match outcome.into_result() {
		Err(error) => assert_eq!(error.kind(), ErrorKind::Stopped, "{error}"),
		Ok(count) => panic!("a gone reactor moved {count} bytes"),
	}
}
