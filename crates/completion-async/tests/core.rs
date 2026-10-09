//! End-to-end core tests against the real backend: completions land, a
//! submission keeps its buffers alive, and a cancelled operation still lands.
//!
//! The async surface drives the reactor through the executor: `Executor::block_on`
//! owns the reactor, so one call both polls the operation and drives the backend
//! under it. The reactor's own mechanics — the poll timeout, the descriptor a
//! caller waits on, the event a caller raises — live in the low crate's tests.
mod common;

use common::block_on;

use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[cfg(unix)]
use completion_async::Facade;
use completion_async::{Config, Error, Executor, File, Handle, Memory, Submitter};

/// The async driver the operations run on: it owns the reactor, and `block_on`
/// drives both the future and the backend beneath it.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// A file with `contents`, wrapped in a facade bound to `submitter`.
fn seeded(submitter: &Submitter, contents: &[u8]) -> File {
	let dir = common::tempdir("completion-seed").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, contents).unwrap();
	// The returned descriptor outlives the deleted directory entry.
	File::new(common::open(path).unwrap(), submitter)
}

/// The buffer in the shape the engine names memory: a second handle on it, so
/// it outlives the operation.
fn memory(buf: &Arc<Vec<u8>>) -> Arc<dyn Memory> {
	let memory: Arc<dyn Memory> = buf.clone();
	memory
}

/// A read completes with the bytes its one call moved: the file has five, the
/// buffer room for eight, and the completion is the five. A caller that wants
/// the eight loops over `File::read_exact_at`.
#[test]
fn a_read_short_of_the_buffer_completes_with_its_count() {
	let mut executor = executor();
	let file = seeded(&executor.submitter(), b"hello");
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 8]);
	let n = executor
		.block_on(file.read_at(memory(&buf), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(n, 5, "the completion is one call's bytes");
	assert_eq!(&buf[..5], b"hello");
}

/// An operation holds the descriptor it was submitted for. The caller's copy
/// can go while the operation is still pending: its number stays the
/// operation's, so no later `open` can take it, and the read still reaches the
/// file it named.
#[test]
#[cfg(unix)]
fn an_operation_holds_its_descriptor_past_the_callers_copy() {
	use std::os::fd::{AsRawFd, RawFd};

	use completion_async::AsDescriptor as _;

	let directory = common::tempdir("completion-fd-hold").unwrap();
	let first = directory.path().join("first");
	let second = directory.path().join("second");
	std::fs::write(&first, b"first").unwrap();
	std::fs::write(&second, b"second!").unwrap();
	let mut executor = executor();

	let handle = Handle::new(std::fs::File::open(&first).unwrap());
	let number: RawFd = handle.as_descriptor().as_raw_fd();
	let facade: Facade<std::fs::File> = Facade::bind(handle.clone(), &executor.submitter());
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let read = facade.read_at(memory(&buf), 0);
	// The caller's copies go before anything reaches the kernel; the read names
	// the file through its own clone of the handle.
	drop(facade);
	drop(handle);

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
	assert_eq!(&buf[..], b"first", "the read landed on another file");
}

/// A dropped operation whose request never reached a `poll` still releases the
/// buffer, so the backend never sees it and the buffer goes with it.
#[test]
fn a_dropped_operation_releases_an_unsubmitted_request() {
	let mut executor = executor();
	let file = seeded(&executor.submitter(), b"data");
	let buf = Arc::new(vec![0u8; 4]);
	let read = file.read_at(memory(&buf), 0);
	assert_eq!(Arc::strong_count(&buf), 2, "the request holds the buffer");
	// Nothing polled: the request is still unsubmitted, so it goes with the drop.
	drop(read);
	assert_eq!(Arc::strong_count(&buf), 1, "the request goes with it");
	executor.block_on(async {}).unwrap();
	assert_eq!(Arc::strong_count(&buf), 1, "and nothing held it after");
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
	let right: Facade<UnixStream> = Facade::new(right, &executor.submitter());
	let buf = Arc::new(vec![0u8; 8]);
	let cancel = Cancel::new();
	let raiser = cancel.clone();
	std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(20));
		raiser.cancel();
	});
	let outcome = executor
		.block_on(right.recv(memory(&buf)).until(&cancel))
		.unwrap();
	assert!(matches!(outcome.result, Err(Error::Cancelled)));
	// The kernel is done, so only the caller's handle remains.
	assert_eq!(Arc::strong_count(&buf), 1);
	// A write after the cancel lands nowhere: the kernel is no longer reading
	// into the buffer.
	(&left).write_all(b"late data").unwrap();
	std::thread::sleep(Duration::from_millis(20));
	assert_eq!(&buf[..], &[0u8; 8], "a byte arrived after the cancel");
}

#[test]
fn concurrent_submits_all_complete() {
	let mut executor = executor();
	let submitter = executor.submitter();
	let mut tasks = Vec::new();
	for i in 0..8u8 {
		let file = seeded(&submitter, &[i; 64]);
		let buf: Arc<Vec<u8>> = Arc::new(vec![0; 64]);
		let read = Arc::clone(&buf);
		tasks.push(executor.spawner().spawn(async move {
			let n = file.read_at(memory(&read), 0).await.into_result().unwrap();
			assert_eq!(n, 64);
			assert!(read.iter().all(|&b| b == i));
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
	let submitter = executor.submitter();
	let file = seeded(&submitter, &[7u8; 4096]);
	let mut tasks = Vec::new();
	for _ in 0..4096 {
		let buf: Arc<Vec<u8>> = Arc::new(vec![0u8; 4096]);
		let read = Arc::clone(&buf);
		let file = file.clone();
		tasks.push(executor.spawner().spawn(async move {
			let n = file.read_at(memory(&read), 0).await.into_result().unwrap();
			assert_eq!(n, 4096);
			assert!(read.iter().all(|&byte| byte == 7));
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
	let submitter = executor.submitter();
	let left: Facade<UnixStream> = Facade::new(left, &submitter);
	let right: Facade<UnixStream> = Facade::new(right, &submitter);

	let sent: Arc<Vec<u8>> = Arc::new(b"over the socket".to_vec());
	assert_eq!(
		executor
			.block_on(left.send(memory(&sent)))
			.unwrap()
			.into_result()
			.unwrap(),
		15
	);

	let received: Arc<Vec<u8>> = Arc::new(vec![0; 15]);
	assert_eq!(
		executor
			.block_on(right.recv(memory(&received)))
			.unwrap()
			.into_result()
			.unwrap(),
		15
	);
	assert_eq!(&received[..], b"over the socket");
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
	let right: Facade<UnixStream> = Facade::new(right, &executor.submitter());
	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; 8]);

	// Nothing has been sent yet, so the receive stays in flight until the peer
	// writes: the non-blocking descriptor does not turn it into a short count.
	let sender = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		(&left).write_all(b"data").unwrap();
	});
	let n = executor
		.block_on(right.recv(memory(&buffer)))
		.unwrap()
		.into_result()
		.unwrap();
	sender.join().unwrap();
	assert_eq!(n, 4, "the completion is one call's bytes");
	assert_eq!(&buffer[..4], b"data");
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
	let file = seeded(&executor.submitter(), b"hello");
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let n = executor
		.block_on(file.read_at(memory(&buf), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(n, 5);
	assert_eq!(&buf[..], b"hello");
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
	let file = seeded(&executor.submitter(), b"data");
	// The reactor is the backend's only caller, so the refusal lands on the
	// first poll rather than at submit time.
	match executor.block_on(file.sync()).unwrap() {
		Ok(()) => panic!("fsync on an IOPOLL ring must be refused"),
		Err(Error::Io(error)) => assert_eq!(error.kind(), io::ErrorKind::Unsupported),
		Err(other) => panic!("fsync on an IOPOLL ring must be refused, got {other:?}"),
	}
}

/// Dropping the executor stops the reactor it owns: a facade left over — every
/// clone a caller kept is one — is refused from then on, so the caller has an
/// error to act on instead of a completion nothing drives.
#[test]
fn a_submission_after_the_executor_drops_is_refused() {
	let executor = executor();
	let submitter = executor.submitter();
	let file = seeded(&submitter, b"hello");
	drop(executor);
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let mut read = pin!(file.read_at(memory(&buf), 0));
	let mut cx = Context::from_waker(Waker::noop());
	// Nothing drives this submission, so it either lands on this poll or never
	// does.
	let Poll::Ready(outcome) = Future::poll(read.as_mut(), &mut cx) else {
		panic!("the operation submitted after the executor dropped parked forever");
	};
	match outcome.into_result() {
		Err(Error::Io(error)) => assert_eq!(error.kind(), io::ErrorKind::BrokenPipe),
		Err(error) => panic!("the refusal was not a broken pipe: {error}"),
		Ok(count) => panic!("a gone reactor moved {count} bytes"),
	}
}

thread_local! {
	/// The take a rendezvous wake re-polls; one test drives it at a time.
	static PENDING_TAKE: std::cell::RefCell<Option<std::pin::Pin<Box<dyn Future<Output = Option<usize>>>>>> =
		std::cell::RefCell::new(None);
	static TAKE_OUTCOME: std::cell::RefCell<Option<Option<usize>>> = const { std::cell::RefCell::new(None) };
}

/// Re-polls the parked take, so what the taker sees is the state at the wake
/// rather than after it: this is what a release-before-wake ordering is
/// observable through, and a wake raised one drop too early is not.
fn repoll() {
	// A wake may still arrive while the thread-locals this reaches into are
	// being torn down, which a plain `with` would abort on.
	let Some(mut take) = PENDING_TAKE
		.try_with(|slot| slot.borrow_mut().take())
		.ok()
		.flatten()
	else {
		return;
	};
	let waker = Waker::from(Arc::new(Repoll));
	let mut cx = Context::from_waker(&waker);
	// The take is out of the slot while it is polled, so a release the poll
	// raises cannot re-enter this with it borrowed.
	match take.as_mut().poll(&mut cx) {
		Poll::Ready(value) => {
			let _ = TAKE_OUTCOME.try_with(|slot| *slot.borrow_mut() = Some(value));
		}
		Poll::Pending => {
			let _ = PENDING_TAKE.try_with(|slot| *slot.borrow_mut() = Some(take));
		}
	}
}

struct Repoll;

impl std::task::Wake for Repoll {
	fn wake(self: Arc<Self>) {
		repoll();
	}
}

/// A take waits for a user clone exactly as long as it holds the value: it
/// resolves on the wake the last drop raises, with the value already released.
#[test]
fn a_take_resolves_when_the_other_clone_is_dropped() {
	let handle = Handle::new(0usize);
	let other = handle.clone();
	PENDING_TAKE.with(|slot| *slot.borrow_mut() = Some(Box::pin(handle.take())));

	// The first poll parks the take: the clone still holds the value.
	repoll();
	assert_eq!(
		TAKE_OUTCOME.with(|slot| slot.borrow().clone()),
		None,
		"the clone still names the value"
	);

	drop(other);
	assert_eq!(
		TAKE_OUTCOME.with(|slot| slot.borrow().clone()),
		Some(Some(0)),
		"the wake found the value still held: the release did not come first"
	);
}

/// One taker at a time: a second take on the same handle gives up at once, and
/// its clone is what frees the value for the take already waiting.
#[test]
fn a_second_take_gives_up_while_one_waits() {
	let handle = Handle::new(1usize);
	let other = handle.clone();
	let mut first = Box::pin(handle.take());
	let mut cx = Context::from_waker(Waker::noop());
	assert!(first.as_mut().poll(&mut cx).is_pending());

	assert_eq!(block_on(other.take()), None);

	assert_eq!(first.as_mut().poll(&mut cx), Poll::Ready(Some(1)));
}

/// A resource becomes a handle through `From` as well as through `new`.
#[test]
fn a_handle_is_built_from_its_resource() {
	let directory = common::tempdir("completion-from").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"data").unwrap();
	let file: Handle<std::fs::File> = Handle::from(std::fs::File::open(&path).unwrap());
	let file = match file.try_take() {
		Ok(file) => file,
		Err(_) => panic!("nothing else names the file"),
	};
	assert_eq!(file.metadata().unwrap().len(), 4);
}
