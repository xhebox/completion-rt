//! Cancellation through the async surface: the [`Scope`] an operation runs
//! under, and the token tree and wait a scope exposes.
//!
//! What a drop does — cancel the operation and detach it, without waiting for
//! the backend to let go of the caller's memory — is a reactor concern the
//! async layer cannot reach: its tests live in the low crate. The tests wait on
//! a socket pair, so the whole file is unix: a port that cannot be made to wait
//! has nothing to cancel.
#![cfg(unix)]

mod common;

use std::future::Future;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use completion_async::{Buffer, Config, ErrorKind, Executor, File, Span, Stream, timeout};

/// The async driver the operations run on.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// The whole of `buf` in a span: the span holds its own handle on the buffer,
/// so it outlives the operation even when the caller's handle goes.
fn span(buf: &Buffer) -> Span {
	buf.whole()
}

/// A socket pair with both ends non-blocking: a receive on one waits in front
/// of the kernel until the peer writes, or until a cancel gives it up.
fn pair() -> (UnixStream, Stream) {
	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	(left, Stream::new(right))
}

/// A scope returns the operation's own result when it completes first, even
/// with a scope that has not been cancelled.
#[test]
fn a_scope_returns_the_op_that_lands_first() {
	let dir = common::tempdir("completion-scope-ok").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let mut executor = executor();
	let file = File::new(std::fs::File::open(path).unwrap());

	let buf = Buffer::zeroed(5);
	let scope = executor.scope();
	let read = executor
		.block_on(async { scope.spawn(file.read_at(span(&buf), 0)).await })
		.unwrap()
		.unwrap();
	assert_eq!(read, 5, "the read completed first");
	assert_eq!(buf.whole().to_vec(), b"hello");
}

/// A task spawned in a scope runs in it: the I/O it submits is given up when
/// the scope is cancelled, while an operation outside the scope — here one run
/// in the executor's root scope — is left to its peer.
#[test]
fn a_spawned_task_takes_the_cancel_of_the_scope_it_was_spawned_in() {
	let mut executor = executor();
	let (scoped_left, scoped_right) = pair();
	let (mut root_left, root_right) = pair();
	let scope = executor.scope();

	let (task_buf, task_live) = common::tracked(8);
	// The spawn resolves the executor `block_on` runs, so it has to happen inside
	// it; one pass over the executor then submits the spawned receive, so it is in
	// flight before the cancel lands.
	let mut task = None;
	executor
		.block_on(async {
			let spawned = scope.spawn(async move { scoped_right.recv(task_buf.whole()).await });
			timeout(Duration::from_millis(1)).await.unwrap();
			task = Some(spawned);
		})
		.unwrap();
	let task = task.expect("the root future spawned its task");

	// The other receive belongs to no child scope, only the root one, and is in
	// flight too.
	let (root_buf, _root_live) = common::tracked(8);
	let mut receive = Box::pin(root_right.recv(root_buf.whole()));
	executor
		.block_on(std::future::poll_fn(|cx| {
			assert!(receive.as_mut().poll(cx).is_pending());
			std::task::Poll::Ready(())
		}))
		.unwrap();

	drop(scope.cancel());

	// The task's receive ends with the scope it was spawned in...
	let error = executor.block_on(task).unwrap().unwrap_err();
	assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
	assert!(
		!task_live.alive(),
		"the kernel let go of the cancelled buffer"
	);

	// ...while the operation outside the scope is still waiting for its peer.
	root_left.write_all(b"hello").unwrap();
	let read = executor
		.block_on(std::future::poll_fn(|cx| receive.as_mut().poll(cx)))
		.unwrap()
		.unwrap();
	assert_eq!(read, 5, "the operation outside the scope was cancelled too");
	assert_eq!(&root_buf.whole().to_vec()[..5], b"hello");
	let _ = scoped_left;
}

/// Cancelling a parent cancels its children; cancelling a child leaves the
/// parent alone; and a child of an already-raised parent starts raised.
#[test]
fn a_child_scope_follows_its_parent() {
	let parent = completion_async::scope();
	let child = parent.child();
	assert!(!parent.is_cancelled());
	assert!(!child.is_cancelled());
	drop(child.cancel());
	assert!(child.is_cancelled());
	assert!(!parent.is_cancelled(), "a child does not cancel its parent");
	drop(parent.cancel());
	assert!(child.is_cancelled(), "a parent cancels its child");

	let grandchild = parent.child().child();
	assert!(
		grandchild.is_cancelled(),
		"a child of a raised parent is raised"
	);

	let later = parent.child();
	assert!(
		later.is_cancelled(),
		"a child made after the raise is raised"
	);
}

/// A `cancel()` on another thread raises the reactor's wake-up, so a thread
/// parked in `block_on` comes back and the scoped operation resolves.
#[test]
fn a_cancel_from_another_thread_wakes_a_parked_block_on() {
	let mut executor = executor();
	let (left, right) = pair();
	let (buf, live) = common::tracked(8);
	let scope = executor.scope();
	let raiser = scope.clone();
	let thread = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		drop(raiser.cancel());
	});
	let started = Instant::now();
	let result = executor
		.block_on(async { scope.spawn(right.recv(buf.whole())).await })
		.unwrap();
	thread.join().unwrap();
	assert_eq!(
		result.expect_err("the cancelled receive resolved").kind(),
		ErrorKind::Cancelled
	);
	assert!(
		started.elapsed() < Duration::from_secs(5),
		"the parked block_on slept through the cancel"
	);
	drop(buf);
	assert!(!live.alive(), "the kernel let go of the buffer");
	let _ = left;
}

/// One scope spans every executor its operations run on, not just the one it
/// names for spawning: a fire on a third thread has to reach both parked
/// `block_on`s, so each registers its own reactor's wake-up with the token.
#[test]
fn a_cancel_shared_by_two_executors_wakes_both() {
	let shared = completion_async::scope();
	let (done, finished) = std::sync::mpsc::channel();
	for _ in 0..2 {
		let shared = shared.clone();
		let done = done.clone();
		std::thread::spawn(move || {
			let mut executor = executor();
			let (left, right) = pair();
			let (buf, live) = common::tracked(8);
			let started = Instant::now();
			let result = executor
				.block_on(async { shared.spawn(right.recv(buf.whole())).await })
				.unwrap();
			// `left` outlives the receive: a peer that went would end it with EOF,
			// not leave it stalled for the cancel to reach.
			let _ = left;
			drop(buf);
			let _ = done.send((result, started.elapsed(), live));
		});
	}
	let raiser = shared.clone();
	let raising = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		drop(raiser.cancel());
	});
	raising.join().unwrap();
	for _ in 0..2 {
		let (result, elapsed, live) = finished
			.recv_timeout(Duration::from_secs(5))
			.expect("a parked block_on never woke: the shared cancel was lost");
		assert_eq!(
			result.expect_err("the cancelled receive resolved").kind(),
			ErrorKind::Cancelled
		);
		assert!(
			elapsed < Duration::from_secs(1),
			"an executor parked in block_on slept through the shared cancel: {elapsed:?}"
		);
		assert!(!live.alive(), "the kernel let go of the buffer");
	}
}

/// A timer given a scope resolves with the cancel, not with its deadline: the
/// reactor disarms it and the future reports `Cancelled` instead of waiting an
/// hour.
#[test]
fn a_cancelled_timeout_reports_the_cancel() {
	let mut executor = executor();
	let scope = executor.scope();
	let raiser = scope.clone();
	let raising = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		drop(raiser.cancel());
	});
	let timeout = timeout(Duration::from_secs(3600));
	let started = Instant::now();
	let result = executor
		.block_on(async { scope.spawn(timeout).await })
		.unwrap();
	raising.join().unwrap();
	let error = result.expect_err("the cancelled timeout resolved instead of reporting the cancel");
	assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
	assert!(
		started.elapsed() < Duration::from_secs(5),
		"the timeout slept to its deadline instead of taking the cancel"
	);
}
