//! Cancellation through the async surface: the cooperative path ([`Cancel`] and
//! `until`), and the [`Cancel`] tree and wait that the async API exposes.
//!
//! What a drop does — cancel the operation and detach it, without waiting for
//! the backend to let go of the caller's memory — is a reactor concern the
//! async layer cannot reach: its tests live in the low crate. The tests wait on
//! a socket pair, so the whole file is unix: a port that cannot be made to wait
//! has nothing to cancel.
#![cfg(unix)]

mod common;
use common::Buffer;

use std::future::Future;
use std::os::unix::net::UnixStream;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};
use std::time::{Duration, Instant};

use completion_async::{Cancel, Config, ErrorKind, Executor, Facade, File, Space, Span, Timeout};

/// The async driver the operations run on.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// The whole of `buf` in its own space: the space holds a second handle on it,
/// so it outlives the operation.
fn span(buf: &Arc<Buffer>) -> Span {
	Space::new(buf.clone()).whole()
}

/// `until` returns `Ok` when the operation completes first, even with a cancel
/// that has not fired.
#[test]
fn until_completes_when_the_op_lands_first() {
	let dir = common::tempdir("completion-until-ok").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let mut executor = executor();
	let file = File::with_executor(std::fs::File::open(path).unwrap(), &executor);

	let buf = Arc::new(Buffer::new(5));
	let cancel = Cancel::new();
	let outcome = executor
		.block_on(file.read_at(span(&buf), 0).until(&cancel))
		.unwrap();
	assert_eq!(outcome.value, 5, "the read completed first");
	assert!(outcome.result.is_ok(), "the cancel never fired");
	assert_eq!(buf.to_vec(), b"hello");
}

/// Cancelling a parent cancels its children; cancelling a child leaves the
/// parent alone; and a child of an already-raised parent starts raised.
#[test]
fn a_child_follows_its_parent() {
	let parent = Cancel::new();
	let child = parent.child();
	assert!(!parent.is_cancelled());
	assert!(!child.is_cancelled());
	child.cancel();
	assert!(child.is_cancelled());
	assert!(!parent.is_cancelled(), "a child does not cancel its parent");
	parent.cancel();
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

#[derive(Default)]
struct Woken(AtomicBool);

impl Wake for Woken {
	fn wake(self: Arc<Self>) {
		self.0.store(true, Ordering::SeqCst);
	}
}

/// A wait on a cancel that already fired resolves on its first poll.
#[test]
fn a_wait_on_a_fired_cancel_is_ready_at_once() {
	let cancel = Cancel::new();
	cancel.cancel();
	let mut cx = Context::from_waker(Waker::noop());
	assert!(
		pin!(cancel.wait()).poll(&mut cx).is_ready(),
		"the wait missed a cancel that had already fired"
	);
}

/// A cancel raised on another thread while a wait's first poll is checking and
/// registering either resolves that poll or wakes the waker it registered.
///
/// Both threads spin on one counter to start each round together, so the raise
/// lands inside the poll's window in some rounds. Lost rounds are counted, not
/// asserted in the loop: a panic there would leave the raiser spinning on a
/// round that never starts.
#[test]
fn a_cancel_racing_the_wait_registration_is_not_lost() {
	const ROUNDS: usize = 20_000;
	let cancels: Vec<Cancel> = (0..ROUNDS).map(|_| Cancel::new()).collect();
	let round = AtomicUsize::new(0);
	let mut lost = 0;
	let mut stuck = 0;
	std::thread::scope(|scope| {
		scope.spawn(|| {
			for (at, cancel) in cancels.iter().enumerate() {
				while round.load(Ordering::Acquire) != at * 2 + 1 {
					std::hint::spin_loop();
				}
				cancel.cancel();
				round.store(at * 2 + 2, Ordering::Release);
			}
		});
		for (at, cancel) in cancels.iter().enumerate() {
			let woken = Arc::new(Woken::default());
			let waker = Waker::from(Arc::clone(&woken));
			let mut cx = Context::from_waker(&waker);
			let mut wait = pin!(cancel.wait());
			round.store(at * 2 + 1, Ordering::Release);
			let ready = wait.as_mut().poll(&mut cx).is_ready();
			while round.load(Ordering::Acquire) != at * 2 + 2 {
				std::hint::spin_loop();
			}
			if !ready && !woken.0.load(Ordering::SeqCst) {
				lost += 1;
			}
			if wait.as_mut().poll(&mut cx).is_pending() {
				stuck += 1;
			}
		}
	});
	assert_eq!(
		lost, 0,
		"a cancel fired around the registration and nothing woke the wait"
	);
	assert_eq!(stuck, 0, "a wait stayed pending on a fired cancel");
}

/// A wait dropped before its cancel fires gives its waker back, so a
/// long-lived cancel holds nothing of the futures that gave up on it.
#[test]
fn a_dropped_wait_lets_go_of_its_waker() {
	let cancel = Cancel::new();
	let woken = Arc::new(Woken::default());
	let waker = Waker::from(Arc::clone(&woken));
	let held = Arc::strong_count(&woken);
	{
		let mut wait = pin!(cancel.wait());
		assert!(
			wait.as_mut()
				.poll(&mut Context::from_waker(&waker))
				.is_pending()
		);
		assert!(
			Arc::strong_count(&woken) > held,
			"the pending wait registered no waker"
		);
	}
	assert_eq!(
		Arc::strong_count(&woken),
		held,
		"the dropped wait left its waker in the cancel"
	);
	cancel.cancel();
	assert!(
		!woken.0.load(Ordering::SeqCst),
		"the cancel woke a wait that was gone"
	);
}

/// A `cancel()` on another thread raises the reactor's wake-up, so a thread
/// parked in `block_on` comes back and the `until` future resolves.
#[test]
fn a_cancel_from_another_thread_wakes_a_parked_block_on() {
	let mut executor = executor();
	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let right: Facade<UnixStream> = Facade::with_executor(right, &executor);
	let buf = Arc::new(Buffer::new(8));
	let cancel = Cancel::new();
	let raiser = cancel.clone();
	let thread = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		raiser.cancel();
	});
	let started = Instant::now();
	let outcome = executor
		.block_on(right.recv(span(&buf)).until(&cancel))
		.unwrap();
	thread.join().unwrap();
	assert!(
		outcome
			.result
			.as_ref()
			.is_err_and(|error| error.kind() == ErrorKind::Cancelled)
	);
	assert!(
		started.elapsed() < Duration::from_secs(5),
		"the parked block_on slept through the cancel"
	);
	assert_eq!(
		Arc::strong_count(&buf),
		1,
		"the kernel let go of the buffer"
	);
	let _ = left;
}

/// A child a holder keeps still goes with its parent.
#[test]
fn a_raised_cancel_fires_a_child_that_is_still_held() {
	let cancel = Cancel::new();
	let child = cancel.child();
	assert!(!child.is_cancelled(), "a child starts unraised");
	cancel.cancel();
	assert!(child.is_cancelled(), "the child went with the parent");
}

/// One cancel spans every executor its waiters run on, not just the one it was
/// first polled through: a raise on a third thread has to reach both parked
/// `block_on`s, so each registers its own reactor's wake-up with the cancel.
#[test]
fn a_cancel_shared_by_two_executors_wakes_both() {
	let cancel = Cancel::new();
	let (done, finished) = std::sync::mpsc::channel();
	for _ in 0..2 {
		let cancel = cancel.clone();
		let done = done.clone();
		std::thread::spawn(move || {
			let mut executor = executor();
			let (left, right) = UnixStream::pair().unwrap();
			left.set_nonblocking(true).unwrap();
			right.set_nonblocking(true).unwrap();
			let right: Facade<UnixStream> = Facade::with_executor(right, &executor);
			let buf = Arc::new(Buffer::new(8));
			let started = Instant::now();
			let outcome = executor
				.block_on(right.recv(span(&buf)).until(&cancel))
				.unwrap();
			// `left` outlives the receive: a peer that went would end it with EOF,
			// not leave it stalled for the cancel to reach.
			let _ = left;
			let _ = done.send((outcome, started.elapsed(), buf));
		});
	}
	let raiser = cancel.clone();
	let raising = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		raiser.cancel();
	});
	raising.join().unwrap();
	for _ in 0..2 {
		let (outcome, elapsed, buf) = finished
			.recv_timeout(Duration::from_secs(5))
			.expect("a parked block_on never woke: the shared cancel was lost");
		assert!(
			outcome
				.result
				.as_ref()
				.is_err_and(|error| error.kind() == ErrorKind::Cancelled)
		);
		assert!(
			elapsed < Duration::from_secs(1),
			"an executor parked in block_on slept through the shared cancel: {elapsed:?}"
		);
		assert_eq!(
			Arc::strong_count(&buf),
			1,
			"the kernel let go of the buffer"
		);
	}
}

/// A cancel fired on another thread has to reach a reactor asleep in its
/// backend, not one spinning on a short poll: the reactor is driven with
/// `poll(None)`, so after the cancel's own raise is spent the only thing that can
/// bring it back is the wake the cancel raises for the `until` it races. The
/// future then resolves cancelled and the kernel lets go of the buffer.
///
/// A wake lost on the way to a park that waits forever is a hang, so the
/// resolver is watched by a deadline rather than left to time out.
#[test]
fn a_cancel_from_another_thread_wakes_a_parked_reactor() {
	let (done, finished) = std::sync::mpsc::channel();
	let resolving = std::thread::spawn(move || {
		let mut executor = executor();
		let (left, right) = UnixStream::pair().unwrap();
		left.set_nonblocking(true).unwrap();
		right.set_nonblocking(true).unwrap();
		let right: Facade<UnixStream> = Facade::with_executor(right, &executor);
		let buf = Arc::new(Buffer::new(8));
		let cancel = Cancel::new();
		let raiser = cancel.clone();
		let raising = std::thread::spawn(move || {
			// Let the reactor reach the park, so the cancel lands on a reactor that is
			// genuinely asleep rather than one about to poll and find the give-up itself.
			std::thread::sleep(Duration::from_millis(200));
			raiser.cancel();
		});
		let outcome = executor
			.block_on(right.recv(span(&buf)).until(&cancel))
			.unwrap();
		raising.join().unwrap();
		assert!(
			outcome
				.result
				.as_ref()
				.is_err_and(|error| error.kind() == ErrorKind::Cancelled),
			"the receive completed instead of cancelling"
		);
		assert_eq!(
			Arc::strong_count(&buf),
			1,
			"the cancelled receive kept its buffer"
		);
		done.send(()).unwrap();
	});
	let finished = finished.recv_timeout(Duration::from_secs(5));
	assert!(
		!matches!(finished, Err(std::sync::mpsc::RecvTimeoutError::Timeout)),
		"a cancel from another thread did not reach the parked reactor, so the \
		 receive it gave up on never settled"
	);
	resolving.join().unwrap();
}

/// A timer given a cancel resolves with the cancel, not with its deadline: the
/// reactor disarms it and the future reports `Cancelled` instead of waiting an
/// hour.
#[test]
fn a_cancelled_timeout_reports_the_cancel() {
	let mut executor = executor();
	let cancel = Cancel::new();
	let raiser = cancel.clone();
	let raising = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		raiser.cancel();
	});
	let timeout = Timeout::with_executor(Duration::from_secs(3600), &executor);
	let started = Instant::now();
	let result = executor.block_on(timeout.until(&cancel)).unwrap();
	raising.join().unwrap();
	let error = result.expect_err("the cancelled timeout resolved instead of reporting the cancel");
	assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
	assert!(
		started.elapsed() < Duration::from_secs(5),
		"the timeout slept to its deadline instead of taking the cancel"
	);
}
