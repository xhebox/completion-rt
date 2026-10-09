//! Reactor mechanics, driven the way the low layer exposes them: the poll
//! timeout, the readiness waits a facade cannot shape, the event a caller waits
//! on, the drop fallback, and the backend's own I/O.
//!
//! These need a reactor the caller polls itself — a timeout, an `Event` to
//! raise, the `is_idle`/`is_stopped`/`stop` state — none of which the async
//! layer, which owns its reactor inside an `Executor`, can name or reach.

mod common;

use common::block_on;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::future::Future;
#[cfg(unix)]
use std::pin::pin;
#[cfg(unix)]
use std::task::{Context, Waker};

use completion_rt::{Cancel, Cancelled, Config, Extent, Handle, Memory, Reactor};
#[cfg(unix)]
use completion_rt::{Completion, Submitter};

/// The buffer in the shape the engine names memory: a second handle on it, so
/// it outlives the operation.
fn memory(buf: &Arc<Vec<u8>>) -> Arc<dyn Memory> {
	let memory: Arc<dyn Memory> = buf.clone();
	memory
}

/// The whole of a buffer, as one extent.
fn whole(buf: &Arc<Vec<u8>>) -> [Extent; 1] {
	[Extent {
		offset: 0,
		len: buf.len(),
	}]
}

/// A file with `contents`, in a scratch directory that goes with its
/// descriptor.
fn seeded_file(contents: &[u8]) -> Handle<std::fs::File> {
	let dir = common::tempdir("completion-seed").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, contents).unwrap();
	// The returned descriptor outlives the deleted directory entry.
	Handle::new(common::open(path).unwrap())
}

/// A connected pair, both ends non-blocking: a receive on one waits for a send
/// on the other.
#[cfg(unix)]
fn socket_pair() -> (
	std::os::unix::net::UnixStream,
	Handle<std::os::unix::net::UnixStream>,
) {
	use std::os::unix::net::UnixStream;

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	(left, Handle::new(right))
}

/// A read that nothing will satisfy: the peer never sends.
#[cfg(unix)]
fn stalled_recv(
	submitter: &Submitter,
	right: &Handle<std::os::unix::net::UnixStream>,
	buf: &Arc<Vec<u8>>,
) -> Completion {
	submitter.read(right, 0, memory(buf), whole(buf)).unwrap()
}

/// Drops an in-flight operation on purpose. The fallback panics once the
/// backend is off the memory when the build has debug assertions; the panic is
/// caught here so the test can go on checking what the drop did.
#[cfg(unix)]
fn drop_in_flight_on_purpose(drop: impl FnOnce()) {
	let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(drop));
	assert_eq!(
		caught.is_err(),
		cfg!(debug_assertions),
		"an in-flight drop panics exactly when the build has debug assertions"
	);
}

/// The message the fallback's panic carries.
#[cfg(unix)]
fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
	match panic.downcast::<String>() {
		Ok(text) => *text,
		Err(other) => match other.downcast::<&str>() {
			Ok(text) => (*text).to_owned(),
			Err(_) => String::new(),
		},
	}
}

/// The reactor only drains at the top of `poll`, so a submission arriving
/// while `poll` is blocked has to wake it; otherwise nothing ever hands that
/// submission to the kernel.
#[test]
fn a_submit_from_another_thread_wakes_a_blocked_poll() {
	let file = seeded_file(b"hello");
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let submitter = Arc::new(submitter);
	let writer = Arc::clone(&submitter);
	let writer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
		let completion = writer.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
		assert_eq!(block_on(completion).1.unwrap().count().unwrap(), 5);
		buf
	});
	// Nothing is queued yet, so this blocks until the wake arrives.
	reactor.poll(None).unwrap();
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	assert_eq!(&writer.join().unwrap()[..], b"hello");
}

/// A file operation lands on a worker's clock, so it can be finished before
/// the reactor is polled at all. The completion is then in hand, and the poll
/// hands it back: sleeping on news already in hand is a stall the timeout, not
/// the work, ends.
#[test]
fn a_landed_file_operation_does_not_wait_out_the_timeout() {
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let file = seeded_file(b"hello");
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
	// Let the pool land it before the reactor is polled once.
	std::thread::sleep(Duration::from_millis(50));

	let started = std::time::Instant::now();
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_secs(30))).unwrap();
	}
	assert_eq!(block_on(completion).1.unwrap().count().unwrap(), 5);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the poll slept on a completion it already had"
	);
}

/// A poll that does not wait has not used the notification, so it must leave
/// it raised: another thread may have raised it while this poll ran, and
/// clearing it here leaves the next waiting poll asleep with news waiting for
/// it.
///
/// Windows: the notification is a completion packet, which a poll takes off
/// the port to see at all; there is no level-triggered state to leave raised.
#[cfg(unix)]
#[test]
fn a_poll_without_a_wait_leaves_the_notification_raised() {
	let (mut reactor, _submitter) = Reactor::new(Config::default()).unwrap();
	reactor.event().notify().unwrap();
	reactor.poll(Some(Duration::ZERO)).unwrap();
	let start = std::time::Instant::now();
	reactor.poll(Some(Duration::from_secs(5))).unwrap();
	assert!(
		start.elapsed() < Duration::from_secs(1),
		"the wake-up raised before the poll that did not wait was dropped"
	);
}

/// Several timers wait at once: each deadline is an entry in the reactor's
/// table, so the nearest one is what the poller sleeps to and a later one
/// stays pending behind it.
#[test]
fn timers_wait_out_their_own_deadlines() {
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let started = std::time::Instant::now();
	let mut near = submitter.timeout(Duration::from_millis(20)).unwrap();
	let far = submitter.timeout(Duration::from_secs(2)).unwrap();
	let mut cx = std::task::Context::from_waker(std::task::Waker::noop());

	// Drive until the near timer lands, taking its completion in the loop:
	// the reactor is not idle either way while the far one waits.
	let mut landed = None;
	while landed.is_none() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
		if let std::task::Poll::Ready(((), result)) =
			std::future::Future::poll(std::pin::Pin::new(&mut near), &mut cx)
		{
			landed = Some(result);
		}
	}
	assert!(landed.unwrap().is_ok(), "the near timer completed");
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the poller slept for the far timer's deadline"
	);
	// The far one is still in the table.
	assert!(!reactor.is_idle());
	drop(far);
}

/// A wait is capped at the nearest deadline: a poll asked for far longer than
/// the timer still returns when the timer comes due, not when its own timeout
/// passes.
#[test]
fn a_poll_does_not_sleep_past_the_nearest_deadline() {
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let timer = submitter.timeout(Duration::from_millis(50)).unwrap();
	let started = std::time::Instant::now();
	while !reactor.is_idle() {
		// Far longer than the timer: only the table cuts the wait short.
		reactor.poll(Some(Duration::from_secs(5))).unwrap();
	}
	let elapsed = started.elapsed();
	assert!(
		elapsed >= Duration::from_millis(50),
		"the timer fired early: {elapsed:?}"
	);
	assert!(
		elapsed < Duration::from_secs(1),
		"the wait slept past the nearest deadline: {elapsed:?}"
	);
	let ((), result) = block_on(timer);
	result.expect("the timer completed");
}

/// A timer submitted from another thread while the reactor is parked in a wait
/// has to raise its wake-up: the wait, which has no deadline of its own, is
/// recomputed against the timer's and settles it on time.
#[test]
fn a_timer_from_another_thread_wakes_a_parked_wait() {
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let stop = Arc::new(AtomicBool::new(false));
	let event = submitter.event();
	let driven = {
		let stop = Arc::clone(&stop);
		let mut reactor = reactor;
		std::thread::spawn(move || {
			while !stop.load(Ordering::SeqCst) {
				// Nothing is in flight at first, so this wait can only end by
				// the raise its own timeout is the bound on.
				reactor.poll(Some(Duration::from_secs(5))).unwrap();
			}
		})
	};
	// Let the driver reach its wait, with nothing in the table.
	std::thread::sleep(Duration::from_millis(50));
	let started = std::time::Instant::now();
	let timer = submitter.timeout(Duration::from_millis(50)).unwrap();
	let ((), result) = block_on(timer);
	result.expect("the timer completed");
	let elapsed = started.elapsed();
	stop.store(true, Ordering::SeqCst);
	let _ = event.notify();
	driven.join().unwrap();
	assert!(
		elapsed < Duration::from_secs(1),
		"the reactor slept through the timer from another thread: {elapsed:?}"
	);
}

/// A dropped timer leaves nothing behind: no kernel operation to wait for, no
/// deadline in the table, and no wait shortened by a deadline that has gone.
#[test]
fn a_dropped_timer_never_fires_and_leaves_the_reactor_idle() {
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let timer = submitter.timeout(Duration::from_millis(20)).unwrap();
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the timer waits in the table");
	drop(timer);
	assert!(reactor.is_idle(), "the dropped timer is still in the table");
	// The deadline passes with nothing to fire.
	std::thread::sleep(Duration::from_millis(50));
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(reactor.is_idle(), "the dropped timer was settled");
	// A stale deadline would cut this wait to zero.
	let started = std::time::Instant::now();
	reactor.poll(Some(Duration::from_millis(20))).unwrap();
	assert!(
		started.elapsed() >= Duration::from_millis(20),
		"the dropped timer's deadline still shortened a wait"
	);
}

/// `Submitter::stop` ends a driver loop parked in `poll(None)`, but the
/// operations already in flight are collected first.
#[test]
fn stopping_the_reactor_drains_what_is_in_flight() {
	let file = seeded_file(b"hello");
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
		reactor
	});
	// Stop with the read still in flight: the loop has to see it land.
	submitter.stop();
	assert_eq!(block_on(completion).1.unwrap().count().unwrap(), 5);
	assert_eq!(&buf[..], b"hello");
	let reactor = driven.join().unwrap();
	assert!(reactor.is_idle());

	// A stopped reactor takes no more work.
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let error = submitter
		.read(&file, 0, memory(&buf), whole(&buf))
		.err()
		.expect("a stopped reactor refuses submissions");
	assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

/// The reactor's own wake-up is a descriptor a caller can wait on
/// (`Submitter::wait`): a raise from another thread must complete that wait,
/// not only make `poll` return — otherwise the task waiting on it never runs
/// again.
///
/// Windows: a completion port is not a descriptor, so `Submitter::wait` has
/// nothing to arm.
#[cfg(unix)]
#[test]
fn an_event_completes_a_wait_armed_on_it() {
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let completion = submitter.wait().unwrap();
	// The first pass hands the submission to the backend.
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the wait is in flight");

	submitter.event().notify().unwrap();
	reactor.poll(Some(Duration::from_millis(100))).unwrap();
	assert!(reactor.is_idle(), "a raise left the wait armed");

	let ((), result) = block_on(completion);
	result.unwrap();
}

/// A timer is cancelled like any other op: `until` asks the reactor to stop it,
/// and the entry settles as cancelled rather than waiting out its deadline.
#[test]
fn until_cancels_a_timer() {
	// `Until` is the future the async layer's `until` narrows to; a driver may
	// hand one to another thread, so it stays `Send`.
	fn assert_send<T: Send>() {}
	assert_send::<completion_rt::Until<()>>();

	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let cancel = Cancel::new();
	let completion = submitter.timeout(Duration::from_secs(30)).unwrap();
	let stop = Arc::new(AtomicBool::new(false));
	let driven = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			while !stop.load(Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};
	cancel.cancel();
	let started = Instant::now();
	let result = block_on(completion.until(&cancel));
	stop.store(true, Ordering::SeqCst);
	driven.join().unwrap();
	assert!(
		matches!(result, Err(Cancelled(()))),
		"the timer was not cancelled"
	);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the cancel waited out the timer's deadline"
	);
}

/// Dropping a completion whose cancel already fired is the given-up path, not a
/// lost operation: the reactor was asked to stop it, and the drop is what waits
/// for the kernel to let go.
///
/// The drop reports it, and a build with debug assertions does not panic on it —
/// the panic is for the operation nobody gave up, whose memory could still be
/// read or written. The cancel is polled through the future first, so the ask is
/// recorded before the drop, which is the state a device reaches when a stop
/// cancels the session and then lets the future go.
#[test]
#[cfg(unix)]
fn a_drop_after_the_cancel_fired_is_not_an_unsignalled_one() {
	use std::panic::{AssertUnwindSafe, catch_unwind};

	let (_left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let cancel = Cancel::new();
	let completion = stalled_recv(&submitter, &right, &buf);
	let mut receive = Box::pin(completion.until(&cancel));
	let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
	assert!(
		receive.as_mut().poll(&mut cx).is_pending(),
		"the peer never sends, so the receive waits"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the receive has to be in flight");

	// The cancel fires, and the future is polled once more: that poll asks the
	// backend to stop the receive, and leaves the ask recorded on the entry.
	cancel.cancel();
	assert!(
		receive.as_mut().poll(&mut cx).is_pending(),
		"the kernel still holds the receive"
	);

	let caught = catch_unwind(AssertUnwindSafe(move || drop(receive)));
	assert!(
		caught.is_ok(),
		"a given-up drop panicked; it is the path a clean stop takes"
	);
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
}

/// A debug build panics on the drop, and the panic comes once the kernel has
/// let go: the buffer is the caller's again, and the bytes the peer sends
/// afterwards reach no operation.
#[test]
#[cfg(unix)]
fn a_dropped_in_flight_read_panics_after_the_kernel_let_go() {
	use std::io::Write;
	use std::panic::{AssertUnwindSafe, catch_unwind};

	let (mut peer, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the receive has to be in flight");
	let caught = catch_unwind(AssertUnwindSafe(|| drop(completion)));
	if cfg!(debug_assertions) {
		let message = panic_message(caught.expect_err("the drop of an in-flight read panics"));
		assert!(
			message.contains("read"),
			"the message names the op: {message}"
		);
		assert!(
			message.contains("reactor.rs"),
			"the message names where it was submitted: {message}"
		);
	} else {
		assert!(caught.is_ok(), "a release build only reports the drop");
	}
	assert_eq!(
		Arc::strong_count(&buf),
		1,
		"the drop came after the kernel let go"
	);
	assert!(reactor.is_idle(), "the drop drove the receive to its end");
	peer.write_all(b"later").unwrap();
	reactor.poll(Some(Duration::from_millis(1))).unwrap();
	assert_eq!(
		&buf[..],
		&[0u8; 8],
		"nothing was left to write the buffer after the drop"
	);
}

/// A thread already unwinding by its own panic only gets the report: a second
/// panic there would abort the process rather than fail what already failed.
#[test]
#[cfg(unix)]
fn an_in_flight_drop_during_unwinding_only_reports() {
	use std::panic::{AssertUnwindSafe, catch_unwind};

	let (_left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the receive has to be in flight");
	let caught = catch_unwind(AssertUnwindSafe(|| {
		let _completion = completion;
		panic!("the caller's own failure");
	}));

	let message = panic_message(caught.expect_err("the caller's panic comes back"));
	assert_eq!(message, "the caller's own failure");
	assert_eq!(
		Arc::strong_count(&buf),
		1,
		"the fallback still waited the kernel out"
	);
}

/// A completion dropped on another thread notifies the reactor and waits on the
/// shared condvar until its entry lands.
#[test]
#[cfg(unix)]
fn a_drop_from_another_thread_waits_for_release() {
	let (_left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	// One pass here puts the receive in flight, so the fallback's waiter path is
	// what the foreign thread's drop runs: the rest of the driving is that
	// thread's.
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the receive has to be in flight");
	let stop = Arc::new(AtomicBool::new(false));
	let driven = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			while !stop.load(Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};
	let started = Instant::now();
	drop_in_flight_on_purpose(|| drop(completion));
	assert!(
		started.elapsed() < Duration::from_secs(5),
		"the drop waited for its entry"
	);
	assert_eq!(
		Arc::strong_count(&buf),
		1,
		"the kernel let go of the buffer"
	);
	stop.store(true, Ordering::SeqCst);
	driven.join().unwrap();
}

/// The reactor may be parked rather than spinning: a drop from another thread
/// asks a cancel the reactor has to submit, so it has to wake the reactor. The
/// test above drives with a bounded poll and so never covers the parked case.
#[test]
#[cfg(unix)]
fn a_cross_thread_drop_wakes_a_parked_reactor() {
	let (_left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the receive has to be in flight");

	// Park indefinitely: only the drop's cancel gives the reactor anything to do,
	// so a missing wake is a hang, not a slow pass.
	let (parked, waiting) = std::sync::mpsc::channel();
	let driving = std::thread::spawn(move || {
		parked.send(()).unwrap();
		while !reactor.is_idle() {
			reactor.poll(None).unwrap();
		}
	});
	waiting.recv().unwrap();
	// Give the reactor the moment it needs to reach the park; the drop is still
	// caught if it lands first, because then the drain sees the give-up.
	std::thread::sleep(Duration::from_millis(200));

	let (done, finished) = std::sync::mpsc::channel();
	let dropping = std::thread::spawn(move || {
		drop_in_flight_on_purpose(|| drop(completion));
		done.send(()).unwrap();
	});
	assert!(
		finished.recv_timeout(Duration::from_secs(5)).is_ok(),
		"a drop from another thread did not wake the parked reactor, so the \
		 operation it gave up on never settled"
	);
	dropping.join().unwrap();
	driving.join().unwrap();
	assert_eq!(
		Arc::strong_count(&buf),
		1,
		"the kernel let go of the buffer"
	);
}

/// A readiness wait is memory-free too, and dropping one does not block.
#[test]
#[cfg(unix)]
fn a_dropped_readiness_wait_does_not_block() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let reader = Handle::new(reader);
	let waker = std::task::Waker::noop();
	let mut cx = std::task::Context::from_waker(waker);
	let started;
	{
		let mut wait = std::pin::pin!(submitter.wait_readable(&reader).unwrap());
		assert!(
			wait.as_mut().poll(&mut cx).is_pending(),
			"the pipe has no data"
		);
		// The first pass hands the wait to the backend.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert!(!reactor.is_idle(), "the wait is in flight");
		started = Instant::now();
	}
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"a readiness wait must not block"
	);
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
}

/// Dropping the reactor with an operation still out drives it to its end, and a
/// completion dropped afterwards finds its entry gone and returns at once.
#[test]
#[cfg(unix)]
fn a_reactor_drop_settles_what_is_in_flight() {
	let (_left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the receive is in flight");
	drop(reactor);
	assert_eq!(Arc::strong_count(&buf), 1, "the reactor held the buffer");
	let started = Instant::now();
	drop(completion);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"a completion after the reactor is gone returns at once"
	);
}

/// A thread blocked on the shared condvar is released when the reactor goes,
/// even though nobody will settle its entry any more.
#[test]
#[cfg(unix)]
fn a_reactor_drop_releases_a_waiting_drop() {
	let (_left, right) = socket_pair();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	// The reactor's driver: one pass to put the receive in flight, then the
	// reactor goes, with the dropping thread still waiting.
	let driver = std::thread::spawn(move || {
		let mut reactor = reactor;
		reactor.poll(Some(Duration::ZERO)).unwrap();
		std::thread::sleep(Duration::from_millis(30));
		drop(reactor);
	});
	std::thread::sleep(Duration::from_millis(10));
	let started = Instant::now();
	drop_in_flight_on_purpose(|| drop(completion));
	driver.join().unwrap();
	assert!(
		started.elapsed() < Duration::from_secs(5),
		"the waiting drop was not released"
	);
}

/// The userdata comes back with a cancelled op, and a byte written to the peer
/// after the cancel never reaches the buffer.
#[test]
#[cfg(unix)]
fn a_cancelled_receive_hands_back_only_the_userdata() {
	use std::io::Write;
	use std::os::unix::net::UnixStream;

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let right = Handle::new(right);
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf).tag("frame");
	let cancel = Cancel::new();
	let stop = Arc::new(AtomicBool::new(false));
	let driven = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			while !stop.load(Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};
	std::thread::sleep(Duration::from_millis(20));
	cancel.cancel();
	let result = block_on(completion.until(&cancel));
	stop.store(true, Ordering::SeqCst);
	driven.join().unwrap();
	let tag = match result {
		Err(Cancelled(tag)) => tag,
		Ok(_) => panic!("the receive completed after the cancel"),
	};
	assert_eq!(tag, "frame", "the userdata came back");
	(&left).write_all(b"late").unwrap();
	std::thread::sleep(Duration::from_millis(20));
	assert_eq!(&buf[..], &[0u8; 8], "a byte arrived after the cancel");
}

/// A cancel fired on another thread has to reach a reactor asleep in its
/// backend, not one spinning on a short poll: the reactor is driven with
/// `poll(None)`, so after the cancel's own raise is spent the only thing that can
/// bring it back is the `request_cancel` the now-cancelled `Until` asks for. The
/// future then resolves cancelled and the kernel lets go of the buffer.
///
/// A wake lost on the way to a park that waits forever is a hang, so the
/// resolver is watched by a deadline rather than left to time out.
#[test]
#[cfg(unix)]
fn a_cancel_from_another_thread_wakes_a_parked_reactor() {
	let (_left, right) = socket_pair();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	let cancel = Cancel::new();
	// Polling `until` registers its waiter's waker with the cancel.
	let until = completion.until(&cancel);

	let stop = Arc::new(AtomicBool::new(false));
	let event = submitter.event();
	let (parked, waiting) = std::sync::mpsc::channel();
	let driving = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			let mut reactor = reactor;
			// One pass hands the receive to the backend; the rest sleeps in it.
			reactor.poll(Some(Duration::ZERO)).unwrap();
			parked.send(()).unwrap();
			while !stop.load(Ordering::SeqCst) {
				reactor.poll(None).unwrap();
			}
		})
	};
	waiting.recv().unwrap();
	// Let the reactor reach the park, so the cancel lands on a reactor that is
	// genuinely asleep rather than one about to poll and find the give-up itself.
	std::thread::sleep(Duration::from_millis(200));

	let raiser = cancel.clone();
	let raising = std::thread::spawn(move || raiser.cancel());

	let (done, finished) = std::sync::mpsc::channel();
	let resolving = std::thread::spawn(move || {
		done.send(block_on(until)).unwrap();
	});
	let result = finished.recv_timeout(Duration::from_secs(5)).expect(
		"a cancel from another thread did not reach the parked reactor, so the \
		 receive it gave up on never settled",
	);
	// Stop the driver deliberately: a reactor dropped the moment it went idle
	// would tear down the entry the resolver takes its result from.
	stop.store(true, Ordering::SeqCst);
	let _ = event.notify();
	resolving.join().unwrap();
	raising.join().unwrap();
	driving.join().unwrap();
	assert!(
		matches!(result, Err(Cancelled(()))),
		"the receive completed instead of cancelling"
	);
	assert_eq!(
		Arc::strong_count(&buf),
		1,
		"the cancelled receive kept its buffer"
	);
}

/// Dropping a still-pending operation from many threads while the reactor
/// drains never leaves the request running: the drop's decision and what it
/// changes are one lock hold, so a `drain` cannot mark the entry in flight and
/// push it after the drop has decided nobody is waiting.
///
/// The window is a moment between two lock holds ([`SlotGuard::drop`] and
/// [`Core::drain`]), so it cannot be opened on cue: the test floods it instead,
/// with the buffer's strong count as the witness a request reached the kernel
/// after its drop returned. The counts are sized so the window is hit on every
/// run under the split, and never under one hold.
///
/// A drop that loses the race is the memory-naming fallback, which a debug
/// build panics on: that panic is caught here, the flood being the point, and
/// a release build is held to never raising one.
#[test]
#[cfg(unix)]
fn a_drop_racing_the_reactor_leaves_no_request_running() {
	let (left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let stop = Arc::new(AtomicBool::new(false));
	let reactor_thread = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			// A reactor that is always driving, so the drop always races a pass.
			while !stop.load(Ordering::SeqCst) {
				reactor.poll(Some(Duration::ZERO)).unwrap();
			}
			reactor
		})
	};
	let caught = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let mut workers = Vec::new();
	for _ in 0..8 {
		let right = right.clone();
		let submitter = submitter.clone();
		let caught = Arc::clone(&caught);
		workers.push(std::thread::spawn(move || {
			for _ in 0..600_000 {
				let buf = Arc::new(vec![0u8; 8]);
				let completion = submitter
					.read(&right, 0, memory(&buf), whole(&buf))
					.unwrap();
				let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
					drop(completion);
				}))
				.is_err();
				assert!(
					!panicked || cfg!(debug_assertions),
					"a fallback drop panicked in a release build"
				);
				if Arc::strong_count(&buf) != 1 {
					caught.fetch_add(1, Ordering::SeqCst);
				}
			}
		}));
	}
	for worker in workers {
		worker.join().unwrap();
	}
	stop.store(true, Ordering::SeqCst);
	let mut reactor = reactor_thread.join().unwrap();
	// Let the reactor finish whatever the workers left pending.
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert_eq!(
		caught.load(Ordering::SeqCst),
		0,
		"a drop returned while the kernel still held the buffer"
	);
	let _ = left;
}

/// A readiness wait names nothing of the caller's, so dropping one is not a
/// bug — but it still has to be stopped and reaped: until its entry goes, the
/// wait holds the descriptor open, and a wait on a descriptor that never
/// becomes ready has no such end.
#[test]
#[cfg(unix)]
fn a_dropped_readiness_wait_is_cancelled_and_reaped() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let mut reader = Handle::new(reader);
	let mut cx = Context::from_waker(Waker::noop());
	{
		let mut wait = pin!(submitter.wait_readable(&reader).unwrap());
		assert!(
			wait.as_mut().poll(&mut cx).is_pending(),
			"the pipe has no data"
		);
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert!(!reactor.is_idle(), "the wait is in flight");
	}
	// The write end stays open, so nothing will ever make the read end ready:
	// only cancelling and reaping the abandoned wait can release the pipe.
	let mut taken = false;
	for _ in 0..8 {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
		reader = match reader.try_take() {
			Ok(_resource) => {
				taken = true;
				break;
			}
			Err(handle) => handle,
		};
	}
	assert!(taken, "the abandoned wait still held the pipe");
}

/// A send that carries bytes names the caller's memory — its descriptors ride
/// in the backend's control buffer, not the caller's memory — so dropping one
/// in flight is a fallback the reactor waits out. A send carrying no data
/// names nothing and drops without one.
#[test]
#[cfg(unix)]
fn a_send_is_memory_free_only_without_data() {
	use std::io::Write;
	use std::os::unix::net::UnixStream;

	let (stream, _peer) = UnixStream::pair().unwrap();
	stream.set_nonblocking(true).unwrap();
	// Fill the socket's buffers so a send that carries bytes cannot take any:
	// the send then stays with the kernel instead of landing short.
	let filler = [0u8; 64 << 10];
	loop {
		match (&stream).write(&filler) {
			Ok(0) => break,
			Ok(_) => continue,
			Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
			Err(error) => panic!("filling the socket failed: {error}"),
		}
	}
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket = Handle::new(stream);
	let mut cx = Context::from_waker(Waker::noop());

	let bytes: Arc<Vec<u8>> = Arc::new(vec![1u8; 5]);
	let payload: Arc<dyn Memory> = Arc::clone(&bytes) as Arc<dyn Memory>;
	let mut send = Box::pin(
		submitter
			.send_with_fds(&socket, payload, whole(&bytes), Vec::new())
			.unwrap(),
	);
	assert!(
		send.as_mut().poll(&mut cx).is_pending(),
		"the full socket cannot take the bytes"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the send has to be in flight");
	drop_in_flight_on_purpose(move || drop(send));
	assert_eq!(Arc::strong_count(&bytes), 1, "the send released its buffer");
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}

	let empty: Arc<Vec<u8>> = Arc::new(Vec::new());
	let payload: Arc<dyn Memory> = Arc::clone(&empty) as Arc<dyn Memory>;
	let mut send = Box::pin(
		submitter
			.send_with_fds(&socket, payload, whole(&empty), Vec::new())
			.unwrap(),
	);
	assert!(
		send.as_mut().poll(&mut cx).is_pending(),
		"the send cannot land before the reactor drains it"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	drop(send);
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
}

/// The uring backend's own waits and its half of the race: the kernel holds
/// an operation's buffers, and only SQPOLL puts the release out of reach of one
/// pass over the ring.
#[cfg(target_os = "linux")]
mod uring {
	use super::*;

	/// A reactor whose completions arrive on the kernel's own thread, so an
	/// operation's release is out of reach of a single pass over the ring: the
	/// inline drive's blocking wait is only reachable here, and that wait is what
	/// the two tests below are about.
	///
	/// A host that refuses SQPOLL has no wait either, and a test that cannot
	/// exercise its subject must fail rather than pass quietly.
	fn sqpoll_reactor() -> (Reactor, Submitter) {
		Reactor::new(Config {
			sqpoll: true,
			..Config::default()
		})
		.expect("this host must grant SQPOLL for these waits to be exercised")
	}

	/// Dropping an in-flight receive on the reactor thread holds until the kernel
	/// has let go: the caller's handle is all that is left of the buffer the moment
	/// the drop returns.
	///
	/// SQPOLL is what puts the release out of reach of one pass: the cancel and the
	/// completion are the kernel thread's to deliver, so the drop has to wait.
	#[test]
	fn a_drop_on_the_reactor_thread_waits_for_release() {
		let (_left, right) = socket_pair();
		let (mut reactor, submitter) = sqpoll_reactor();
		let buf = Arc::new(vec![0u8; 8]);
		let completion = stalled_recv(&submitter, &right, &buf);
		// This thread is the reactor's driver from here on.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert!(
			!reactor.is_idle(),
			"the receive has to be in flight for the fallback to run"
		);
		assert_eq!(Arc::strong_count(&buf), 2, "the submission holds it");
		drop_in_flight_on_purpose(|| drop(completion));
		assert_eq!(
			Arc::strong_count(&buf),
			1,
			"the kernel still held the buffer"
		);
		assert!(reactor.is_idle(), "the drop drove the receive to its end");
	}

	/// The inline drive runs the reactor's blocking wait, which consumes a raw ring
	/// left on the backend. A wake raised through the reactor's own protocol is not
	/// that: it is the `Park`'s state word, which the drive's wait never touches, so
	/// it still brings the next pass back instead of letting it sleep.
	///
	/// The drive only reaches its wait on a ring whose completions arrive on another
	/// thread (SQPOLL): an operation cancelled on the polling ring itself lands
	/// before the wait, and the raise is left alone.
	#[test]
	fn an_inline_drive_does_not_eat_a_protocol_wake() {
		let (_left, right) = socket_pair();
		let (mut reactor, submitter) = sqpoll_reactor();
		let buf = Arc::new(vec![0u8; 8]);
		let completion = stalled_recv(&submitter, &right, &buf);
		reactor.poll(Some(Duration::ZERO)).unwrap();
		// A wake raised through the reactor's protocol, landed while the receive is in
		// flight: the timer is only what raises it, and it goes before the pass.
		let timer = submitter.timeout(Duration::from_secs(2)).unwrap();
		drop_in_flight_on_purpose(|| drop(completion));
		drop(timer);
		// The drop's inline drive must not have eaten the wake: the pass after returns
		// at once instead of sleeping out its own timeout.
		let started = Instant::now();
		reactor.poll(Some(Duration::from_secs(2))).unwrap();
		assert!(
			started.elapsed() < Duration::from_secs(1),
			"the inline drive ate a wake raised through the reactor's protocol"
		);
	}

	/// An operation that wins the race keeps its result: the cancel is a request,
	/// and a completion that already carries bytes is not thrown away. A receive
	/// that moved bytes hands them to the caller instead of reporting a stop that
	/// never happened.
	///
	/// The race is the completion backend's — the kernel holds the buffers, so a
	/// cancel asked for after the data arrived is too late. The readiness backend's
	/// half of it is in the `portable` module below.
	#[test]
	fn an_op_that_wins_the_race_keeps_its_bytes() {
		use std::io::Write;

		let (mut left, right) = socket_pair();
		let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
		let buf = Arc::new(vec![0u8; 4]);
		let completion = submitter
			.read(&right, 0, memory(&buf), whole(&buf))
			.unwrap();
		// The receive goes to the kernel with nothing to move, so it is in flight
		// and unsettled when the completion is next looked at.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert_eq!(Arc::strong_count(&buf), 2, "the kernel holds the buffer");
		// The bytes are queued, and the kernel finishes the receive on its own.
		left.write_all(b"wins").unwrap();
		// The cancel fires before the outcome is looked at, so the cancel is
		// requested with the receive in flight; the receive gets there first.
		let cancel = Cancel::new();
		cancel.cancel();
		let mut until = std::pin::pin!(completion.until(&cancel));
		let waker = std::task::Waker::noop();
		let mut cx = std::task::Context::from_waker(waker);
		assert!(
			std::future::Future::poll(until.as_mut(), &mut cx).is_pending(),
			"the receive is in the kernel, not yet reported"
		);
		let mut ready = None;
		for _ in 0..500 {
			if let std::task::Poll::Ready(value) =
				std::future::Future::poll(until.as_mut(), &mut cx)
			{
				ready = Some(value);
				break;
			}
			reactor.poll(Some(Duration::from_millis(10))).unwrap();
		}
		let ((), response) = ready
			.expect("the receive landed")
			.expect("the bytes are the operation's own result, cancel or not");
		assert_eq!(response.unwrap().count().unwrap(), 4);
		assert_eq!(&buf[..], b"wins", "the bytes the receive moved");
	}
}

/// The readiness backend stops an operation before it runs, so a cancel that
/// arrives first decides the race.
#[cfg(all(unix, not(target_os = "linux")))]
mod portable {
	use super::*;

	/// A readiness backend holds no operation in the kernel for a cancel to arrive
	/// too late for: the receive it was asked to stop is stopped before it ever
	/// runs, and the bytes the peer had queued are still the peer's.
	#[test]
	fn a_cancel_that_wins_the_race_leaves_the_bytes_with_the_peer() {
		use std::io::{Read, Write};

		let (mut left, right) = socket_pair();
		let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
		let buf = Arc::new(vec![0u8; 4]);
		let completion = submitter
			.read(&right, 0, memory(&buf), whole(&buf))
			.unwrap();
		// The receive is armed with nothing to move, so it is in flight and
		// unsettled when the completion is next looked at.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert_eq!(Arc::strong_count(&buf), 2, "the operation holds the buffer");
		// The bytes are queued, and no pass has run the receive over them.
		left.write_all(b"wins").unwrap();
		// The cancel fires before the outcome is looked at, so the cancel is
		// requested with the receive in flight; the receive never gets there.
		let cancel = Cancel::new();
		cancel.cancel();
		let mut until = std::pin::pin!(completion.until(&cancel));
		let waker = std::task::Waker::noop();
		let mut cx = std::task::Context::from_waker(waker);
		let mut ready = None;
		for _ in 0..500 {
			if let std::task::Poll::Ready(value) =
				std::future::Future::poll(until.as_mut(), &mut cx)
			{
				ready = Some(value);
				break;
			}
			reactor.poll(Some(Duration::from_millis(10))).unwrap();
		}
		assert!(
			matches!(ready.expect("the receive was stopped"), Err(Cancelled(()))),
			"a readiness backend stops the receive it was asked to"
		);
		assert_eq!(&buf[..], &[0u8; 4], "the stopped receive wrote the buffer");
		// Nothing is lost: the bytes wait in the peer's buffer for the next one.
		let mut reader: &std::os::unix::net::UnixStream = &right;
		let mut got = [0u8; 4];
		reader.read_exact(&mut got).unwrap();
		assert_eq!(&got, b"wins", "the bytes stayed with the peer");
	}
}
