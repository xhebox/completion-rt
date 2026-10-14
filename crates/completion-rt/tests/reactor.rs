//! Reactor mechanics, driven the way the low layer exposes them: the poll
//! timeout, the readiness waits a facade cannot shape, the event a caller waits
//! on, the drop fallback, and the backend's own I/O.
//!
//! These need a reactor the caller polls itself — a timeout, an `Event` to
//! raise, the `in_flight`/`stop` state — none of which the async
//! layer, which owns its reactor inside an `Executor`, can name or reach.

mod common;

use common::block_on;

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::future::Future;
#[cfg(unix)]
use std::pin::pin;
use std::task::{Context, Waker};

use common::submit;
#[cfg(unix)]
use common::{Completion, wait_event};
use completion_rt::{Config, Extent, Extents, Handle, Memory, Op, Reactor};

/// The buffer in the shape the engine names memory: a second handle on it, so
/// it outlives the operation.
fn memory(buf: &Arc<Vec<u8>>) -> Arc<dyn Memory> {
	let memory: Arc<dyn Memory> = buf.clone();
	memory
}

/// The whole of a buffer, as one extent.
fn whole(buf: &Arc<Vec<u8>>) -> Extents {
	Extents::from_slice(&[Extent {
		offset: 0,
		len: buf.len(),
	}])
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
	reactor: &Reactor,
	right: &Handle<std::os::unix::net::UnixStream>,
	buf: &Arc<Vec<u8>>,
) -> Completion {
	submit(
		reactor,
		right,
		Op::Read {
			offset: 0,
			memory: memory(buf),
			extents: whole(buf),
		},
	)
	.unwrap()
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

/// A file operation lands on a worker's clock, so it can be finished before
/// the reactor is polled at all. The completion is then in hand, and the poll
/// hands it back: sleeping on news already in hand is a stall the timeout, not
/// the work, ends.
#[test]
fn a_landed_file_operation_does_not_wait_out_the_timeout() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let file = seeded_file(b"hello");
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let completion = submit(
		&reactor,
		&file,
		Op::Read {
			offset: 0,
			memory: memory(&buf),
			extents: whole(&buf),
		},
	)
	.unwrap();
	// Let the pool land it before the reactor is polled once.
	std::thread::sleep(Duration::from_millis(50));

	let started = std::time::Instant::now();
	while !common::idle(&reactor) {
		reactor.poll(Some(Duration::from_secs(30))).unwrap();
	}
	assert_eq!(
		block_on(&reactor, completion).1.unwrap().count().unwrap(),
		5
	);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the poll slept on a completion it already had"
	);
}

/// A poll that does not wait has not used the notification, so it must leave
/// it raised: a wake raised before it has to still stop the next waiting poll
/// from sleeping, or that poll sleeps with news waiting for it.
///
/// Windows: the notification is a completion packet, which a poll takes off
/// the port to see at all; there is no level-triggered state to leave raised.
#[cfg(unix)]
#[test]
fn a_poll_without_a_wait_leaves_the_notification_raised() {
	let reactor = Reactor::new(Config::default()).unwrap();
	reactor.event().notify().unwrap();
	reactor.poll(Some(Duration::ZERO)).unwrap();
	let start = std::time::Instant::now();
	reactor.poll(Some(Duration::from_secs(5))).unwrap();
	assert!(
		start.elapsed() < Duration::from_secs(1),
		"the wake-up raised before the poll that did not wait was dropped"
	);
}

/// Several timers wait at once: each is armed on the reactor's wheel, so the
/// nearest one is what the poller sleeps to and a later one stays pending
/// behind it.
#[test]
fn timers_wait_out_their_own_deadlines() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let started = Instant::now();
	let near = reactor
		.insert_timer(Instant::now() + Duration::from_millis(20))
		.unwrap();
	let far = reactor
		.insert_timer(Instant::now() + Duration::from_secs(2))
		.unwrap();
	let mut cx = Context::from_waker(Waker::noop());

	while reactor.poll_timer(near, &mut cx).is_pending() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the poller slept for the far timer's deadline"
	);
	assert!(
		reactor.poll_timer(far, &mut cx).is_pending(),
		"the far timer came due with the near one"
	);
	reactor.cancel_timer(far);
}

/// A wait is capped at the nearest deadline: a poll asked for far longer than
/// the timer still returns when the timer comes due, not when its own timeout
/// passes.
#[test]
fn a_poll_does_not_sleep_past_the_nearest_deadline() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let timer = reactor
		.insert_timer(Instant::now() + Duration::from_millis(50))
		.unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	let started = Instant::now();
	let result = loop {
		match reactor.poll_timer(timer, &mut cx) {
			std::task::Poll::Ready(result) => break result,
			// Far longer than the timer: only the wheel cuts the wait short.
			std::task::Poll::Pending => reactor.poll(Some(Duration::from_secs(5))).unwrap(),
		}
	};
	let elapsed = started.elapsed();
	result.expect("the timer completed");
	assert!(
		elapsed >= Duration::from_millis(50),
		"the timer fired early: {elapsed:?}"
	);
	assert!(
		elapsed < Duration::from_secs(1),
		"the wait slept past the nearest deadline: {elapsed:?}"
	);
}

/// A cancelled timer leaves nothing behind: its poll reports the cancel, and
/// once its deadline has passed no wait is shortened by it.
#[test]
fn a_cancelled_timer_never_fires_and_leaves_no_deadline_behind() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let timer = reactor
		.insert_timer(Instant::now() + Duration::from_millis(20))
		.unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(
		reactor.poll_timer(timer, &mut cx).is_pending(),
		"the timer waits on the wheel"
	);
	reactor.cancel_timer(timer);
	assert!(
		matches!(
			reactor.poll_timer(timer, &mut cx),
			std::task::Poll::Ready(Err(_))
		),
		"the cancelled timer still reported a fire"
	);
	// The deadline passes with nothing to fire.
	std::thread::sleep(Duration::from_millis(50));
	reactor.poll(Some(Duration::ZERO)).unwrap();
	// A stale deadline would cut this wait to zero.
	let started = Instant::now();
	reactor.poll(Some(Duration::from_millis(20))).unwrap();
	assert!(
		started.elapsed() >= Duration::from_millis(20),
		"the cancelled timer's deadline still shortened a wait"
	);
}

/// `Reactor::stop` refuses new work, but the operations already in flight are
/// still driven to their end.
#[test]
fn stopping_the_reactor_drains_what_is_in_flight() {
	let file = seeded_file(b"hello");
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let completion = submit(
		&reactor,
		&file,
		Op::Read {
			offset: 0,
			memory: memory(&buf),
			extents: whole(&buf),
		},
	)
	.unwrap();
	// Stop with the read still in flight: driving the reactor still lands it.
	reactor.stop();
	assert_eq!(
		block_on(&reactor, completion).1.unwrap().count().unwrap(),
		5
	);
	assert_eq!(&buf[..], b"hello");
	assert!(common::idle(&reactor));

	// A stopped reactor takes no more work.
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let error = submit(
		&reactor,
		&file,
		Op::Read {
			offset: 0,
			memory: memory(&buf),
			extents: whole(&buf),
		},
	)
	.err()
	.expect("a stopped reactor refuses submissions");
	assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

/// The reactor's own wake-up is a descriptor a caller can wait on
/// (`Reactor::wait`): a raise from another thread must complete that wait,
/// not only make `poll` return — otherwise the task waiting on it never runs
/// again.
///
/// Windows: a completion port is not a descriptor, so `Reactor::wait` has
/// nothing to arm.
#[cfg(unix)]
#[test]
fn an_event_completes_a_wait_armed_on_it() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let completion = wait_event(&reactor).unwrap();
	// The first pass hands the submission to the backend.
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!common::idle(&reactor), "the wait is in flight");

	reactor.event().notify().unwrap();
	reactor.poll(Some(Duration::from_millis(100))).unwrap();
	assert!(common::idle(&reactor), "a raise left the wait armed");

	let ((), result) = block_on(&reactor, completion);
	result.unwrap();
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
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&reactor, &right, &buf);
	let mut receive = Box::pin(completion);
	let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
	assert!(
		receive.as_mut().poll(&mut cx).is_pending(),
		"the peer never sends, so the receive waits"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!common::idle(&reactor), "the receive has to be in flight");

	// The cancel is raised on the completion: that asks the backend to stop the
	// receive, and leaves the ask recorded on the entry.
	receive.cancel();
	assert!(
		receive.as_mut().poll(&mut cx).is_pending(),
		"the kernel still holds the receive"
	);

	let caught = catch_unwind(AssertUnwindSafe(move || drop(receive)));
	assert!(
		caught.is_ok(),
		"a given-up drop panicked; it is the path a clean stop takes"
	);
	while !common::idle(&reactor) {
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
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&reactor, &right, &buf);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!common::idle(&reactor), "the receive has to be in flight");
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
	assert!(
		common::idle(&reactor),
		"the drop drove the receive to its end"
	);
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
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&reactor, &right, &buf);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!common::idle(&reactor), "the receive has to be in flight");
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

/// A readiness wait is memory-free too, and dropping one does not block.
#[test]
#[cfg(unix)]
fn a_dropped_readiness_wait_does_not_block() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let reactor = Reactor::new(Config::default()).unwrap();
	let reader = Handle::new(reader);
	let waker = std::task::Waker::noop();
	let mut cx = std::task::Context::from_waker(waker);
	let started;
	{
		let mut wait = std::pin::pin!(submit(&reactor, &reader, Op::Readable).unwrap());
		assert!(
			wait.as_mut().poll(&mut cx).is_pending(),
			"the pipe has no data"
		);
		// The first pass hands the wait to the backend.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert!(!common::idle(&reactor), "the wait is in flight");
		started = Instant::now();
	}
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"a readiness wait must not block"
	);
	while !common::idle(&reactor) {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
}

/// Shutting the reactor down with an operation still out drives it to its end, and a
/// completion dropped afterwards finds its entry gone and returns at once.
#[test]
#[cfg(unix)]
fn a_reactor_drop_settles_what_is_in_flight() {
	let (_left, right) = socket_pair();
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&reactor, &right, &buf);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!common::idle(&reactor), "the receive is in flight");
	reactor.shutdown(true);
	assert_eq!(Arc::strong_count(&buf), 1, "the reactor held the buffer");
	let started = Instant::now();
	drop(completion);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"a completion after the reactor is gone returns at once"
	);
}

/// Without force a shutdown lets what is out run to its own end: the receive
/// gets the bytes the peer sends later, not the stopped error.
#[test]
#[cfg(unix)]
fn a_graceful_shutdown_lets_an_operation_land_on_its_own() {
	use std::io::Write;

	let (mut left, right) = socket_pair();
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let mut completion = pin!(stalled_recv(&reactor, &right, &buf));
	reactor.poll(Some(Duration::ZERO)).unwrap();
	let peer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		left.write_all(b"late").unwrap();
		left
	});
	reactor.shutdown(false);
	let _left = peer.join().unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	let std::task::Poll::Ready(((), result)) = completion.as_mut().poll(&mut cx) else {
		panic!("the shutdown returned with the receive still out");
	};
	assert_eq!(result.unwrap().count().unwrap(), 4);
	assert_eq!(&buf[..4], b"late");
}

/// A shutdown ends the operations still out, but a completion that outlives it
/// still resolves: to the stopped error a submission after the shutdown gets.
#[test]
#[cfg(unix)]
fn a_completion_polled_after_shutdown_resolves_to_stopped() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let reactor = Reactor::new(Config::default()).unwrap();
	let reader = Handle::new(reader);
	let mut wait = pin!(submit(&reactor, &reader, Op::Readable).unwrap());
	let mut cx = Context::from_waker(Waker::noop());
	assert!(
		wait.as_mut().poll(&mut cx).is_pending(),
		"the pipe has no data"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	reactor.shutdown(true);
	let std::task::Poll::Ready(((), result)) = wait.as_mut().poll(&mut cx) else {
		panic!("a completion after shutdown stayed pending");
	};
	assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
}

/// A cancel its holder asked for before a shutdown keeps reading as a cancel,
/// even when the operation never reached the backend.
#[test]
#[cfg(unix)]
fn a_cancel_before_shutdown_still_reads_as_a_cancel() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let reactor = Reactor::new(Config::default()).unwrap();
	let reader = Handle::new(reader);
	let mut wait = pin!(submit(&reactor, &reader, Op::Readable).unwrap());
	wait.cancel();
	reactor.shutdown(true);
	let mut cx = Context::from_waker(Waker::noop());
	let std::task::Poll::Ready(((), result)) = wait.as_mut().poll(&mut cx) else {
		panic!("a completion after shutdown stayed pending");
	};
	let error = result.unwrap_err();
	assert!(reactor.is_cancelled(&error), "the cancel became {error}");
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
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&reactor, &right, &buf).tag("frame");
	// One pass puts the receive in flight, so the cancel reaches the kernel's op.
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!common::idle(&reactor), "the receive has to be in flight");
	completion.cancel();
	let (tag, result) = block_on(&reactor, completion);
	let error = result.expect_err("the receive completed after the cancel");
	assert!(
		reactor.is_cancelled(&error),
		"the receive was not cancelled: {error:?}"
	);
	assert_eq!(tag, "frame", "the userdata came back");
	(&left).write_all(b"late").unwrap();
	reactor.poll(Some(Duration::from_millis(20))).unwrap();
	assert_eq!(&buf[..], &[0u8; 8], "a byte arrived after the cancel");
}

/// A readiness wait names nothing of the caller's, so dropping one is not a
/// bug — but it still has to be stopped and reaped: until its entry goes, the
/// wait holds the descriptor open, and a wait on a descriptor that never
/// becomes ready has no such end.
#[test]
#[cfg(unix)]
fn a_dropped_readiness_wait_is_cancelled_and_reaped() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut reader = Handle::new(reader);
	let mut cx = Context::from_waker(Waker::noop());
	{
		let mut wait = pin!(submit(&reactor, &reader, Op::Readable).unwrap());
		assert!(
			wait.as_mut().poll(&mut cx).is_pending(),
			"the pipe has no data"
		);
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert!(!common::idle(&reactor), "the wait is in flight");
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
	let reactor = Reactor::new(Config::default()).unwrap();
	let socket = Handle::new(stream);
	let mut cx = Context::from_waker(Waker::noop());

	let bytes: Arc<Vec<u8>> = Arc::new(vec![1u8; 5]);
	let payload: Arc<dyn Memory> = Arc::clone(&bytes) as Arc<dyn Memory>;
	let mut send = Box::pin(
		submit(
			&reactor,
			&socket,
			Op::SendWithFds {
				memory: payload,
				extents: whole(&bytes),
				fds: Vec::new(),
			},
		)
		.unwrap(),
	);
	assert!(
		send.as_mut().poll(&mut cx).is_pending(),
		"the full socket cannot take the bytes"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!common::idle(&reactor), "the send has to be in flight");
	drop_in_flight_on_purpose(move || drop(send));
	assert_eq!(Arc::strong_count(&bytes), 1, "the send released its buffer");
	while !common::idle(&reactor) {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}

	let empty: Arc<Vec<u8>> = Arc::new(Vec::new());
	let payload: Arc<dyn Memory> = Arc::clone(&empty) as Arc<dyn Memory>;
	let mut send = Box::pin(
		submit(
			&reactor,
			&socket,
			Op::SendWithFds {
				memory: payload,
				extents: whole(&empty),
				fds: Vec::new(),
			},
		)
		.unwrap(),
	);
	assert!(
		send.as_mut().poll(&mut cx).is_pending(),
		"the send cannot land before the reactor drains it"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	drop(send);
	while !common::idle(&reactor) {
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
	fn sqpoll_reactor() -> Reactor {
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
		let reactor = sqpoll_reactor();
		let buf = Arc::new(vec![0u8; 8]);
		let completion = stalled_recv(&reactor, &right, &buf);
		// This thread is the reactor's driver from here on.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert!(
			!common::idle(&reactor),
			"the receive has to be in flight for the fallback to run"
		);
		assert_eq!(Arc::strong_count(&buf), 2, "the submission holds it");
		drop_in_flight_on_purpose(|| drop(completion));
		assert_eq!(
			Arc::strong_count(&buf),
			1,
			"the kernel still held the buffer"
		);
		assert!(
			common::idle(&reactor),
			"the drop drove the receive to its end"
		);
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
		let reactor = sqpoll_reactor();
		let buf = Arc::new(vec![0u8; 8]);
		let completion = stalled_recv(&reactor, &right, &buf);
		reactor.poll(Some(Duration::ZERO)).unwrap();
		// A wake raised through the reactor's protocol, landed while the receive is in
		// flight: the timer is only what raises it, and it goes before the pass.
		let timer = reactor
			.insert_timer(Instant::now() + Duration::from_secs(2))
			.unwrap();
		drop_in_flight_on_purpose(|| drop(completion));
		reactor.cancel_timer(timer);
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
		let reactor = Reactor::new(Config::default()).unwrap();
		let buf = Arc::new(vec![0u8; 4]);
		let completion = submit(
			&reactor,
			&right,
			Op::Read {
				offset: 0,
				memory: memory(&buf),
				extents: whole(&buf),
			},
		)
		.unwrap();
		// The receive goes to the kernel with nothing to move, so it is in flight
		// and unsettled when the completion is next looked at.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert_eq!(Arc::strong_count(&buf), 2, "the kernel holds the buffer");
		// The bytes are queued, and the kernel finishes the receive on its own.
		left.write_all(b"wins").unwrap();
		// The cancel fires before the outcome is looked at, so the cancel is
		// requested with the receive in flight; the receive gets there first.
		completion.cancel();
		let mut until = std::pin::pin!(completion);
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
		let ((), response) = ready.expect("the receive landed");
		let response = response.expect("the bytes are the operation's own result, cancel or not");
		assert_eq!(response.count().unwrap(), 4);
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
		let reactor = Reactor::new(Config::default()).unwrap();
		let buf = Arc::new(vec![0u8; 4]);
		let completion = submit(
			&reactor,
			&right,
			Op::Read {
				offset: 0,
				memory: memory(&buf),
				extents: whole(&buf),
			},
		)
		.unwrap();
		// The receive is armed with nothing to move, so it is in flight and
		// unsettled when the completion is next looked at.
		reactor.poll(Some(Duration::ZERO)).unwrap();
		assert_eq!(Arc::strong_count(&buf), 2, "the operation holds the buffer");
		// The bytes are queued, and no pass has run the receive over them.
		left.write_all(b"wins").unwrap();
		// The cancel fires before the outcome is looked at, so the cancel is
		// requested with the receive in flight; the receive never gets there.
		completion.cancel();
		let mut until = std::pin::pin!(completion);
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
		let ((), response) = ready.expect("the receive was stopped");
		let error = response.expect_err("the receive was stopped");
		assert!(
			reactor.is_cancelled(&error),
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
