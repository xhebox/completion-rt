//! Cancellation: the cooperative path ([`Cancel`] and `until`) and the drop
//! fallback, which has to hold until the backend has let go of the caller's
//! memory.
//!
//! The tests wait on a socket pair, so the whole file is unix: a port that
//! cannot be made to wait has nothing to cancel.
#![cfg(unix)]

mod common;

use common::block_on;

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use completion_rt::{
	Accept, AsDescriptor, Cancel, Cancelled, Completion, Config, Executor, Extent, Facade, Handle,
	ListenerHandle, Memory, Op, Reactor, Receive, Submitter, Transfer, Until,
};

/// A connected pair, both ends non-blocking: a receive on one waits for a send
/// on the other. The receiving end comes shared, which is the shape a
/// submission takes.
fn socket_pair() -> (UnixStream, Handle<UnixStream>) {
	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	(left, Handle::new(right))
}

fn memory(buf: &Arc<Vec<u8>>) -> Arc<dyn Memory> {
	let memory: Arc<dyn Memory> = buf.clone();
	memory
}

fn whole(buf: &Arc<Vec<u8>>) -> [Extent; 1] {
	[Extent {
		offset: 0,
		len: buf.len(),
	}]
}

/// A read that nothing will satisfy: the peer never sends.
fn stalled_recv(
	submitter: &Submitter,
	right: &Handle<UnixStream>,
	buf: &Arc<Vec<u8>>,
) -> Completion {
	submitter.read(right, 0, memory(buf), whole(buf)).unwrap()
}

/// Drops an in-flight operation on purpose. The fallback panics once the
/// backend is off the memory when the build has debug assertions; the panic is
/// caught here so the test can go on checking what the drop did.
fn drop_in_flight_on_purpose(drop: impl FnOnce()) {
	let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(drop));
	assert_eq!(
		caught.is_err(),
		cfg!(debug_assertions),
		"an in-flight drop panics exactly when the build has debug assertions"
	);
}

/// The facade futures keep their state in named fields, not a boxed
/// `dyn Future`, so they are `Send` wherever the singleton they name is: a
/// driver may hand one to another thread.
#[test]
fn a_facade_future_is_send_when_its_singleton_is() {
	fn assert_send<T: Send>() {}
	fn transfer<S: AsDescriptor + Send + Sync + 'static>() {
		assert_send::<Transfer<S>>();
		assert_send::<Receive<S>>();
		assert_send::<Op<S>>();
	}
	fn accept<S: ListenerHandle>() {
		assert_send::<Accept<S>>();
	}
	assert_send::<Until<()>>();
	transfer::<UnixStream>();
	accept::<std::net::TcpListener>();
}

/// `until` returns `Ok` when the operation completes first, even with a cancel
/// that has not fired.
#[test]
fn until_completes_when_the_op_lands_first() {
	let dir = common::tempdir("completion-until-ok").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let file = Handle::new(std::fs::File::open(path).unwrap());

	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 5]);
	let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
	let cancel = Cancel::new();
	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let driven = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			while !stop.load(std::sync::atomic::Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};
	let result = block_on(completion.until(&cancel));
	stop.store(true, std::sync::atomic::Ordering::SeqCst);
	driven.join().unwrap();
	let ((), response) = result.expect("the read completed first");
	assert_eq!(response.unwrap().count().unwrap(), 5);
	assert_eq!(&buf[..], b"hello");
}

/// A timer is cancelled like any other op: `until` asks the reactor to stop it,
/// and the entry settles as cancelled rather than waiting out its deadline.
#[test]
fn until_cancels_a_timer() {
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let cancel = Cancel::new();
	let completion = submitter.timeout(Duration::from_secs(30)).unwrap();
	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let driven = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			while !stop.load(std::sync::atomic::Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};
	cancel.cancel();
	let started = Instant::now();
	let result = block_on(completion.until(&cancel));
	stop.store(true, std::sync::atomic::Ordering::SeqCst);
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

/// A `cancel()` on another thread raises the reactor's wake-up, so a thread
/// parked in `block_on` comes back and the `until` future resolves.
#[test]
fn a_cancel_from_another_thread_wakes_a_parked_block_on() {
	let mut executor = Executor::new(Config::default()).unwrap();
	let submitter = executor.submitter();
	let (left, right) = socket_pair();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	let cancel = Cancel::new();
	let raiser = cancel.clone();
	let thread = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		raiser.cancel();
	});
	let started = Instant::now();
	let result = executor.block_on(completion.until(&cancel)).unwrap();
	thread.join().unwrap();
	assert!(matches!(result, Err(Cancelled(()))));
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

/// The fallback counter is the witness a caller uses to see a missed
/// cooperative cancel: a completion dropped while its operation still names the
/// caller's memory counts once, and a memory-free drop counts not at all.
///
/// The count is read after the drop's panic has been caught, so it is the
/// count the drop left behind either way.
#[test]
fn a_fallback_drop_is_counted_and_a_memory_free_one_is_not() {
	let (_left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let before = reactor.fallback_drops();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the receive has to be in flight");
	drop_in_flight_on_purpose(|| drop(completion));
	assert_eq!(
		reactor.fallback_drops(),
		before + 1,
		"the in-flight receive was waited out"
	);

	let timer = submitter.timeout(Duration::from_secs(30)).unwrap();
	reactor.poll(Some(Duration::ZERO)).unwrap();
	drop(timer);
	assert_eq!(
		reactor.fallback_drops(),
		before + 1,
		"a memory-free drop is not a fallback"
	);
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
}

/// The count belongs to the reactor the completion was dropped on: a witness a
/// test can read without another reactor's or another test's drops in it.
#[test]
fn a_reactor_counts_only_its_own_fallback_drops() {
	let (_left, right) = socket_pair();
	let (idle, _idle_submitter) = Reactor::new(Config::default()).unwrap();
	let (mut busy, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	busy.poll(Some(Duration::ZERO)).unwrap();
	assert!(!busy.is_idle(), "the receive has to be in flight");
	drop_in_flight_on_purpose(|| drop(completion));
	assert_eq!(busy.fallback_drops(), 1, "the drop was waited out");
	assert_eq!(
		idle.fallback_drops(),
		0,
		"a drop on one reactor was counted against another"
	);
	while !busy.is_idle() {
		busy.poll(Some(Duration::from_millis(1))).unwrap();
	}
}

/// A debug build panics on the drop, and the panic comes once the kernel has
/// let go: the buffer is the caller's again, and the bytes the peer sends
/// afterwards reach no operation.
#[test]
fn a_dropped_in_flight_read_panics_after_the_kernel_let_go() {
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
			message.contains("cancel.rs"),
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

/// The message the fallback's panic carries.
fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
	match panic.downcast::<String>() {
		Ok(text) => *text,
		Err(other) => match other.downcast::<&str>() {
			Ok(text) => (*text).to_owned(),
			Err(_) => String::new(),
		},
	}
}

/// A completion dropped on another thread notifies the reactor and waits on the
/// shared condvar until its entry lands.
#[test]
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
	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let driven = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			while !stop.load(std::sync::atomic::Ordering::SeqCst) {
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
	stop.store(true, std::sync::atomic::Ordering::SeqCst);
	driven.join().unwrap();
}

/// A readiness wait is memory-free too, and dropping one does not block.
#[test]
fn a_dropped_readiness_wait_does_not_block() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let reader: Facade<std::io::PipeReader> = Facade::new(reader, &submitter);
	let waker = std::task::Waker::noop();
	let mut cx = std::task::Context::from_waker(waker);
	let started;
	{
		let mut wait = std::pin::pin!(reader.wait_readable());
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
fn a_cancelled_receive_hands_back_only_the_userdata() {
	use std::io::Write;

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let right = Handle::new(right);
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf).tag("frame");
	let cancel = Cancel::new();
	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let driven = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			while !stop.load(std::sync::atomic::Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};
	std::thread::sleep(Duration::from_millis(20));
	cancel.cancel();
	let result = block_on(completion.until(&cancel));
	stop.store(true, std::sync::atomic::Ordering::SeqCst);
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

/// A cancel spans the reactors its waiters run on, not the number of waits: a
/// reactor that only appears ninth still gets its wake-up registered, so a
/// cancel from another thread reaches it.
#[test]
fn a_cancel_used_on_many_reactors_reaches_the_last() {
	let dir = common::tempdir("completion-cancel-reactors").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let file = Handle::new(std::fs::File::open(path).unwrap());

	// One reactor, waited on eight times: a wake-up list that grew per wait
	// would be exactly full by the end of this.
	let mut first = Executor::new(Config::default()).unwrap();
	let first_submitter = first.submitter();
	let cancel = Cancel::new();
	for _ in 0..8 {
		let buf = Arc::new(vec![0u8; 5]);
		let completion = first_submitter
			.read(&file, 0, memory(&buf), whole(&buf))
			.unwrap();
		let ((), response) = first
			.block_on(completion.until(&cancel))
			.unwrap()
			.expect("the read completed");
		response.expect("the read produced no error");
	}

	// A second reactor, the ninth the cancel spans. Only a cancel from another
	// thread wakes it, and only if its wake-up was registered.
	let (_left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = stalled_recv(&submitter, &right, &buf);
	let mut until = std::pin::pin!(completion.until(&cancel));
	let waker = std::task::Waker::noop();
	let mut cx = std::task::Context::from_waker(waker);
	// The first poll registers the receive and the cancel's wake-up with it.
	assert!(std::future::Future::poll(until.as_mut(), &mut cx).is_pending());
	reactor.poll(Some(Duration::ZERO)).unwrap();
	let raiser = cancel.clone();
	let thread = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		raiser.cancel();
	});
	let started = Instant::now();
	let mut ready = None;
	while started.elapsed() < Duration::from_secs(5) {
		if let std::task::Poll::Ready(value) = std::future::Future::poll(until.as_mut(), &mut cx) {
			ready = Some(value);
			break;
		}
		// One long park: only the cancel's wake-up ends it early.
		reactor.poll(Some(Duration::from_secs(2))).unwrap();
	}
	thread.join().unwrap();
	assert!(
		matches!(ready, Some(Err(Cancelled(())))),
		"the cancel never reached the second reactor"
	);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the second reactor's wait was not woken by the cancel"
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
fn a_drop_racing_the_reactor_leaves_no_request_running() {
	let (left, right) = socket_pair();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let reactor_thread = {
		let stop = Arc::clone(&stop);
		std::thread::spawn(move || {
			// A reactor that is always driving, so the drop always races a pass.
			while !stop.load(std::sync::atomic::Ordering::SeqCst) {
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
					caught.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
				}
			}
		}));
	}
	for worker in workers {
		worker.join().unwrap();
	}
	stop.store(true, std::sync::atomic::Ordering::SeqCst);
	let mut reactor = reactor_thread.join().unwrap();
	// Let the reactor finish whatever the workers left pending.
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert_eq!(
		caught.load(std::sync::atomic::Ordering::SeqCst),
		0,
		"a drop returned while the kernel still held the buffer"
	);
	let _ = left;
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

	/// The inline drive runs the whole reactor, so a wake-up it consumes on the way
	/// has to be raised again: the next pass would otherwise sleep through news the
	/// drive left behind.
	///
	/// The drive only reaches its wait on a ring whose completions arrive on another
	/// thread (SQPOLL): an operation cancelled on the polling ring itself lands
	/// before the wait, and the raise is left alone.
	#[test]
	fn an_inline_drive_leaves_no_wake_up_behind() {
		let (_left, right) = socket_pair();
		let (mut reactor, submitter) = sqpoll_reactor();
		let buf = Arc::new(vec![0u8; 8]);
		let completion = stalled_recv(&submitter, &right, &buf);
		reactor.poll(Some(Duration::ZERO)).unwrap();
		// A raise from another thread, landed while the receive is in flight.
		submitter.event().notify().unwrap();
		drop_in_flight_on_purpose(|| drop(completion));
		// The drop's inline drive parked and ate the raise; the pass after must not
		// sleep on it.
		let started = Instant::now();
		reactor.poll(Some(Duration::from_secs(2))).unwrap();
		assert!(
			started.elapsed() < Duration::from_secs(1),
			"the pass slept through the wake-up the inline drive consumed"
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
#[cfg(not(target_os = "linux"))]
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
		let mut reader: &UnixStream = &right;
		let mut got = [0u8; 4];
		reader.read_exact(&mut got).unwrap();
		assert_eq!(&got, b"wins", "the bytes stayed with the peer");
	}
}

/// A send that carries bytes names the caller's memory — its descriptors ride
/// in the backend's control buffer, not the caller's memory — so dropping one
/// in flight is a fallback the reactor waits out. A send carrying no data
/// names nothing and drops without one.
#[test]
fn a_send_is_memory_free_only_without_data() {
	use std::future::Future;
	use std::task::{Context, Waker};

	let (stream, _peer) = UnixStream::pair().unwrap();
	stream.set_nonblocking(true).unwrap();
	// Fill the socket's buffers so a send that carries bytes cannot take any:
	// the facade send then stays with the kernel instead of landing short.
	let filler = [0u8; 64 << 10];
	loop {
		match (&stream).write(&filler) {
			Ok(0) => break,
			Ok(_) => continue,
			Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
			Err(error) => panic!("filling the socket failed: {error}"),
		}
	}
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let socket: Facade<UnixStream> = Facade::new(stream, &submitter);
	let mut cx = Context::from_waker(Waker::noop());

	let bytes: Arc<Vec<u8>> = Arc::new(vec![1u8; 5]);
	let payload: Arc<dyn Memory> = Arc::clone(&bytes) as Arc<dyn Memory>;
	let before = reactor.fallback_drops();
	let mut send = Box::pin(socket.send_with_fds(payload, Vec::new()));
	assert!(
		send.as_mut().poll(&mut cx).is_pending(),
		"the full socket cannot take the bytes"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.is_idle(), "the send has to be in flight");
	drop_in_flight_on_purpose(move || drop(send));
	assert_eq!(
		reactor.fallback_drops(),
		before + 1,
		"the send named the bytes it carried"
	);
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}

	let empty: Arc<Vec<u8>> = Arc::new(Vec::new());
	let payload: Arc<dyn Memory> = Arc::clone(&empty) as Arc<dyn Memory>;
	let before = reactor.fallback_drops();
	let mut send = Box::pin(socket.send_with_fds(payload, Vec::new()));
	assert!(
		send.as_mut().poll(&mut cx).is_pending(),
		"the send cannot land before the reactor drains it"
	);
	reactor.poll(Some(Duration::ZERO)).unwrap();
	drop(send);
	assert_eq!(
		reactor.fallback_drops(),
		before,
		"a send of no bytes names nothing of the caller's"
	);
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
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

/// A readiness wait names nothing of the caller's, so dropping one is not a
/// bug — but it still has to be stopped and reaped: until its entry goes, the
/// wait holds the descriptor open, and a wait on a descriptor that never
/// becomes ready has no such end.
#[test]
fn a_dropped_readiness_wait_is_cancelled_and_reaped() {
	use std::future::Future;
	use std::task::{Context, Waker};

	let (reader, _writer) = std::io::pipe().unwrap();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let mut reader: Facade<std::io::PipeReader> = Facade::new(reader, &submitter);
	let mut cx = Context::from_waker(Waker::noop());
	{
		let mut wait = std::pin::pin!(reader.wait_readable());
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
			Err(facade) => facade,
		};
	}
	assert!(taken, "the abandoned wait still held the pipe");
}
