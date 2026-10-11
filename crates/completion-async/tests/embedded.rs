//! The embedded executor: a caller that owns the poll loop waits on
//! [`Executor::poll_fd`] with its own poller and drives the reactor with
//! [`Executor::turn`] once per wake, and the tasks a spawner made run in
//! those passes.
//!
//! These are the waits a facade cannot shape — a bare descriptor to wait on,
//! a ring that has none — reached through the async surface rather than a
//! reactor the caller polls by hand.

mod common;

use std::io;
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
use std::time::Duration;

use completion_async::{Config, Executor};

/// The wait [`Executor::turn`] asked for, capped at five seconds when it
/// named none: a test must fail rather than hang if the descriptor never becomes
/// ready.
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn wait_for(wait: Option<Duration>) -> rustix::event::Timespec {
	rustix::event::Timespec::try_from(wait.unwrap_or(Duration::from_secs(5)))
		.expect("a representable timeout")
}

/// An embedded executor's caller waits on the descriptor with its own poller and
/// drives once it fires, instead of polling with a timeout.
#[test]
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn waiting_on_the_descriptor_drives_the_backend() {
	use std::os::fd::AsRawFd;
	use std::sync::Arc;

	use completion_async::{Error, File, Memory};
	use rustix::event::{PollFd, PollFlags, poll};

	let directory = common::tempdir("completion-embedded").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();

	let mut executor = Executor::embedded(Config::default()).unwrap();
	assert!(
		executor.poll_fd().unwrap().as_raw_fd() >= 0,
		"an embedded executor hands out a descriptor to wait on"
	);

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let memory: Arc<dyn Memory> = Arc::clone(&buffer) as Arc<dyn Memory>;
	let file = File::new(std::fs::File::open(&path).unwrap(), &executor.submitter());
	let (done, finished) = std::sync::mpsc::channel::<Result<usize, Error>>();
	executor
		.spawner()
		.spawn(async move {
			let result = file.read_at(memory, 0).await.into_result();
			let _ = done.send(result);
		})
		.detach();

	// The descriptor says "something finished"; poll takes the wake out and the
	// pass after it runs the task. Each wait is what the previous turn reported,
	// capped at five seconds. The read of a plain file may land on io-wq, so it is
	// not instantaneous.
	let started = std::time::Instant::now();
	let mut wait = None;
	let mut received = None;
	for _ in 0..1000 {
		if let Ok(result) = finished.try_recv() {
			received = Some(result);
			break;
		}
		assert!(
			started.elapsed() < Duration::from_secs(5),
			"the read never completed"
		);
		// The borrow ends before `turn`, which needs the executor mutably.
		{
			let descriptor = executor.poll_fd().unwrap();
			let mut fds = [PollFd::new(&descriptor, PollFlags::IN)];
			poll(&mut fds, Some(&wait_for(wait))).unwrap();
		}
		wait = executor.turn().unwrap();
	}
	let n = received
		.expect("the read never completed")
		.expect("the read failed");
	assert_eq!(n, 5);
	assert_eq!(&buffer[..], b"hello");
}

/// `turn` reports the time to the next timer deadline, which the external
/// poller cannot know itself: a caller that waits exactly that long wakes the
/// timer, where its own timeout would otherwise be what ends the wait.
#[test]
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn a_timer_fires_within_the_timeout_turn_reports() {
	use completion_async::Timeout;
	use rustix::event::{PollFd, PollFlags, poll};

	let mut executor = Executor::embedded(Config::default()).unwrap();
	let submitter = executor.submitter();
	let (done, finished) = std::sync::mpsc::channel::<io::Result<()>>();
	executor
		.spawner()
		.spawn(async move {
			let _ = done.send(Timeout::new(Duration::from_millis(150), &submitter).await);
		})
		.detach();

	let started = std::time::Instant::now();
	let mut wait = None;
	let mut received = None;
	for _ in 0..1000 {
		if let Ok(result) = finished.try_recv() {
			received = Some(result);
			break;
		}
		assert!(
			started.elapsed() < Duration::from_secs(1),
			"the timer did not fire on the wait turn reported: {:?}",
			started.elapsed()
		);
		// The borrow ends before `turn`, which needs the executor mutably.
		{
			let descriptor = executor.poll_fd().unwrap();
			let mut fds = [PollFd::new(&descriptor, PollFlags::IN)];
			poll(&mut fds, Some(&wait_for(wait))).unwrap();
		}
		wait = executor.turn().unwrap();
	}
	received
		.expect("the timer never completed")
		.expect("the timer failed");
}

/// An idle embedded executor's descriptor is quiet: the turn drains the
/// notification the poller woke on, and nothing re-raises it, so a
/// level-triggered poller is not left busy-spinning.
#[test]
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn an_idle_embedded_executor_leaves_the_descriptor_quiet() {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};

	use rustix::event::{PollFd, PollFlags, Timespec, poll};

	let mut executor = Executor::embedded(Config::default()).unwrap();
	let ran = Arc::new(AtomicUsize::new(0));
	let flag = Arc::clone(&ran);
	executor
		.spawner()
		.spawn(async move {
			flag.fetch_add(1, Ordering::AcqRel);
		})
		.detach();

	let mut wait = None;
	for _ in 0..1000 {
		if ran.load(Ordering::Acquire) != 0 {
			break;
		}
		{
			let descriptor = executor.poll_fd().unwrap();
			let mut fds = [PollFd::new(&descriptor, PollFlags::IN)];
			poll(&mut fds, Some(&wait_for(wait))).unwrap();
		}
		wait = executor.turn().unwrap();
	}
	assert_eq!(ran.load(Ordering::Acquire), 1, "the trivial task never ran");

	// One more pass, as the caller takes after every wake.
	executor.turn().unwrap();

	let descriptor = executor.poll_fd().unwrap();
	let mut fds = [PollFd::new(&descriptor, PollFlags::IN)];
	let ready = poll(
		&mut fds,
		Some(&Timespec {
			tv_sec: 0,
			tv_nsec: 0,
		}),
	)
	.unwrap();
	assert_eq!(
		ready, 0,
		"an idle embedded executor's descriptor is still readable"
	);
}

/// More ready tasks than one tick's budget still run: `turn` reports a zero wait
/// while any remain, so the caller comes straight back instead of stalling on a
/// descriptor nothing has raised.
#[test]
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn every_task_runs_past_the_tick_budget() {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};

	use rustix::event::{PollFd, PollFlags, poll};

	let mut executor = Executor::embedded(Config::default()).unwrap();
	let ran = Arc::new(AtomicUsize::new(0));
	let spawner = executor.spawner();
	for _ in 0..200 {
		let flag = Arc::clone(&ran);
		spawner
			.spawn(async move {
				flag.fetch_add(1, Ordering::AcqRel);
			})
			.detach();
	}

	let started = std::time::Instant::now();
	let mut wait = None;
	for _ in 0..1000 {
		if ran.load(Ordering::Acquire) == 200 {
			break;
		}
		assert!(
			started.elapsed() < Duration::from_secs(1),
			"tasks past the tick budget waited on an unrelated event: {} ran",
			ran.load(Ordering::Acquire)
		);
		// The borrow ends before `turn`, which needs the executor mutably.
		{
			let descriptor = executor.poll_fd().unwrap();
			let mut fds = [PollFd::new(&descriptor, PollFlags::IN)];
			poll(&mut fds, Some(&wait_for(wait))).unwrap();
		}
		wait = executor.turn().unwrap();
	}
	assert_eq!(ran.load(Ordering::Acquire), 200, "not every task ran");
}

/// An executor that cannot hand out a descriptor fails when it is built, not when
/// someone first waits on a descriptor that will never become ready.
#[test]
fn embedded_with_iopoll_is_rejected_up_front() {
	let error = Executor::embedded(Config {
		iopoll: true,
		..Config::default()
	})
	.err()
	.expect("an embedded executor with IOPOLL must be rejected");
	assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}
