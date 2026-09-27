//! End-to-end core tests against the real backend: completions land, a
//! submission keeps its buffers alive, and a cancelled file operation still
//! lands: a worker inside a syscall cannot be called back.
//! The reactor has no thread of its own, so every test drives it the way a
//! caller would.
mod common;

use common::block_on;

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

#[cfg(unix)]
use completion_rt::Facade;
use completion_rt::{Completion, Config, Extent, Handle, Memory, PollMode, Reactor, Response};

fn seeded_file(contents: &[u8]) -> Handle<std::fs::File> {
	let dir = common::tempdir("completion-seed").unwrap();
	let path = dir.path().join("data");
	std::fs::write(&path, contents).unwrap();
	// The returned descriptor outlives the deleted directory entry.
	Handle::new(std::fs::File::open(path).unwrap())
}

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

/// Polls until the operation is done, then takes the result. A read of a plain
/// file may land on io-wq, so this waits rather than assuming one poll is
/// enough.
fn run(reactor: &mut Reactor, completion: Completion) -> io::Result<Response> {
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	block_on(completion).1
}

/// A read completes with the bytes its one call moved: the file has five, the
/// buffer room for eight, and the completion is the five. A caller that wants
/// the eight loops over `File::read_exact_at`.
#[test]
fn a_read_short_of_the_buffer_completes_with_its_count() {
	let file = seeded_file(b"hello");
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 8]);
	let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
	let n = run(&mut reactor, completion).unwrap().count().unwrap();
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

	use completion_rt::AsDescriptor as _;

	let directory = common::tempdir("completion-fd-hold").unwrap();
	let first = directory.path().join("first");
	let second = directory.path().join("second");
	std::fs::write(&first, b"first").unwrap();
	std::fs::write(&second, b"second!").unwrap();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();

	let file = Handle::new(std::fs::File::open(&first).unwrap());
	let number: RawFd = file.as_descriptor().as_raw_fd();
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
	// The caller's copy goes before anything reaches the kernel.
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

	assert_eq!(run(&mut reactor, completion).unwrap().count().unwrap(), 5);
	assert_eq!(&buf[..], b"first", "the read landed on another file");
}

/// A dropped completion whose request never reached a `poll` still releases
/// the entry, so the backend never sees it and the buffer goes with it.
#[test]
fn a_dropped_completion_releases_an_unsubmitted_request() {
	let file = seeded_file(b"data");
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 4]);
	let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
	assert_eq!(Arc::strong_count(&buf), 2, "the request holds the buffer");
	// Nothing polled: the entry is still `Pending`, so it goes with the drop.
	drop(completion);
	assert_eq!(Arc::strong_count(&buf), 1, "the request goes with it");
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	assert_eq!(Arc::strong_count(&buf), 1, "and nothing held it after");
}

/// `until` returns `Cancelled` only after the kernel has let go of the spans:
/// the caller's handle is all that is left of the buffer when it does, and a
/// byte written to the peer afterwards lands nowhere.
#[test]
#[cfg(unix)]
fn cancelling_waits_for_the_kernel() {
	use std::io::Write;
	use std::os::unix::net::UnixStream;

	use completion_rt::{Cancel, Cancelled};

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let left = Handle::new(left);
	let right = Handle::new(right);
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(vec![0u8; 8]);
	let completion = submitter
		.read(&right, 0, memory(&buf), whole(&buf))
		.unwrap();
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
	// Let the reactor hand the receive to the kernel.
	std::thread::sleep(Duration::from_millis(20));
	assert_eq!(Arc::strong_count(&buf), 2, "the submission holds it");
	let mut until = std::pin::pin!(completion.until(&cancel));
	let waker = std::task::Waker::noop();
	let mut cx = std::task::Context::from_waker(waker);
	assert!(
		std::future::Future::poll(until.as_mut(), &mut cx).is_pending(),
		"the read has no data"
	);
	cancel.cancel();
	let result = block_on(until);
	assert!(matches!(result, Err(Cancelled(()))));
	// The kernel is done, so only the caller's handle remains.
	assert_eq!(Arc::strong_count(&buf), 1);
	// A write after the cancel lands nowhere: the kernel is no longer reading
	// into the buffer.
	(&*left).write_all(b"late data").unwrap();
	std::thread::sleep(Duration::from_millis(20));
	assert_eq!(&buf[..], &[0u8; 8], "a byte arrived after the cancel");
	stop.store(true, Ordering::SeqCst);
	driven.join().unwrap();
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

#[test]
fn concurrent_submits_all_complete() {
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let stop = Arc::new(AtomicBool::new(false));
	let driven = {
		let stop = stop.clone();
		std::thread::spawn(move || {
			while !stop.load(Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};
	let mut handles = Vec::new();
	for i in 0..8u8 {
		let submitter = submitter.clone();
		let file = seeded_file(&[i; 64]);
		handles.push(std::thread::spawn(move || {
			let buf: Arc<Vec<u8>> = Arc::new(vec![0; 64]);
			let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
			let n = block_on(completion).1.unwrap().count().unwrap();
			assert_eq!(n, 64);
			assert!(buf.iter().all(|&b| b == i));
		}));
	}
	for handle in handles {
		handle.join().unwrap();
	}
	stop.store(true, Ordering::SeqCst);
	driven.join().unwrap();
}

/// The pool runs a fixed number of file operations at a time and the rest
/// wait for a worker: a caller submits as many as it likes, and every one of
/// them lands.
#[test]
fn many_transfers_all_complete() {
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let stop = Arc::new(AtomicBool::new(false));
	let driven = {
		let stop = stop.clone();
		std::thread::spawn(move || {
			while !stop.load(Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};
	let file = seeded_file(&[7u8; 4096]);
	let mut pending = Vec::new();
	for _ in 0..4096 {
		let buf: Arc<Vec<u8>> = Arc::new(vec![0u8; 4096]);
		let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
		pending.push((completion, buf));
	}
	for (completion, buf) in pending {
		let (_, result) = block_on(completion);
		assert_eq!(result.unwrap().count().unwrap(), 4096);
		assert!(buf.iter().all(|&byte| byte == 7));
	}
	stop.store(true, Ordering::SeqCst);
	driven.join().unwrap();
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

/// A `PollMode::Fd` caller waits on the descriptor with its own poller and drives
/// once it fires, instead of polling with a timeout.
#[test]
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn waiting_on_the_descriptor_drives_the_backend() {
	use std::os::fd::AsRawFd;

	use rustix::event::{PollFd, PollFlags, Timespec, poll};

	let file = seeded_file(b"hello");
	let (mut reactor, submitter) = Reactor::new(Config {
		wakeup: PollMode::Fd,
		..Config::default()
	})
	.unwrap();
	assert!(
		reactor.poll_fd().unwrap().as_raw_fd() >= 0,
		"fd mode hands out a descriptor to wait on"
	);
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
	// The descriptor says "something finished"; poll takes it out. The read of a plain
	// file may land on io-wq, so it is not instantaneous.
	let timeout = Timespec {
		tv_sec: 0,
		tv_nsec: 100_000_000,
	};
	for _ in 0..100 {
		if reactor.is_idle() {
			break;
		}
		// The borrow ends before `poll`, which needs the reactor mutably.
		{
			let descriptor = reactor.poll_fd().unwrap();
			let mut fds = [PollFd::new(&descriptor, PollFlags::IN)];
			poll(&mut fds, Some(&timeout)).unwrap();
		}
		reactor.poll(Some(Duration::ZERO)).unwrap();
	}
	assert!(reactor.is_idle(), "the operation never completed");
	let n = block_on(completion).1.unwrap().count().unwrap();
	assert_eq!(n, 5);
	assert_eq!(&buf[..], b"hello");
}

/// A poll that does not wait has not used the notification, so it must leave
/// it raised: another thread may have raised it while this poll ran, and
/// clearing it here leaves the next waiting poll asleep with news waiting for
/// it.
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

/// The socket ops move bytes: one send on one end, one receive on the other.
#[cfg(unix)]
#[test]
fn a_socket_send_lands_in_the_receive_buffer() {
	use std::os::unix::net::UnixStream;

	let (left, right) = UnixStream::pair().unwrap();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let left: Facade<UnixStream> = Facade::new(left, &submitter);
	let right: Facade<UnixStream> = Facade::new(right, &submitter);
	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
		reactor
	});

	let sent: Arc<Vec<u8>> = Arc::new(b"over the socket".to_vec());
	let payload: Arc<dyn Memory> = sent.clone();
	assert_eq!(block_on(left.send(payload)).into_result().unwrap(), 15);

	let received: Arc<Vec<u8>> = Arc::new(vec![0; 15]);
	let payload: Arc<dyn Memory> = received.clone();
	assert_eq!(block_on(right.recv(payload)).into_result().unwrap(), 15);
	assert_eq!(&received[..], b"over the socket");

	submitter.stop();
	driven.join().unwrap();
}

/// A socket op waits for data even when the descriptor says not to block:
/// io_uring arms the readiness itself, so a device can keep a non-blocking
/// socket (a connect still needs one) and still transfer through the engine.
/// The completion is the bytes one call moved — four here, of the eight the
/// buffer has room for; a caller that wants the eight loops over
/// [`Facade::recv_exact`].
#[cfg(unix)]
#[test]
fn a_receive_waits_on_a_nonblocking_socket() {
	// The descriptor is the part under test: it must not block, and the ring
	// must still wait for it.
	let (left, right) = std::os::unix::net::UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let right: Facade<std::os::unix::net::UnixStream> = Facade::new(right, &submitter);
	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; 8]);
	let payload: Arc<dyn Memory> = buffer.clone();
	let mut receive = std::pin::pin!(right.recv(payload));

	// Nothing has been sent yet, so the op has to stay in flight.
	let mut cx = Context::from_waker(Waker::noop());
	assert!(
		receive.as_mut().poll(&mut cx).is_pending(),
		"the receive resolved without any data"
	);
	reactor.poll(Some(Duration::from_millis(50))).unwrap();
	assert!(!reactor.is_idle(), "the receive resolved without any data");

	{
		use std::io::Write;
		(&left).write_all(b"data").unwrap();
	}
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(10))).unwrap();
	}
	let Poll::Ready(outcome) = receive.as_mut().poll(&mut cx) else {
		panic!("the receive did not land");
	};
	assert_eq!(
		outcome.into_result().unwrap(),
		4,
		"the completion is one call's bytes"
	);
	assert_eq!(&buffer[..4], b"data");
}

/// A mode that cannot hand out a descriptor fails when the reactor is built,
/// not when someone first waits on a descriptor that will never become ready.
#[test]
fn fd_mode_with_iopoll_is_rejected_up_front() {
	let error = Reactor::new(Config {
		iopoll: true,
		wakeup: PollMode::Fd,
		..Config::default()
	})
	.err()
	.expect("fd mode with IOPOLL must be rejected");
	assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}

/// IOPOLL rings have nothing to wait on: a completion appears only while
/// polling, so `poll_fd` says so instead of handing out a quiet descriptor.
#[test]
#[cfg(target_os = "linux")]
fn iopoll_rings_have_no_descriptor_to_wait_on() {
	let (reactor, _) = Reactor::new(Config {
		iopoll: true,
		..Config::default()
	})
	.unwrap();
	let error = reactor.poll_fd().err().expect("IOPOLL has no poll_fd");
	assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}

/// SQPOLL hands submissions to a kernel thread; the completions come back the
/// same way, so the caller's side of the reactor does not care.
#[test]
#[cfg(target_os = "linux")]
fn sqpoll_rings_complete_too() {
	let file = seeded_file(b"hello");
	let (mut reactor, submitter) = match Reactor::new(Config {
		sqpoll: true,
		..Config::default()
	}) {
		Ok(pair) => pair,
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
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let completion = submitter.read(&file, 0, memory(&buf), whole(&buf)).unwrap();
	let n = run(&mut reactor, completion).unwrap().count().unwrap();
	assert_eq!(n, 5);
	assert_eq!(&buf[..], b"hello");
}

/// A fsync has no overlapped form, so an IOPOLL ring refuses it here instead
/// of letting the kernel answer EINVAL — and instead of silently dropping it.
#[test]
#[cfg(target_os = "linux")]
fn fsync_is_refused_on_an_iopoll_ring() {
	let file = seeded_file(b"data");
	let (mut reactor, submitter) = Reactor::new(Config {
		iopoll: true,
		..Config::default()
	})
	.unwrap();
	// The reactor is the backend's only caller, so the refusal lands on the
	// first poll rather than at submit time.
	let completion = submitter.fsync(&file).unwrap();
	reactor.poll(Some(Duration::from_millis(1))).unwrap();
	let error = match block_on(completion).1 {
		Ok(_) => panic!("fsync on an IOPOLL ring must be refused"),
		Err(error) => error,
	};
	assert_eq!(error.kind(), io::ErrorKind::Unsupported);
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

/// Dropping the reactor stops it: a submitter left over — every clone a caller
/// kept is one — is refused from then on, the same as after `Submitter::stop`,
/// so the caller has an error to act on instead of a completion nothing drives.
#[test]
fn a_submission_after_the_reactor_drops_is_refused() {
	let file = seeded_file(b"hello");
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	drop(reactor);
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let error = submitter
		.read(&file, 0, memory(&buf), whole(&buf))
		.err()
		.expect("a dropped reactor refuses submissions");
	assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

/// The reactor's own wake-up is a descriptor a caller can wait on
/// (`Submitter::wait`): a raise from another thread must complete that wait,
/// not only make `poll` return — otherwise the task waiting on it never runs
/// again.
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

thread_local! {
	/// The take a rendezvous wake re-polls; one test drives it at a time.
	static PENDING_TAKE: RefCell<Option<Pin<Box<dyn Future<Output = Option<usize>>>>>> =
		RefCell::new(None);
	static TAKE_OUTCOME: RefCell<Option<Option<usize>>> = RefCell::new(None);
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

impl Wake for Repoll {
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
