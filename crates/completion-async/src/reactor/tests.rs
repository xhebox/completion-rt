//! Reactor mechanics, driven the way the low layer exposes them: the poll
//! timeout, the readiness waits a facade cannot shape, the cancellation a
//! release sets off, and the backend's own I/O.
//!
//! These need a reactor the caller polls itself — a timeout, the `idle` state,
//! dropping an operation — none of which the facade, which owns its reactor
//! inside an `Executor`, exposes.

use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Relaxed;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use completion_backend::{AsDescriptor, Config, Error, ErrorKind, Extent, IoVec, Memory, Response};

#[cfg(target_os = "linux")]
use completion_backend::Status;

#[cfg(unix)]
use std::io::{Read, Write};

use crate::completion::{Completion, submit, submit_timer};
use crate::io::OpenOptions;
use crate::reactor::{Op, Reactor};

/// Bytes a test binds to a space. Atomic, so the backend may write them through
/// an iovec while the buffer is shared.
struct Buffer(Arc<[AtomicU8]>);

impl Buffer {
	fn new(len: usize) -> Buffer {
		Buffer::from_slice(&vec![0; len])
	}

	fn from_slice(bytes: &[u8]) -> Buffer {
		Buffer(bytes.iter().map(|&byte| AtomicU8::new(byte)).collect())
	}

	fn to_vec(&self) -> Vec<u8> {
		self.0.iter().map(|byte| byte.load(Relaxed)).collect()
	}
}

// SAFETY: the bytes are atomics, so a write through the pointer races on bytes
// only, never on a reference; `iovec` checks the extent against the length.
unsafe impl Memory for Buffer {
	fn len(&self) -> usize {
		self.0.len()
	}

	fn iovec(&self, extent: Extent) -> io::Result<IoVec> {
		let end = usize::try_from(extent.offset)
			.ok()
			.and_then(|offset| offset.checked_add(extent.len));
		if !end.is_some_and(|end| end <= self.0.len()) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"range outside the buffer",
			));
		}
		let base = self.0.as_ptr() as *mut u8;
		// SAFETY: in bounds, checked above.
		Ok(unsafe { IoVec::new(base.add(extent.offset as usize), extent.len) })
	}
}

/// A second handle on `buf`, in the shape an operation names memory.
fn memory(buf: &Arc<Buffer>) -> Arc<dyn Memory> {
	let memory: Arc<dyn Memory> = buf.clone();
	memory
}

/// The descriptor of `resource`, in the shape a submission names one.
fn fd<S: AsDescriptor + 'static>(resource: &Arc<S>) -> Arc<dyn AsDescriptor> {
	let fd: Arc<dyn AsDescriptor> = resource.clone();
	fd
}

/// A scratch directory below the crate's `target/test-tmp`, removed with the
/// returned handle. The path stays relative to the crate root, which is where
/// cargo runs a test binary: an absolute one carries the depth of the checkout
/// into every path a test hands the kernel, and a unix socket has to fit
/// `sockaddr_un::sun_path`.
fn tempdir(tag: &str) -> Scratch {
	let base = std::env::var_os("COMPLETION_RT_TEST_TMP")
		.map(std::path::PathBuf::from)
		.unwrap_or_else(|| std::path::PathBuf::from("target/test-tmp"));
	std::fs::create_dir_all(&base).unwrap();
	let dir = tempfile::Builder::default()
		.prefix(tag)
		.tempdir_in(&base)
		.unwrap();
	let name = dir.path().file_name().unwrap();
	Scratch {
		path: base.join(name),
		_dir: dir,
	}
}

struct Scratch {
	// Owns the directory: dropping the scratch removes it.
	_dir: tempfile::TempDir,
	path: std::path::PathBuf,
}

impl Scratch {
	fn path(&self) -> &std::path::Path {
		&self.path
	}
}

/// Drives the reactor until the completion lands, then takes its result.
fn drive_to_result(reactor: &Reactor, mut completion: Completion) -> Result<Response, Error> {
	let mut cx = Context::from_waker(Waker::noop());
	loop {
		match completion.poll(&mut cx) {
			Poll::Ready(Some(result)) => return result,
			Poll::Ready(None) => panic!("a completion without a cancel reported none"),
			Poll::Pending => reactor.drive(None).unwrap(),
		}
	}
}

#[cfg(unix)]
fn drive_until_idle(reactor: &Reactor) {
	while !reactor.idle() {
		reactor.drive(Some(Duration::from_millis(1))).unwrap();
	}
}

/// A file with `contents`, in a scratch directory that goes with its
/// descriptor.
fn seeded_file(contents: &[u8]) -> Arc<std::fs::File> {
	let dir = tempdir("completion-seed");
	let path = dir.path().join("data");
	std::fs::write(&path, contents).unwrap();
	// The returned descriptor outlives the deleted directory entry.
	Arc::new(OpenOptions::new().read(true).open(&path).unwrap())
}

/// A connected pair, both ends non-blocking: a receive on one waits for a send
/// on the other.
#[cfg(unix)]
fn socket_pair() -> (
	std::os::unix::net::UnixStream,
	Arc<std::os::unix::net::UnixStream>,
) {
	use std::os::unix::net::UnixStream;

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	(left, Arc::new(right))
}

/// A read that nothing will satisfy: the peer never sends.
#[cfg(unix)]
fn stalled_recv(
	reactor: &Reactor,
	right: &Arc<std::os::unix::net::UnixStream>,
	buf: &Arc<Buffer>,
) -> Completion {
	submit(
		reactor,
		fd(right),
		Op::Read {
			offset: 0,
			span: completion_backend::Space::new(memory(buf)).whole(),
		},
		None,
	)
	.unwrap()
}

/// A file operation lands on a worker's clock, so it can be finished before
/// the reactor is polled at all. The completion is then in hand, and the poll
/// hands it back: sleeping on news already in hand is a stall the timeout, not
/// the work, ends.
#[test]
fn a_landed_file_operation_does_not_wait_out_the_timeout() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let file = seeded_file(b"hello");
	let buf: Arc<Buffer> = Arc::new(Buffer::new(5));
	let completion = submit(
		&reactor,
		fd(&file),
		Op::Read {
			offset: 0,
			span: completion_backend::Space::new(memory(&buf)).whole(),
		},
		None,
	)
	.unwrap();
	// Let the pool land it before the reactor is polled once.
	std::thread::sleep(Duration::from_millis(50));

	let started = Instant::now();
	while !reactor.idle() {
		reactor.drive(Some(Duration::from_secs(30))).unwrap();
	}
	assert_eq!(
		drive_to_result(&reactor, completion)
			.unwrap()
			.count()
			.unwrap(),
		5
	);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the poll slept on a completion it already had"
	);
}

/// Several timers wait at once: each is armed on the reactor, so the nearest one
/// is what the poller sleeps to and a later one stays pending behind it.
#[test]
fn timers_wait_out_their_own_deadlines() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let started = Instant::now();
	let mut near =
		submit_timer(&reactor, Instant::now() + Duration::from_millis(20), None).unwrap();
	let mut far = submit_timer(&reactor, Instant::now() + Duration::from_secs(2), None).unwrap();
	let mut cx = Context::from_waker(Waker::noop());

	while near.poll(&mut cx).is_pending() {
		reactor.drive(Some(Duration::from_millis(1))).unwrap();
	}
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the poller slept for the far timer's deadline"
	);
	assert!(
		far.poll(&mut cx).is_pending(),
		"the far timer came due with the near one"
	);
	far.cancel();
}

/// A wait is capped at the nearest deadline: a poll asked for far longer than
/// the timer still returns when the timer comes due, not when its own timeout
/// passes.
#[test]
fn a_poll_does_not_sleep_past_the_nearest_deadline() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut timer =
		submit_timer(&reactor, Instant::now() + Duration::from_millis(50), None).unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	let started = Instant::now();
	let result = loop {
		match timer.poll(&mut cx) {
			Poll::Ready(Some(result)) => break result,
			Poll::Ready(None) => panic!("a timer without a cancel reported none"),
			// Far longer than the timer: only the timer set cuts the wait short.
			Poll::Pending => reactor.drive(Some(Duration::from_secs(5))).unwrap(),
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
	let mut timer =
		submit_timer(&reactor, Instant::now() + Duration::from_millis(20), None).unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	reactor.drive(Some(Duration::ZERO)).unwrap();
	assert!(
		timer.poll(&mut cx).is_pending(),
		"the timer waits on its deadline"
	);
	timer.cancel();
	assert!(
		matches!(timer.poll(&mut cx), Poll::Ready(Some(Err(_)))),
		"the cancelled timer still reported a fire"
	);
	// The deadline passes with nothing to fire.
	std::thread::sleep(Duration::from_millis(50));
	reactor.drive(Some(Duration::ZERO)).unwrap();
	// A stale deadline would cut this wait to zero.
	let started = Instant::now();
	reactor.drive(Some(Duration::from_millis(20))).unwrap();
	assert!(
		started.elapsed() >= Duration::from_millis(20),
		"the cancelled timer's deadline still shortened a wait"
	);
}

/// A cancelled timer's poll reports the cancel itself, not a failed call.
#[test]
fn a_cancelled_timer_reports_a_cancel() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut timer = submit_timer(&reactor, Instant::now() + Duration::from_secs(60), None).unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	timer.cancel();
	match timer.poll(&mut cx) {
		Poll::Ready(Some(Err(error))) => {
			assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
		}
		other => panic!("the cancelled timer reported {other:?}"),
	}
}

/// A reactor that has shut down refuses a new timer as stopped, not as a failed call.
#[test]
fn a_stopped_reactor_refuses_a_timer_as_stopped() {
	let reactor = Reactor::new(Config::default()).unwrap();
	reactor.shutdown(false).unwrap();
	let error = submit_timer(&reactor, Instant::now() + Duration::from_secs(60), None)
		.err()
		.expect("a reactor that has shut down refuses timers");
	assert_eq!(error.kind(), ErrorKind::Stopped, "{error}");
}

/// A timer cancelled before a shutdown keeps reading as a cancel, even though
/// the shutdown stops the timers still armed.
#[test]
fn a_cancelled_timer_before_shutdown_still_reads_as_a_cancel() {
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut timer = submit_timer(&reactor, Instant::now() + Duration::from_secs(60), None).unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	timer.cancel();
	reactor.shutdown(true).unwrap();
	match timer.poll(&mut cx) {
		Poll::Ready(Some(Err(error))) => {
			assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
		}
		other => panic!("the cancelled timer reported {other:?}"),
	}
}

/// A shutdown lands the operations still in flight, and the reactor takes no
/// more work afterwards.
#[test]
fn a_shutdown_lands_what_is_in_flight_and_refuses_new_work() {
	let file = seeded_file(b"hello");
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf: Arc<Buffer> = Arc::new(Buffer::new(5));
	let completion = submit(
		&reactor,
		fd(&file),
		Op::Read {
			offset: 0,
			span: completion_backend::Space::new(memory(&buf)).whole(),
		},
		None,
	)
	.unwrap();
	// Shut down with the read still only submitted: the shutdown drives it to
	// its end.
	reactor.shutdown(false).unwrap();
	assert_eq!(
		drive_to_result(&reactor, completion)
			.unwrap()
			.count()
			.unwrap(),
		5
	);
	assert_eq!(buf.to_vec(), b"hello");
	assert!(reactor.idle());

	// A reactor that has shut down takes no more work.
	let buf: Arc<Buffer> = Arc::new(Buffer::new(5));
	let error = submit(
		&reactor,
		fd(&file),
		Op::Read {
			offset: 0,
			span: completion_backend::Space::new(memory(&buf)).whole(),
		},
		None,
	)
	.err()
	.expect("a reactor that has shut down refuses submissions");
	assert_eq!(error.kind(), ErrorKind::Stopped);
}

/// Dropping an in-flight read neither blocks nor panics: the release cancels
/// the receive and detaches it, and the reactor keeps the buffer alive until
/// the kernel reports the cancellation. Once reaped, the buffer is the
/// caller's again, and a byte the peer sends later reaches no operation.
#[test]
#[cfg(unix)]
fn a_dropped_in_flight_read_is_cancelled_and_reaped() {
	let (mut peer, right) = socket_pair();
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(Buffer::new(8));
	let completion = stalled_recv(&reactor, &right, &buf);
	reactor.drive(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.idle(), "the receive has to be in flight");
	let started = Instant::now();
	drop(completion);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the drop waited for the kernel"
	);
	assert_eq!(
		Arc::strong_count(&buf),
		2,
		"the detached receive holds the buffer until its cancellation lands"
	);
	drive_until_idle(&reactor);
	assert_eq!(
		Arc::strong_count(&buf),
		1,
		"the reactor released the buffer once the cancellation landed"
	);
	peer.write_all(b"later").unwrap();
	reactor.drive(Some(Duration::from_millis(1))).unwrap();
	assert_eq!(
		buf.to_vec(),
		&[0u8; 8],
		"nothing was left to write the buffer after the drop"
	);
}

/// A readiness wait is memory-free too, and dropping one does not block.
#[test]
#[cfg(unix)]
fn a_dropped_readiness_wait_does_not_block() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut wait = submit(&reactor, Arc::new(reader), Op::Readable, None).unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	assert!(wait.poll(&mut cx).is_pending(), "the pipe has no data");
	// The first pass hands the wait to the backend.
	reactor.drive(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.idle(), "the wait is in flight");
	let started = Instant::now();
	drop(wait);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"a readiness wait must not block"
	);
	drive_until_idle(&reactor);
}

/// Shutting the reactor down with an operation still out drives it to its end, and a
/// completion dropped afterwards finds its entry gone and returns at once.
#[test]
#[cfg(unix)]
fn a_reactor_drop_settles_what_is_in_flight() {
	let (_left, right) = socket_pair();
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(Buffer::new(8));
	let completion = stalled_recv(&reactor, &right, &buf);
	reactor.drive(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.idle(), "the receive is in flight");
	reactor.shutdown(true).unwrap();
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
	let (mut left, right) = socket_pair();
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(Buffer::new(8));
	let mut completion = stalled_recv(&reactor, &right, &buf);
	reactor.drive(Some(Duration::ZERO)).unwrap();
	let peer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		left.write_all(b"late").unwrap();
		left
	});
	reactor.shutdown(false).unwrap();
	let _left = peer.join().unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	let result = match completion.poll(&mut cx) {
		Poll::Ready(Some(result)) => result,
		Poll::Pending => panic!("the shutdown returned with the receive still out"),
		Poll::Ready(None) => panic!("the receive reported a cancel"),
	};
	assert_eq!(result.unwrap().count().unwrap(), 4);
	assert_eq!(&buf.to_vec()[..4], b"late");
}

/// A shutdown ends the operations still out, but a completion that outlives it
/// still resolves: to the stopped error a submission after the shutdown gets.
#[test]
#[cfg(unix)]
fn a_completion_polled_after_shutdown_resolves_to_stopped() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut wait = submit(&reactor, Arc::new(reader), Op::Readable, None).unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	assert!(wait.poll(&mut cx).is_pending(), "the pipe has no data");
	reactor.drive(Some(Duration::ZERO)).unwrap();
	reactor.shutdown(true).unwrap();
	let result = match wait.poll(&mut cx) {
		Poll::Ready(Some(result)) => result,
		Poll::Pending => panic!("a completion after shutdown stayed pending"),
		Poll::Ready(None) => panic!("the wait reported a cancel"),
	};
	assert_eq!(result.unwrap_err().kind(), ErrorKind::Stopped);
}

/// A cancel its holder asked for before a shutdown keeps reading as a cancel,
/// even when the operation never reached the backend.
#[test]
#[cfg(unix)]
fn a_cancel_before_shutdown_still_reads_as_a_cancel() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let reactor = Reactor::new(Config::default()).unwrap();
	let mut wait = submit(&reactor, Arc::new(reader), Op::Readable, None).unwrap();
	wait.cancel();
	reactor.shutdown(true).unwrap();
	let mut cx = Context::from_waker(Waker::noop());
	let result = match wait.poll(&mut cx) {
		Poll::Ready(Some(result)) => result,
		Poll::Pending => panic!("a completion after shutdown stayed pending"),
		Poll::Ready(None) => panic!("the wait reported a cancel"),
	};
	let error = result.unwrap_err();
	assert_eq!(
		error.kind(),
		ErrorKind::Cancelled,
		"the cancel became {error}"
	);
}

/// A byte written to the peer after the cancel never reaches the buffer.
#[test]
#[cfg(unix)]
fn a_cancelled_receive_keeps_the_late_bytes_out() {
	use std::os::unix::net::UnixStream;

	let (left, right) = UnixStream::pair().unwrap();
	left.set_nonblocking(true).unwrap();
	right.set_nonblocking(true).unwrap();
	let right = Arc::new(right);
	let reactor = Reactor::new(Config::default()).unwrap();
	let buf = Arc::new(Buffer::new(8));
	let completion = stalled_recv(&reactor, &right, &buf);
	// One pass puts the receive in flight, so the cancel reaches the kernel's op.
	reactor.drive(Some(Duration::ZERO)).unwrap();
	assert!(!reactor.idle(), "the receive has to be in flight");
	completion.cancel();
	let error =
		drive_to_result(&reactor, completion).expect_err("the receive completed after the cancel");
	assert!(
		error.kind() == ErrorKind::Cancelled,
		"the receive was not cancelled: {error:?}"
	);
	(&left).write_all(b"late").unwrap();
	reactor.drive(Some(Duration::from_millis(20))).unwrap();
	assert_eq!(buf.to_vec(), &[0u8; 8], "a byte arrived after the cancel");
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
	let mut reader = Arc::new(reader);
	let mut cx = Context::from_waker(Waker::noop());
	{
		let mut wait = submit(&reactor, fd(&reader), Op::Readable, None).unwrap();
		assert!(wait.poll(&mut cx).is_pending(), "the pipe has no data");
		reactor.drive(Some(Duration::ZERO)).unwrap();
		assert!(!reactor.idle(), "the wait is in flight");
	}
	// The write end stays open, so nothing will ever make the read end ready:
	// only cancelling and reaping the abandoned wait can release the pipe.
	let mut taken = false;
	for _ in 0..8 {
		reactor.drive(Some(Duration::from_millis(1))).unwrap();
		reader = match Arc::try_unwrap(reader) {
			Ok(_resource) => {
				taken = true;
				break;
			}
			Err(reader) => reader,
		};
	}
	assert!(taken, "the abandoned wait still held the pipe");
}

#[cfg(target_os = "linux")]
mod uring {
	use super::*;

	/// An operation that wins the race keeps its result: the cancel is a request,
	/// and a completion that already carries bytes is not thrown away.
	#[test]
	fn an_op_that_wins_the_race_keeps_its_bytes() {
		let (mut left, right) = socket_pair();
		let reactor = Reactor::new(Config::default()).unwrap();
		let buf = Arc::new(Buffer::new(4));
		let mut completion = stalled_recv(&reactor, &right, &buf);
		// The receive goes to the kernel with nothing to move, so it is in flight
		// and unsettled when the completion is next looked at.
		reactor.drive(Some(Duration::ZERO)).unwrap();
		assert_eq!(Arc::strong_count(&buf), 2, "the kernel holds the buffer");
		// The bytes are queued, and the kernel finishes the receive on its own.
		left.write_all(b"wins").unwrap();
		// The cancel fires before the outcome is looked at; the receive gets there
		// first.
		completion.cancel();
		let mut cx = Context::from_waker(Waker::noop());
		assert!(
			completion.poll(&mut cx).is_pending(),
			"the receive is in the kernel, not yet reported"
		);
		let mut ready = None;
		for _ in 0..500 {
			if let Poll::Ready(Some(value)) = completion.poll(&mut cx) {
				ready = Some(value);
				break;
			}
			reactor.drive(Some(Duration::from_millis(10))).unwrap();
		}
		let response = ready
			.expect("the receive landed")
			.expect("the bytes are the operation's own result, cancel or not");
		assert_eq!(response.count().unwrap(), 4);
		assert_eq!(buf.to_vec(), b"wins", "the bytes the receive moved");
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
		let (mut left, right) = socket_pair();
		let reactor = Reactor::new(Config::default()).unwrap();
		let buf = Arc::new(Buffer::new(4));
		let mut completion = stalled_recv(&reactor, &right, &buf);
		// The receive is armed with nothing to move, so it is in flight and
		// unsettled when the completion is next looked at.
		reactor.drive(Some(Duration::ZERO)).unwrap();
		assert_eq!(Arc::strong_count(&buf), 2, "the operation holds the buffer");
		// The bytes are queued, and no pass has run the receive over them.
		left.write_all(b"wins").unwrap();
		// The cancel fires before the outcome is looked at; the receive never
		// gets there.
		completion.cancel();
		let mut cx = Context::from_waker(Waker::noop());
		let mut ready = None;
		for _ in 0..500 {
			if let Poll::Ready(Some(value)) = completion.poll(&mut cx) {
				ready = Some(value);
				break;
			}
			reactor.drive(Some(Duration::from_millis(10))).unwrap();
		}
		let error = ready
			.expect("the receive was stopped")
			.expect_err("the receive was stopped");
		assert!(
			error.kind() == ErrorKind::Cancelled,
			"a readiness backend stops the receive it was asked to"
		);
		assert_eq!(
			buf.to_vec(),
			&[0u8; 4],
			"the stopped receive wrote the buffer"
		);
		// Nothing is lost: the bytes wait in the peer's buffer for the next one.
		let mut reader: &std::os::unix::net::UnixStream = &right;
		let mut got = [0u8; 4];
		reader.read_exact(&mut got).unwrap();
		assert_eq!(&got, b"wins", "the bytes stayed with the peer");
	}
}

#[cfg(unix)]
mod sockets {
	use super::*;

	/// A listener to connect to inside `directory`, and the path its socket lives
	/// at.
	fn listener(
		directory: &std::path::Path,
	) -> (std::os::unix::net::UnixListener, std::path::PathBuf) {
		let path = directory.join("sock");
		assert!(
			path.as_os_str().len() < 104,
			"the socket path does not fit sun_path: {}",
			path.display()
		);
		let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
		listener.set_nonblocking(true).unwrap();
		(listener, path)
	}

	/// Drives the reactor until the accept's response is in the entry that holds
	/// it.
	fn drive_until_settled(reactor: &Reactor) {
		while !reactor.idle() {
			reactor.drive(Some(Duration::from_millis(1))).unwrap();
		}
	}

	/// The peer's read once the accepted end is gone: zero bytes is the
	/// connection ending, and a leaked descriptor would leave it waiting.
	fn assert_the_connection_ended(client: &mut std::os::unix::net::UnixStream) {
		client.set_nonblocking(true).unwrap();
		let deadline = Instant::now() + Duration::from_secs(5);
		let mut byte = [0u8; 1];
		loop {
			match client.read(&mut byte) {
				Ok(0) => return,
				Ok(read) => panic!("the peer read {read} bytes, not the connection ending"),
				Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
					assert!(
						Instant::now() < deadline,
						"the peer never saw the connection end"
					);
					std::thread::sleep(Duration::from_millis(1));
				}
				Err(error) => panic!("the peer's read failed: {error}"),
			}
		}
	}

	/// Drives the reactor by hand until the peer sees the connection end. An entry
	/// whose result was given up on no longer counts as outstanding, so nothing
	/// else reaps for it, and a socket left open keeps the peer waiting.
	#[cfg(target_os = "linux")]
	fn drive_until_the_connection_ends(
		reactor: &Reactor,
		client: &mut std::os::unix::net::UnixStream,
	) {
		client.set_nonblocking(true).unwrap();
		let mut byte = [0u8; 1];
		for _ in 0..500 {
			reactor.drive(Some(Duration::from_millis(1))).unwrap();
			match client.read(&mut byte) {
				Ok(0) => return,
				Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
				other => panic!("the peer read {other:?}"),
			}
		}
		panic!("the accepted socket was never closed");
	}

	/// An accept whose landed result is never taken closes the socket the kernel
	/// created: the peer sees the connection end.
	#[test]
	fn a_dropped_accept_closes_the_new_socket() {
		use std::os::unix::net::UnixStream;

		let directory = tempdir("completion-accept-drop");
		let (listener, path) = listener(directory.path());
		let reactor = Reactor::new(Config::default()).unwrap();
		let listener = Arc::new(listener);
		let mut client;
		{
			let mut accept = submit(&reactor, fd(&listener), Op::Accept, None).unwrap();
			let mut cx = Context::from_waker(Waker::noop());
			// Hand the accept to the kernel.
			assert!(accept.poll(&mut cx).is_pending());
			client = UnixStream::connect(&path).unwrap();
			drive_until_settled(&reactor);
			// The socket exists and the response holds it, so this is not a
			// request saying nobody wanted it yet.
		}

		assert_the_connection_ended(&mut client);
	}

	/// An accept the kernel has already taken, whose result the reactor has not
	/// reaped, is dropped with nobody waiting: the socket it carries goes with
	/// the entry, and the peer sees the connection end.
	///
	/// io_uring only: a readiness backend calls accept only when the listener is
	/// ready, so one given up on before that never accepts, and the connection
	/// stays in the backlog with no socket to close.
	#[test]
	#[cfg(target_os = "linux")]
	fn an_abandoned_accept_that_succeeds_closes_its_socket() {
		use std::os::unix::net::UnixStream;

		let directory = tempdir("completion-accept-abandoned");
		let (listener, path) = listener(directory.path());
		let reactor = Reactor::new(Config::default()).unwrap();
		let listener = Arc::new(listener);
		let mut client;
		{
			let mut accept = submit(&reactor, fd(&listener), Op::Accept, None).unwrap();
			let mut cx = Context::from_waker(Waker::noop());
			// Hand the accept to the kernel with nothing to accept yet.
			assert!(accept.poll(&mut cx).is_pending());
			reactor.drive(Some(Duration::ZERO)).unwrap();
			// The kernel takes the connection before the reactor reaps the result,
			// so the cancel the drop asks for reaches the ring too late to stop it.
			client = UnixStream::connect(&path).unwrap();
			std::thread::sleep(Duration::from_millis(50));
		}

		drive_until_the_connection_ends(&reactor, &mut client);
	}

	/// An until future dropped after its cancel fired, whose accept raced to
	/// success anyway, leaves no socket behind: the caller never takes the result,
	/// and the peer still sees the connection end.
	///
	/// io_uring only: a readiness backend calls accept only when the listener is
	/// ready, so one given up on before that never accepts, and the connection
	/// stays in the backlog with no socket to close.
	#[test]
	#[cfg(target_os = "linux")]
	fn a_cancelled_accept_that_succeeded_closes_its_socket() {
		use std::os::unix::net::UnixStream;

		let directory = tempdir("completion-accept-raced");
		let (listener, path) = listener(directory.path());
		let reactor = Reactor::new(Config::default()).unwrap();
		let listener = Arc::new(listener);

		let mut client;
		{
			let mut accept = submit(&reactor, fd(&listener), Op::Accept, None).unwrap();
			let mut cx = Context::from_waker(Waker::noop());
			// Hand the accept to the kernel with nothing to accept yet.
			assert!(accept.poll(&mut cx).is_pending());
			reactor.drive(Some(Duration::ZERO)).unwrap();

			client = UnixStream::connect(&path).unwrap();
			// The kernel reports the connection without the reactor being driven,
			// so the accept is done before the cancel fires.
			std::thread::sleep(Duration::from_millis(50));
			accept.cancel();
			assert!(
				accept.poll(&mut cx).is_pending(),
				"the caller asked for the accept to stop"
			);
			// The ask reaches the backend, which the accept has already outrun.
			reactor.drive(Some(Duration::ZERO)).unwrap();
			// The caller goes without the result, and the socket it never took
			// goes with the future.
		}

		drive_until_the_connection_ends(&reactor, &mut client);
	}
}

/// A hangup wait resolves once the peer is gone, not when it only half-closes:
/// a stream whose peer shut its write side is still open the other way.
#[cfg(target_os = "linux")]
mod hangup {
	use super::*;
	use std::net::Shutdown;
	use std::os::unix::net::UnixStream;

	fn pair() -> (UnixStream, Arc<UnixStream>) {
		socket_pair()
	}

	#[test]
	fn a_half_close_is_not_a_hangup_but_the_peer_going_is() {
		let (left, right) = pair();
		let reactor = Reactor::new(Config::default()).unwrap();
		let mut hangup = submit(&reactor, fd(&right), Op::Hangup, None).unwrap();
		let mut cx = Context::from_waker(Waker::noop());
		assert!(hangup.poll(&mut cx).is_pending());

		left.shutdown(Shutdown::Write).unwrap();
		for _ in 0..10 {
			reactor.drive(Some(Duration::from_millis(10))).unwrap();
		}
		assert!(
			hangup.poll(&mut cx).is_pending(),
			"a peer that only shut its write side is still there"
		);

		drop(left);
		let result = drive_to_result(&reactor, hangup);
		assert!(matches!(result.unwrap(), Response::Done));

		// The poll the hangup rode is withdrawn once reported; its last completion
		// lands on nothing.
		for _ in 0..5 {
			reactor.drive(Some(Duration::from_millis(5))).unwrap();
		}
		assert!(reactor.idle(), "the reported hangup left an entry");
	}

	#[test]
	fn a_peer_already_gone_is_a_hangup_at_once() {
		let (left, right) = pair();
		drop(left);
		let reactor = Reactor::new(Config::default()).unwrap();
		let hangup = submit(&reactor, fd(&right), Op::Hangup, None).unwrap();
		let result = drive_to_result(&reactor, hangup);
		assert!(matches!(result.unwrap(), Response::Done));
	}

	/// TCP reads a peer's close as a FIN, the same half-close a `shutdown(Write)`
	/// sends: the connection hangs up once this side shuts its own write side too.
	#[test]
	fn a_tcp_peer_close_hangs_up_once_this_side_shuts_too() {
		use std::net::{TcpListener, TcpStream};

		let listener = TcpListener::bind("127.0.0.1:0").unwrap();
		let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
		let (stream, _) = listener.accept().unwrap();
		stream.set_nonblocking(true).unwrap();
		let stream = Arc::new(stream);
		let reactor = Reactor::new(Config::default()).unwrap();
		let mut hangup = submit(&reactor, fd(&stream), Op::Hangup, None).unwrap();
		let mut cx = Context::from_waker(Waker::noop());
		assert!(hangup.poll(&mut cx).is_pending());

		drop(peer);
		for _ in 0..10 {
			reactor.drive(Some(Duration::from_millis(10))).unwrap();
		}
		assert!(
			hangup.poll(&mut cx).is_pending(),
			"a FIN alone is a half-close"
		);

		stream.shutdown(Shutdown::Write).unwrap();
		let result = drive_to_result(&reactor, hangup);
		assert!(matches!(result.unwrap(), Response::Done));
	}

	#[test]
	fn a_hangup_cancelled_before_the_peer_goes_reports_a_cancel() {
		let (_left, right) = pair();
		let reactor = Reactor::new(Config::default()).unwrap();
		let mut hangup = submit(&reactor, fd(&right), Op::Hangup, None).unwrap();
		let mut cx = Context::from_waker(Waker::noop());
		assert!(hangup.poll(&mut cx).is_pending());
		reactor.drive(Some(Duration::ZERO)).unwrap();

		hangup.cancel();
		let result = drive_to_result(&reactor, hangup);
		assert_eq!(result.unwrap_err().kind(), ErrorKind::Cancelled);
	}

	/// A multishot poll the kernel cannot report to — its completion queue full —
	/// is ended rather than left armed. The wait is still owed, so what lands is a
	/// temporary failure the caller can re-issue, not a hangup and not a wait that
	/// never ends.
	#[test]
	fn a_poll_the_kernel_ends_early_is_a_temporary_failure() {
		// A ring whose completion queue holds four: a burst overflows it, which is
		// what makes the kernel end the poll rather than queue its report.
		let reactor = Reactor::new(Config {
			entries: 2,
			..Config::default()
		})
		.unwrap();
		let (left, right) = pair();
		let mut hangup = submit(&reactor, fd(&right), Op::Hangup, None).unwrap();
		let mut cx = Context::from_waker(Waker::noop());
		assert!(hangup.poll(&mut cx).is_pending());

		// The burst: reads that land at once, none of them reaped yet, and each
		// held so its operation stays with the ring.
		let scratch = tempdir("hangup-terminated");
		let buf: Arc<Buffer> = Arc::new(Buffer::new(24));
		let space = completion_backend::Space::new(memory(&buf));
		let mut reads = Vec::new();
		for index in 0..24u64 {
			let path = scratch.path().join(format!("data{index}"));
			std::fs::write(&path, b"hello").unwrap();
			let file = Arc::new(OpenOptions::new().read(true).open(&path).unwrap());
			let span = space.span(index, 1);
			reads.push(submit(&reactor, fd(&file), Op::Read { offset: 0, span }, None).unwrap());
		}
		// Let every read land, and leave them in the queue: the poll's report has
		// nowhere to go, so the kernel ends the multishot instead.
		std::thread::sleep(Duration::from_millis(50));
		left.shutdown(Shutdown::Write).unwrap();

		let deadline = Instant::now() + Duration::from_secs(5);
		let result = loop {
			reactor.drive(Some(Duration::from_millis(20))).unwrap();
			if let Poll::Ready(Some(result)) = hangup.poll(&mut cx) {
				break result;
			}
			assert!(
				Instant::now() < deadline,
				"the wait outlived a poll the kernel ended"
			);
		};
		let error = result.expect_err("an ended poll is not a hangup");
		assert_eq!(
			error.status(),
			Status::Temporary,
			"the wait is still owed, so it can be issued again: {error}"
		);

		// The burst has to land before this ends: dropping an operation that still
		// names caller memory is a bug, and a completion the kernel deferred
		// arrives as room for it appears.
		drive_until_idle(&reactor);
		// The reads land while their completions are held, so the drops below find
		// their entries gone.
		drop(reads);
	}

	/// The kernel's last word on a hangup that already reported — the cancel its
	/// report asked for, arriving — names the operation that is gone. The slot
	/// that operation lived in may already hold another wait, and that wait's id is
	/// the same slot under a later generation: the late completion must find
	/// nothing rather than land on the operation that took the slot.
	#[test]
	fn a_late_completion_does_not_land_on_the_operation_that_took_the_slot() {
		let (left, right) = pair();
		let reactor = Reactor::new(Config::default()).unwrap();
		let mut hangup = submit(&reactor, fd(&right), Op::Hangup, None).unwrap();
		let mut cx = Context::from_waker(Waker::noop());
		assert!(hangup.poll(&mut cx).is_pending());

		drop(left);
		let result = drive_to_result(&reactor, hangup);
		assert!(matches!(result.unwrap(), Response::Done));

		// The slot the reported hangup lived in is free again, and the next wait is
		// put in it: the table hands a freed slot straight back out.
		let (peer, other) = pair();
		let mut wait = submit(&reactor, fd(&other), Op::Hangup, None).unwrap();
		assert!(wait.poll(&mut cx).is_pending());
		for _ in 0..5 {
			reactor.drive(Some(Duration::from_millis(5))).unwrap();
		}
		assert!(
			wait.poll(&mut cx).is_pending(),
			"the completion of the operation that had the slot landed on this one"
		);

		// This wait still answers to its own peer.
		drop(peer);
		let result = drive_to_result(&reactor, wait);
		assert!(matches!(result.unwrap(), Response::Done));
	}
}
