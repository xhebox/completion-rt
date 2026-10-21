//! Reactor mechanics, driven the way the low layer exposes them: the poll
//! timeout, the readiness waits a handle cannot shape, the cancellation a
//! release sets off, and the backend's own I/O.
//!
//! These need a reactor the caller polls itself — a timeout, the `idle` state,
//! dropping an operation — none of which a handle, which owns its reactor
//! inside an `Executor`, exposes. A test enters its reactor the way an executor
//! does, so the completions it makes submit there.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use completion_backend::{AsDescriptor, Buffer, Config, ErrorKind, Op, Response, Result};

#[cfg(unix)]
use std::io::{Read as _, Write};

use crate::io::OpenOptions;
use crate::reactor::{Driver, REACTOR, enter, submit};
use crate::timeout;

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

/// Polls a submission or timer by hand, the way an executor polls the operation over it.
fn poll<F: Future + Unpin>(future: &mut F, cx: &mut Context<'_>) -> Poll<F::Output> {
	Pin::new(future).poll(cx)
}

/// A driver entered on the test's thread, driven through the thread-local slot
/// an executor would leave it in.
struct Entered;

fn entered(config: Config) -> Entered {
	enter(Some(Driver::new(config).unwrap()), |_| ());
	Entered
}

impl Entered {
	fn drive(&self, timeout: Option<Duration>) -> Result<()> {
		REACTOR.with(|slot| slot.borrow_mut().as_mut().unwrap().drive(timeout))
	}

	fn shutdown(&self, force: bool) -> Result<()> {
		REACTOR.with(|slot| slot.borrow_mut().as_mut().unwrap().teardown(force))
	}

	fn idle(&self) -> bool {
		REACTOR.with(|slot| slot.borrow().as_ref().unwrap().is_idle())
	}
}

impl Drop for Entered {
	fn drop(&mut self) {
		drop(REACTOR.with(|slot| slot.borrow_mut().take()));
	}
}

/// Drives the reactor until the submission lands, then takes its result.
fn drive_to_result<F: Future + Unpin>(reactor: &Entered, mut future: F) -> F::Output {
	let mut cx = Context::from_waker(Waker::noop());
	loop {
		match poll(&mut future, &mut cx) {
			Poll::Ready(result) => return result,
			Poll::Pending => reactor.drive(None).unwrap(),
		}
	}
}

#[cfg(unix)]
fn drive_until_idle(reactor: &Entered) {
	while !reactor.idle() {
		reactor.drive(Some(Duration::from_millis(1))).unwrap();
	}
}

/// A submission polled by hand, the way an executor polls the operation over it.
///
/// `Pin<Box<..>>` is `Unpin`, so a test can keep one across polls and still hand
/// it to [`drive_to_result`].
type Submission = Pin<Box<dyn Future<Output = Result<Response>>>>;

fn submission(fd: Arc<dyn AsDescriptor>, op: Op) -> Submission {
	Box::pin(submit(fd, op))
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
fn stalled_recv(right: &Arc<std::os::unix::net::UnixStream>, buf: &Buffer) -> Submission {
	submission(
		fd(right),
		Op::Read {
			offset: None,
			span: buf.whole(),
		},
	)
}

/// A file operation lands on a worker's clock, so it can be finished before
/// the reactor is polled at all. The completion is then in hand, and the poll
/// hands it back: sleeping on news already in hand is a stall the timeout, not
/// the work, ends.
#[test]
fn a_landed_file_operation_does_not_wait_out_the_timeout() {
	let reactor = entered(Config::default());
	let file = seeded_file(b"hello");
	let buf: Buffer = Buffer::zeroed(5);
	let completion = submission(
		fd(&file),
		Op::Read {
			offset: Some(0),
			span: buf.whole(),
		},
	);
	// Let the pool land it before the reactor is polled once.
	std::thread::sleep(Duration::from_millis(50));

	let started = Instant::now();
	while !reactor.idle() {
		reactor.drive(Some(Duration::from_secs(30))).unwrap();
	}
	assert!(matches!(
		drive_to_result(&reactor, completion),
		Ok(Response::Count(5))
	));
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the poll slept on a completion it already had"
	);
}

/// Several timers wait at once: each is armed on the reactor, so the nearest one
/// is what the poller sleeps to and a later one stays pending behind it.
#[test]
fn timers_wait_out_their_own_deadlines() {
	let reactor = entered(Config::default());
	let started = Instant::now();
	let mut near = timeout(Duration::from_millis(20));
	let mut far = timeout(Duration::from_secs(2));
	let mut cx = Context::from_waker(Waker::noop());

	while poll(&mut near, &mut cx).is_pending() {
		reactor.drive(Some(Duration::from_millis(1))).unwrap();
	}
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the poller slept for the far timer's deadline"
	);
	assert!(
		poll(&mut far, &mut cx).is_pending(),
		"the far timer came due with the near one"
	);
}

/// A wait is capped at the nearest deadline: a poll asked for far longer than
/// the timer still returns when the timer comes due, not when its own timeout
/// passes.
#[test]
fn a_poll_does_not_sleep_past_the_nearest_deadline() {
	let reactor = entered(Config::default());
	let mut timer = timeout(Duration::from_millis(50));
	let mut cx = Context::from_waker(Waker::noop());
	let started = Instant::now();
	let result = loop {
		match poll(&mut timer, &mut cx) {
			Poll::Ready(result) => break result,
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

/// A shutdown lands the operations still in flight, and the reactor takes no
/// more work afterwards.
#[test]
fn a_shutdown_lands_what_is_in_flight_and_refuses_new_work() {
	let file = seeded_file(b"hello");
	let reactor = entered(Config::default());
	let buf: Buffer = Buffer::zeroed(5);
	let mut completion = submission(
		fd(&file),
		Op::Read {
			offset: Some(0),
			span: buf.whole(),
		},
	);
	// One poll submits the read; the shutdown then drives it to its end.
	let mut cx = Context::from_waker(Waker::noop());
	assert!(poll(&mut completion, &mut cx).is_pending());
	reactor.shutdown(false).unwrap();
	assert!(matches!(
		drive_to_result(&reactor, completion),
		Ok(Response::Count(5))
	));
	assert_eq!(buf.whole().to_vec(), b"hello");
	assert!(reactor.idle());

	// A reactor that has shut down takes no more work.
	let buf: Buffer = Buffer::zeroed(5);
	let mut completion = submission(
		fd(&file),
		Op::Read {
			offset: Some(0),
			span: buf.whole(),
		},
	);
	let error = match poll(&mut completion, &mut cx) {
		Poll::Ready(Err(error)) => error,
		other => panic!("a reactor that has shut down refused the read as {other:?}"),
	};
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
	let reactor = entered(Config::default());
	let buf = Buffer::zeroed(8);
	let mut completion = stalled_recv(&right, &buf);
	// The poll is what submits the receive; nothing else does.
	let mut cx = Context::from_waker(Waker::noop());
	assert!(poll(&mut completion, &mut cx).is_pending());
	assert!(!reactor.idle(), "the receive has to be in flight");
	let started = Instant::now();
	drop(completion);
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"the drop waited for the kernel"
	);
	assert!(
		!reactor.idle(),
		"the detached receive holds the buffer until its cancellation lands"
	);
	drive_until_idle(&reactor);
	assert!(
		reactor.idle(),
		"the reactor released the buffer once the cancellation landed"
	);
	peer.write_all(b"later").unwrap();
	reactor.drive(Some(Duration::from_millis(1))).unwrap();
	assert_eq!(
		buf.whole().to_vec(),
		&[0u8; 8],
		"nothing was left to write the buffer after the drop"
	);
}

/// Without force a shutdown lets what is out run to its own end: the receive
/// gets the bytes the peer sends later, not the stopped error.
#[test]
#[cfg(unix)]
fn a_graceful_shutdown_lets_an_operation_land_on_its_own() {
	let (mut left, right) = socket_pair();
	let reactor = entered(Config::default());
	let buf = Buffer::zeroed(8);
	let mut completion = stalled_recv(&right, &buf);
	let mut cx = Context::from_waker(Waker::noop());
	assert!(poll(&mut completion, &mut cx).is_pending());
	let peer = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_millis(50));
		left.write_all(b"late").unwrap();
		left
	});
	reactor.shutdown(false).unwrap();
	let _left = peer.join().unwrap();
	let result = match poll(&mut completion, &mut cx) {
		Poll::Ready(result) => result,
		Poll::Pending => panic!("the shutdown returned with the receive still out"),
	};
	assert!(matches!(result, Ok(Response::Count(4))));
	assert_eq!(&buf.whole().to_vec()[..4], b"late");
}

/// A readiness wait names nothing of the caller's, so dropping one is not a
/// bug — but it still has to be stopped and reaped: until its entry goes, the
/// wait holds the descriptor open, and a wait on a descriptor that never
/// becomes ready has no such end.
#[test]
#[cfg(unix)]
fn a_dropped_readiness_wait_is_cancelled_and_reaped() {
	let (reader, _writer) = std::io::pipe().unwrap();
	let reactor = entered(Config::default());
	let mut reader = Arc::new(reader);
	let mut cx = Context::from_waker(Waker::noop());
	{
		let mut wait = submission(fd(&reader), Op::Readable);
		assert!(
			poll(&mut wait, &mut cx).is_pending(),
			"the pipe has no data"
		);
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
	fn drive_until_settled(reactor: &Entered) {
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
				Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
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
		reactor: &Entered,
		client: &mut std::os::unix::net::UnixStream,
	) {
		client.set_nonblocking(true).unwrap();
		let mut byte = [0u8; 1];
		for _ in 0..500 {
			reactor.drive(Some(Duration::from_millis(1))).unwrap();
			match client.read(&mut byte) {
				Ok(0) => return,
				Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
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
		let reactor = entered(Config::default());
		let listener = Arc::new(listener);
		let mut client;
		{
			let mut accept = submission(fd(&listener), Op::Accept);
			let mut cx = Context::from_waker(Waker::noop());
			// Hand the accept to the kernel.
			assert!(poll(&mut accept, &mut cx).is_pending());
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
		let reactor = entered(Config::default());
		let listener = Arc::new(listener);
		let mut client;
		{
			let mut accept = submission(fd(&listener), Op::Accept);
			let mut cx = Context::from_waker(Waker::noop());
			// Hand the accept to the kernel with nothing to accept yet.
			assert!(poll(&mut accept, &mut cx).is_pending());
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
		use crate::Scope;
		use std::os::unix::net::UnixStream;

		let directory = tempdir("completion-accept-raced");
		let (listener, path) = listener(directory.path());
		let reactor = entered(Config::default());
		let listener = Arc::new(listener);
		let scope = Scope::new();

		let mut client;
		{
			// Filed by hand: with no executor on the thread, a submission has no
			// scope to file with, so the test files it the way an executor would.
			let slot = submit(fd(&listener), Op::Accept);
			scope.track(slot.clone());
			let mut accept: Submission = Box::pin(slot);
			let mut cx = Context::from_waker(Waker::noop());
			// Hand the accept to the kernel with nothing to accept yet.
			assert!(poll(&mut accept, &mut cx).is_pending());
			reactor.drive(Some(Duration::ZERO)).unwrap();

			client = UnixStream::connect(&path).unwrap();
			// The kernel reports the connection without the reactor being driven,
			// so the accept is done before the cancel fires.
			std::thread::sleep(Duration::from_millis(50));
			drop(scope.cancel());
			assert!(
				poll(&mut accept, &mut cx).is_pending(),
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
	use std::error::Error as _;
	use std::io;
	use std::net::Shutdown;
	use std::os::unix::net::UnixStream;

	fn pair() -> (UnixStream, Arc<UnixStream>) {
		socket_pair()
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
		let reactor = entered(Config::default());
		let mut hangup = submission(fd(&stream), Op::Hangup);
		let mut cx = Context::from_waker(Waker::noop());
		assert!(poll(&mut hangup, &mut cx).is_pending());

		drop(peer);
		for _ in 0..10 {
			reactor.drive(Some(Duration::from_millis(10))).unwrap();
		}
		assert!(
			poll(&mut hangup, &mut cx).is_pending(),
			"a FIN alone is a half-close"
		);

		stream.shutdown(Shutdown::Write).unwrap();
		drive_to_result(&reactor, hangup).unwrap();
	}

	/// A multishot poll the kernel cannot report to — its completion queue full —
	/// is ended rather than left armed. The wait is still owed, so what lands is a
	/// temporary failure the caller can re-issue, not a hangup and not a wait that
	/// never ends.
	#[test]
	fn a_poll_the_kernel_ends_early_is_a_temporary_failure() {
		// A ring whose completion queue holds four: a burst overflows it, which is
		// what makes the kernel end the poll rather than queue its report.
		let reactor = entered(Config {
			entries: 2,
			..Config::default()
		});
		let (left, right) = pair();
		let mut hangup = submission(fd(&right), Op::Hangup);
		let mut cx = Context::from_waker(Waker::noop());
		assert!(poll(&mut hangup, &mut cx).is_pending());

		// The burst: reads that land at once, none of them reaped yet, and each
		// held so its operation stays with the ring.
		let scratch = tempdir("hangup-terminated");
		let buf: Buffer = Buffer::zeroed(24);
		let mut reads = Vec::new();
		for index in 0..24usize {
			let path = scratch.path().join(format!("data{index}"));
			std::fs::write(&path, b"hello").unwrap();
			let file = Arc::new(OpenOptions::new().read(true).open(&path).unwrap());
			let span = buf.span(index, 1);
			let mut read = submission(
				fd(&file),
				Op::Read {
					offset: Some(0),
					span,
				},
			);
			assert!(poll(&mut read, &mut cx).is_pending());
			reads.push(read);
		}
		// Let every read land, and leave them in the queue: the poll's report has
		// nowhere to go, so the kernel ends the multishot instead.
		std::thread::sleep(Duration::from_millis(50));
		left.shutdown(Shutdown::Write).unwrap();

		let deadline = Instant::now() + Duration::from_secs(5);
		let result = loop {
			reactor.drive(Some(Duration::from_millis(20))).unwrap();
			if let Poll::Ready(result) = poll(&mut hangup, &mut cx) {
				break result;
			}
			assert!(
				Instant::now() < deadline,
				"the wait outlived a poll the kernel ended"
			);
		};
		let error = result.expect_err("an ended poll is not a hangup");
		assert_eq!(
			error
				.source()
				.and_then(|s| s.downcast_ref::<io::Error>())
				.map(io::Error::kind),
			Some(io::ErrorKind::WouldBlock),
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
		let reactor = entered(Config::default());
		let mut hangup = submission(fd(&right), Op::Hangup);
		let mut cx = Context::from_waker(Waker::noop());
		assert!(poll(&mut hangup, &mut cx).is_pending());

		drop(left);
		drive_to_result(&reactor, hangup).unwrap();

		// The slot the reported hangup lived in is free again, and the next wait is
		// put in it: the table hands a freed slot straight back out.
		let (peer, other) = pair();
		let mut wait = submission(fd(&other), Op::Hangup);
		assert!(poll(&mut wait, &mut cx).is_pending());
		for _ in 0..5 {
			reactor.drive(Some(Duration::from_millis(5))).unwrap();
		}
		assert!(
			poll(&mut wait, &mut cx).is_pending(),
			"the completion of the operation that had the slot landed on this one"
		);

		// This wait still answers to its own peer.
		drop(peer);
		drive_to_result(&reactor, wait).unwrap();
	}
}
