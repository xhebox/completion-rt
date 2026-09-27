//! The executor: which wakes reach a thread parked in `block_on`, and which
//! tasks a pass over the loop runs.
//!
//! A wake on the driver thread itself is seen by the poll that follows it
//! whatever the reactor does, so the off-thread tests drive their executor on a
//! thread of its own: only a wake raised from another thread can show whether
//! it reached the wait the driver is parked in.

mod common;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use completion_rt::{Config, Error, Executor, File, Memory};

fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// A future that hands its waker to another thread on the first poll and
/// stays pending until that thread raises `woken` and wakes it.
fn handed_over(woken: Arc<AtomicBool>, handoff: Sender<Waker>) -> impl Future<Output = ()> {
	let mut handoff = Some(handoff);
	std::future::poll_fn(move |cx| {
		if woken.load(Ordering::Acquire) {
			return Poll::Ready(());
		}
		if let Some(handoff) = handoff.take() {
			let _ = handoff.send(cx.waker().clone());
		}
		Poll::Pending
	})
}

/// A thread that `block_on`s a future handing its waker over on the first
/// poll: nothing goes through the reactor, so the waker is the only way back.
/// The two channels are what the caller keeps its deadline on.
fn driver_on_another_thread(woken: Arc<AtomicBool>) -> (Receiver<Waker>, Receiver<()>) {
	let (handoff, handed) = std::sync::mpsc::channel();
	let (done, finished) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let mut executor = executor();
		executor.block_on(handed_over(woken, handoff)).unwrap();
		let _ = done.send(());
	});
	(handed, finished)
}

#[test]
fn a_wake_from_outside_the_reactor_ends_block_on() {
	let woken = Arc::new(AtomicBool::new(false));
	let (handed, finished) = driver_on_another_thread(Arc::clone(&woken));
	let deadline = Duration::from_secs(10);
	let waker = handed
		.recv_timeout(deadline)
		.expect("the future hands its waker to the thread");
	// Long enough that the thread is parked in the reactor's wait, so the
	// wake has to reach the wait rather than a poll about to happen.
	std::thread::sleep(Duration::from_millis(50));
	woken.store(true, Ordering::Release);
	waker.wake();
	finished
		.recv_timeout(deadline)
		.expect("block_on returns once the thread is woken");
}

#[test]
fn a_task_woken_from_another_thread_ends_block_on() {
	let woken = Arc::new(AtomicBool::new(false));
	let (handoff, handed) = std::sync::mpsc::channel();
	let (done, finished) = std::sync::mpsc::channel();
	let flag = Arc::clone(&woken);
	std::thread::spawn(move || {
		let mut executor = executor();
		// What is awaited this time is a task, whose waker is the executor's
		// own: the task is made ready, and the reactor nothing.
		let task = executor.spawner().spawn(handed_over(flag, handoff));
		executor.block_on(task).unwrap();
		let _ = done.send(());
	});
	let deadline = Duration::from_secs(10);
	let waker = handed
		.recv_timeout(deadline)
		.expect("the task hands its waker to the thread");
	// Long enough that the thread is parked in the reactor's wait.
	std::thread::sleep(Duration::from_millis(50));
	woken.store(true, Ordering::Release);
	waker.wake();
	finished
		.recv_timeout(deadline)
		.expect("block_on returns once the task is woken");
}

#[test]
fn a_wake_on_the_driver_thread_does_not_park_the_loop() {
	let (done, finished) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let mut executor = executor();
		let (handoff, handed) = std::sync::mpsc::channel();
		let woken = Arc::new(AtomicBool::new(false));
		let flag = Arc::clone(&woken);
		let ran = Arc::new(AtomicBool::new(false));
		let ran_in_task = Arc::clone(&ran);
		let task = executor.spawner().spawn(async move {
			handed_over(flag, handoff).await;
			ran_in_task.store(true, Ordering::Release);
		});
		task.detach();
		let mut handed = Some(handed);
		// Nothing wakes this future: it returns once the task it readied has
		// run, so the loop has to poll it again after the tick.
		executor
			.block_on(std::future::poll_fn(move |_| {
				if ran.load(Ordering::Acquire) {
					return Poll::Ready(());
				}
				if let Some(handed) = handed.take() {
					let waker = handed
						.try_recv()
						.expect("the task ran before the future was polled");
					woken.store(true, Ordering::Release);
					waker.wake();
				}
				Poll::Pending
			}))
			.unwrap();
		let _ = done.send(());
	});
	finished
		.recv_timeout(Duration::from_secs(10))
		.expect("block_on runs the task it readied");
}

#[test]
fn block_on_runs_the_tasks_it_finds_ready() {
	let mut executor = executor();
	let ran = Arc::new(AtomicUsize::new(0));
	let flag = Arc::clone(&ran);
	executor
		.spawner()
		.spawn(async move { flag.fetch_add(1, Ordering::AcqRel) })
		.detach();
	executor.block_on(async {}).unwrap();
	assert_eq!(ran.load(Ordering::Acquire), 1);
}

/// The executor owns the reactor, so dropping it drops the reactor too. A
/// facade built from that executor's submitter — the kibotos shape, where a
/// mount's executor goes and a facade bound to it stays — is then left with
/// nothing to drive its operations: the next one has to be refused at once, not
/// accepted into a table no poller will ever reach.
#[test]
fn an_operation_after_the_executor_drops_is_refused() {
	let directory = common::tempdir("completion-drop").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();

	let executor = executor();
	let file = File::new(std::fs::File::open(&path).unwrap(), &executor.submitter());
	drop(executor);

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let payload: Arc<dyn Memory> = buffer.clone();
	let mut read = std::pin::pin!(file.read_at(payload, 0));
	let mut cx = Context::from_waker(Waker::noop());
	// Nothing drives this submission, so it either lands on this poll or never
	// does. Failing here rather than parking keeps a regression from hanging the
	// suite.
	let Poll::Ready(outcome) = std::future::Future::poll(read.as_mut(), &mut cx) else {
		panic!("the operation submitted after the executor dropped parked forever");
	};
	match outcome.into_result() {
		Err(Error::Io(error)) => assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe),
		Err(error) => panic!("the refusal was not a broken pipe: {error}"),
		Ok(count) => panic!("a gone reactor took the operation and moved {count} bytes"),
	}
}

/// A mount makes its executor on one thread and hands it to the one that will
/// drive it, so an `Executor` has to move, and the thread it lands on is the
/// reactor's driver from then on. A wake raised from the thread that made it
/// reaches the new driver parked in `block_on`; it is not recorded for the
/// thread that is no longer driving.
#[test]
fn a_moved_executor_is_woken_from_the_thread_that_made_it() {
	let directory = common::tempdir("completion-moved").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();

	let mut executor = executor();
	// The making thread drives the reactor for one real operation first.
	let file = File::new(std::fs::File::open(&path).unwrap(), &executor.submitter());
	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let payload: Arc<dyn Memory> = buffer.clone();
	let outcome = executor.block_on(file.read_at(payload, 0)).unwrap();
	assert_eq!(outcome.into_result().unwrap(), 5, "the first read landed");
	assert_eq!(&buffer[..], b"hello");

	let woken = Arc::new(AtomicBool::new(false));
	let (handoff, handed) = std::sync::mpsc::channel();
	let (done, finished) = std::sync::mpsc::channel();
	let flag = Arc::clone(&woken);
	std::thread::spawn(move || {
		let mut executor = executor;
		// This wait is ended by a wake from the thread that made the executor,
		// which is not the thread parked in the reactor.
		executor.block_on(handed_over(flag, handoff)).unwrap();
		let _ = done.send(());
	});

	let deadline = Duration::from_secs(10);
	let waker = handed
		.recv_timeout(deadline)
		.expect("the future hands its waker to the making thread");
	// Long enough that the moved thread has parked in the reactor's wait, so the
	// wake has to reach that wait rather than a poll that is about to happen.
	std::thread::sleep(Duration::from_millis(50));
	woken.store(true, Ordering::Release);
	waker.wake();
	finished
		.recv_timeout(deadline)
		.expect("the wake from the making thread reached the moved executor");
}
