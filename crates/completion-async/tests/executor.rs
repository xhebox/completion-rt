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

use completion_async::{
	Config, Error, Executor as _, ExecutorClosed, ExecutorEnum, File, Memory, Timeout,
};

fn executor() -> ExecutorEnum {
	common::init_logging();
	ExecutorEnum::new(Config::default()).unwrap()
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

/// kibotos notifies the reactor right after spawning a task, so a freshly
/// spawned task runs even when nothing else stirs the loop. That notify is
/// redundant: `spawn` raises the executor's own wake, and the loop has to carry
/// it into the pass that follows while the root future stays pending. Reading
/// the wake without running the task — consuming it below the top of the loop —
/// leaves the reactor parked with a runnable task in hand. Nothing raises a
/// second notification here, so a lost wake parks forever and the run is watched
/// by a deadline.
#[test]
fn a_task_spawned_while_the_root_future_is_pending_runs_without_a_notify() {
	let (done, finished) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let mut executor = executor();
		let ran = Arc::new(AtomicBool::new(false));
		let flag = Arc::clone(&ran);
		let spawner = executor.spawner();
		let mut spawned = false;
		let outcome = executor.block_on(std::future::poll_fn(move |_| {
			if ran.load(Ordering::Acquire) {
				return Poll::Ready(());
			}
			if !spawned {
				// The spawn's own wake is the only notification: the root future
				// raises no other, and nothing touches the reactor.
				spawned = true;
				let flag = Arc::clone(&flag);
				spawner
					.spawn(async move { flag.store(true, Ordering::Release) })
					.detach();
			}
			Poll::Pending
		}));
		let _ = done.send(outcome.is_ok());
	});
	let completed = finished.recv_timeout(Duration::from_secs(10));
	assert!(
		completed.is_ok(),
		"the spawned task never ran: the executor parked with the wake the spawn raised"
	);
	assert!(completed.unwrap(), "block_on failed before the task ran");
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
	let file = File::new(common::open(&path).unwrap(), &executor.submitter());
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

#[test]
fn a_local_executor_reads_a_real_file() {
	let directory = common::tempdir("completion-local").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let mut executor = executor();
	let file = File::new(common::open(&path).unwrap(), &executor.submitter());
	let buffer = Arc::new(vec![0u8; 5]);
	let payload: Arc<dyn Memory> = buffer.clone();
	let outcome = executor.block_on(file.read_at(payload, 0)).unwrap();
	assert_eq!(outcome.into_result().unwrap(), 5);
	assert_eq!(&buffer[..], b"hello");
}

#[test]
fn a_spawner_cannot_spawn_after_its_executor_drops() {
	let executor = executor();
	let spawner = executor.spawner();
	drop(executor);
	let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		spawner.spawn(async {}).detach();
	}));
	assert!(caught.is_err(), "a dead executor accepted a task");
}

#[test]
fn try_spawn_reports_a_dropped_executor() {
	let executor = executor();
	let spawner = executor.spawner();
	drop(executor);
	assert_eq!(spawner.try_spawn(async {}).err(), Some(ExecutorClosed));
}

#[test]
fn awaiting_a_task_after_its_executor_drops_panics() {
	let executor = executor();
	let mut task = Box::pin(executor.spawner().spawn(std::future::pending::<()>()));
	drop(executor);
	let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		task.as_mut().poll(&mut Context::from_waker(Waker::noop()))
	}));
	assert!(caught.is_err(), "a task with no executor parked forever");
}

#[test]
fn a_busy_task_does_not_starve_file_io_or_timers() {
	let stop = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&stop);
	let (done, finished) = std::sync::mpsc::channel();
	let driver = std::thread::spawn(move || {
		let directory = common::tempdir("completion-busy").unwrap();
		let path = directory.path().join("data");
		std::fs::write(&path, b"hello").unwrap();
		let mut executor = executor();
		let submitter = executor.submitter();
		let busy = executor.spawner().spawn(std::future::poll_fn(move |cx| {
			if flag.load(Ordering::Acquire) {
				Poll::Ready(())
			} else {
				cx.waker().wake_by_ref();
				Poll::Pending
			}
		}));
		let file = File::new(common::open(&path).unwrap(), &submitter);
		let buffer = Arc::new(vec![0u8; 5]);
		let payload: Arc<dyn Memory> = buffer.clone();
		executor
			.block_on(async {
				assert_eq!(file.read_at(payload, 0).await.into_result().unwrap(), 5);
				Timeout::new(Duration::from_millis(1), &submitter)
					.await
					.unwrap();
			})
			.unwrap();
		assert_eq!(&buffer[..], b"hello");
		drop(busy);
		let _ = done.send(());
	});
	let completed = finished.recv_timeout(Duration::from_secs(3));
	stop.store(true, Ordering::Release);
	driver.join().unwrap();
	completed.expect("a self-waking task starved real I/O or the timer");
}

fn socket_pair(
	submitter: &completion_async::Submitter,
) -> (
	std::net::TcpStream,
	completion_async::Facade<std::net::TcpStream>,
) {
	let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
	let peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (stream, _) = listener.accept().unwrap();
	stream.set_nonblocking(true).unwrap();
	(peer, completion_async::Facade::new(stream, submitter))
}

fn stalled_task(
	executor: &mut ExecutorEnum,
	buffer: &Arc<Vec<u8>>,
) -> (std::net::TcpStream, impl Future<Output = ()> + use<>) {
	let (peer, socket) = socket_pair(&executor.submitter());
	let payload: Arc<dyn Memory> = buffer.clone();
	let task = executor.spawner().spawn(async move {
		socket.recv(payload).await.into_result().unwrap();
	});
	let submitter = executor.submitter();
	executor
		.block_on(Timeout::new(Duration::from_millis(1), &submitter))
		.unwrap()
		.unwrap();
	(peer, task)
}

#[test]
fn dropping_a_task_defers_cancellation_and_returns_its_io_buffer() {
	let mut executor = executor();
	let buffer = Arc::new(vec![0u8; 8]);
	let (_peer, task) = stalled_task(&mut executor, &buffer);
	drop(task);
	assert!(
		Arc::strong_count(&buffer) > 1,
		"Task::drop destroyed its future inline"
	);
	executor.block_on(async {}).unwrap();
	assert_eq!(
		Arc::strong_count(&buffer),
		1,
		"cancelled I/O kept the buffer"
	);
}

#[test]
fn a_task_can_cancel_itself_while_finishing_with_io_still_in_flight() {
	use std::cell::{Cell, RefCell};
	use std::rc::Rc;

	let mut executor = executor();
	let submitter = executor.submitter();
	let (_peer, socket) = socket_pair(&submitter);
	let buffer = Arc::new(vec![0u8; 8]);
	let payload: Arc<dyn Memory> = buffer.clone();
	let mut receive = Box::pin(socket.recv(payload));
	let handle = Rc::new(RefCell::new(None::<completion_async::Task<()>>));
	let cancel = Rc::new(Cell::new(false));
	let wake = Rc::new(RefCell::new(None::<Waker>));
	let own_handle = Rc::clone(&handle);
	let flag = Rc::clone(&cancel);
	let task_wake = Rc::clone(&wake);
	*handle.borrow_mut() = Some(executor.spawner().spawn(std::future::poll_fn(move |cx| {
		if flag.get() {
			drop(own_handle.borrow_mut().take());
			return Poll::Ready(());
		}
		*task_wake.borrow_mut() = Some(cx.waker().clone());
		assert!(receive.as_mut().poll(cx).is_pending());
		Poll::Pending
	})));
	executor
		.block_on(Timeout::new(Duration::from_millis(1), &submitter))
		.unwrap()
		.unwrap();
	cancel.set(true);
	wake.borrow().as_ref().unwrap().wake_by_ref();
	executor.block_on(async {}).unwrap();
	assert_eq!(
		Arc::strong_count(&buffer),
		1,
		"self-cancellation kept the I/O buffer"
	);
}

#[test]
fn a_panicking_task_releases_in_flight_io_and_leaves_the_executor_usable() {
	use std::cell::Cell;
	use std::rc::Rc;

	let mut executor = executor();
	let submitter = executor.submitter();
	let (_peer, socket) = socket_pair(&submitter);
	let buffer = Arc::new(vec![0u8; 8]);
	let payload: Arc<dyn Memory> = buffer.clone();
	let mut receive = Box::pin(socket.recv(payload));
	let panic_next = Rc::new(Cell::new(false));
	let flag = Rc::clone(&panic_next);
	let (handoff, handed) = std::sync::mpsc::channel();
	let mut task = executor.spawner().spawn(std::future::poll_fn(move |cx| {
		if flag.get() {
			panic!("task failed while waiting for I/O");
		}
		handoff.send(cx.waker().clone()).unwrap();
		assert!(receive.as_mut().poll(cx).is_pending());
		Poll::Pending::<()>
	}));
	executor
		.block_on(Timeout::new(Duration::from_millis(1), &submitter))
		.unwrap()
		.unwrap();
	panic_next.set(true);
	handed.try_recv().unwrap().wake();
	let caught =
		std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| executor.block_on(async {})));
	let panic = caught.expect_err("a task panic must leave block_on");
	assert_eq!(
		panic.downcast_ref::<&str>(),
		Some(&"task failed while waiting for I/O")
	);
	assert_eq!(
		Arc::strong_count(&buffer),
		1,
		"unwinding kept the I/O buffer"
	);
	let gone = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		std::pin::Pin::new(&mut task).poll(&mut Context::from_waker(Waker::noop()))
	}));
	assert!(gone.is_err(), "joining a failed task parked forever");
	let next = executor.spawner().spawn(async { Rc::new(42) });
	assert_eq!(*executor.block_on(next).unwrap(), 42);
}

#[test]
fn cancellation_unwinding_does_not_hide_later_unsignalled_io_drops() {
	struct PanicOnDrop;

	impl Drop for PanicOnDrop {
		fn drop(&mut self) {
			panic!("cancelled future failed during drop");
		}
	}

	let mut executor = executor();
	let guard = PanicOnDrop;
	let task = executor.spawner().spawn(async move {
		std::future::pending::<()>().await;
		drop(guard);
	});
	drop(task);
	let caught =
		std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| executor.block_on(async {})));
	assert!(caught.is_err(), "a future destructor panic was swallowed");

	let submitter = executor.submitter();
	let (_peer, socket) = socket_pair(&submitter);
	let buffer = Arc::new(vec![0u8; 8]);
	let payload: Arc<dyn Memory> = buffer.clone();
	let mut receive = Box::pin(socket.recv(payload));
	assert!(
		receive
			.as_mut()
			.poll(&mut Context::from_waker(Waker::noop()))
			.is_pending()
	);
	executor
		.block_on(Timeout::new(Duration::from_millis(1), &submitter))
		.unwrap()
		.unwrap();
	let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(receive)));
	assert_eq!(
		caught.is_err(),
		cfg!(debug_assertions),
		"task cancellation hid a later unsignalled I/O drop"
	);
	assert_eq!(
		Arc::strong_count(&buffer),
		1,
		"the unsignalled drop kept its buffer"
	);
}

#[test]
fn task_destructors_observe_other_joins_as_gone_during_executor_teardown() {
	use std::cell::{Cell, RefCell};
	use std::rc::Rc;

	struct Observe {
		tasks: Rc<RefCell<Vec<completion_async::Task<()>>>>,
		all_gone: Rc<Cell<bool>>,
	}

	impl Drop for Observe {
		fn drop(&mut self) {
			for task in self.tasks.borrow_mut().iter_mut() {
				let gone = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
					std::pin::Pin::new(task).poll(&mut Context::from_waker(Waker::noop()))
				}))
				.is_err();
				self.all_gone.set(self.all_gone.get() && gone);
			}
		}
	}

	let mut executor = executor();
	let tasks = Rc::new(RefCell::new(Vec::new()));
	let all_gone = Rc::new(Cell::new(true));
	for _ in 0..2 {
		let observe = Observe {
			tasks: Rc::clone(&tasks),
			all_gone: Rc::clone(&all_gone),
		};
		tasks
			.borrow_mut()
			.push(executor.spawner().spawn(async move {
				std::future::pending::<()>().await;
				drop(observe);
			}));
	}
	executor.block_on(async {}).unwrap();
	drop(executor);
	assert!(
		all_gone.get(),
		"a task destructor found a join still waiting on a dead executor"
	);
}

#[test]
fn dropping_the_executor_returns_a_tasks_in_flight_io_buffer() {
	let mut executor = executor();
	let buffer = Arc::new(vec![0u8; 8]);
	let (_peer, task) = stalled_task(&mut executor, &buffer);
	drop(executor);
	assert_eq!(
		Arc::strong_count(&buffer),
		1,
		"executor teardown kept the buffer"
	);
	drop(task);
}

/// A root future that is ready on its first poll returns there and then. A task
/// it detached before returning is queued only as a consequence of that poll,
/// so the task has not been polled once by the time the loop would return.
/// `block_on` has to make one closing pass before it hands the result back, or
/// the detached task waits for the next call to run at all.
#[test]
fn block_on_runs_a_detached_task_before_returning_when_the_root_future_is_ready_at_once() {
	let mut executor = executor();
	let ran = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&ran);
	let spawner = executor.spawner();
	let mut to_spawn = Some(flag);
	executor
		.block_on(std::future::poll_fn(move |_| {
			if let Some(flag) = to_spawn.take() {
				spawner
					.spawn(async move { flag.store(true, Ordering::Release) })
					.detach();
			}
			Poll::Ready(())
		}))
		.unwrap();
	assert!(
		ran.load(Ordering::Acquire),
		"a task detached by a root future ready at once never ran"
	);
}

/// The root future is polled before the ready-task queue is drained, so work
/// already queued when `block_on` starts cannot delay the future the caller
/// handed in. This is the ordering compio's `block_on_at` uses
/// (`ignore/compio/compio-runtime/src/lib.rs:190-201`).
#[test]
fn block_on_polls_the_root_future_before_a_task_queued_before_it() {
	use std::cell::Cell;
	use std::rc::Rc;

	let mut executor = executor();
	let ran = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&ran);
	let polled = Arc::clone(&ran);
	let task = executor
		.spawner()
		.spawn(async move { flag.store(true, Ordering::Release) });
	let seen_unpolled = Rc::new(Cell::new(false));
	let observed = Rc::clone(&seen_unpolled);
	let mut first = true;
	let mut task = std::pin::pin!(task);
	executor
		.block_on(std::future::poll_fn(move |cx| {
			if first {
				first = false;
				observed.set(!polled.load(Ordering::Acquire));
			}
			task.as_mut().poll(cx)
		}))
		.unwrap();
	assert!(
		seen_unpolled.get(),
		"a task queued before block_on ran before the root future was first polled"
	);
	assert!(ran.load(Ordering::Acquire), "the queued task never ran");
}
