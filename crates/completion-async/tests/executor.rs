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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use completion_async::{Buffer, Config, ErrorKind, Executor, File, timeout};

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
		let task = executor.spawn(handed_over(flag, handoff));
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
		// The scope, not the executor: `block_on` holds the executor mutably for
		// the whole run, and the scope is what a spawn goes through.
		let scope = executor.scope();
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
				drop(scope.spawn(async move { flag.store(true, Ordering::Release) }));
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

/// The executor owns the reactor, so dropping it drops the reactor too: an
/// operation already in flight on that reactor resolves to `ErrorKind::Cancelled`
/// when polled after the executor is dropped.
#[test]
fn an_operation_after_the_executor_drops_is_refused() {
	let mut executor = executor();
	let (_peer, socket) = socket_pair();
	let buffer: Buffer = Buffer::zeroed(5);
	let mut recv = Box::pin(socket.recv(&buffer));
	executor
		.block_on(std::future::poll_fn(|cx| {
			assert!(recv.as_mut().poll(cx).is_pending());
			Poll::Ready(())
		}))
		.unwrap();
	drop(executor);

	let mut cx = Context::from_waker(Waker::noop());
	let Poll::Ready(outcome) = std::future::Future::poll(recv.as_mut(), &mut cx) else {
		panic!("the operation parked forever after the executor dropped");
	};
	match outcome {
		Err(error) => assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}"),
		Ok(count) => panic!("a gone reactor took the operation and moved {count} bytes"),
	}
}

#[test]
fn awaiting_a_task_after_its_executor_drops_panics() {
	let executor = executor();
	let mut task = Box::pin(executor.spawn(std::future::pending::<()>()));
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
		let _busy = executor.spawn(std::future::poll_fn(move |cx| {
			if flag.load(Ordering::Acquire) {
				Poll::Ready(())
			} else {
				cx.waker().wake_by_ref();
				Poll::Pending
			}
		}));
		let file = File::new(common::open(&path).unwrap());
		let buffer = Buffer::zeroed(5);
		let payload = buffer.whole();
		let timeout = timeout(Duration::from_millis(1));
		executor
			.block_on(async {
				assert_eq!(file.read_at(payload, 0).await.unwrap(), 5);
				timeout.await.unwrap();
			})
			.unwrap();
		assert_eq!(buffer.whole().to_vec(), b"hello");
		let _ = done.send(());
	});
	let completed = finished.recv_timeout(Duration::from_secs(3));
	stop.store(true, Ordering::Release);
	driver.join().unwrap();
	completed.expect("a self-waking task starved real I/O or the timer");
}

fn socket_pair() -> (std::net::TcpStream, completion_async::Stream) {
	let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
	let peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (stream, _) = listener.accept().unwrap();
	stream.set_nonblocking(true).unwrap();
	(peer, completion_async::Stream::new(stream))
}

fn stalled_task(
	executor: &mut Executor,
	buffer: &Buffer,
) -> (std::net::TcpStream, impl Future<Output = ()> + use<>) {
	let (peer, socket) = socket_pair();
	let payload = buffer.whole();
	let task = executor.spawn(async move {
		socket.recv(payload).await.unwrap();
	});
	executor
		.block_on(timeout(Duration::from_millis(1)))
		.unwrap()
		.unwrap();
	(peer, task)
}

/// The deadline is fixed when the timer is constructed, not when it is first
/// polled: a timer held past its duration has already come due, so the
/// first poll finds it fired rather than starting the wait then.
#[test]
fn a_timeout_held_past_its_duration_fires_on_its_first_poll() {
	let mut executor = executor();
	let timeout = timeout(Duration::from_millis(5));
	std::thread::sleep(Duration::from_millis(30));
	let started = std::time::Instant::now();
	executor.block_on(timeout).unwrap().unwrap();
	assert!(
		started.elapsed() < Duration::from_millis(5),
		"the wait restarted at the first poll instead of holding the deadline"
	);
}

/// A timer armed on an executor before that executor drops resolves to
/// `ErrorKind::Cancelled` when polled after teardown.
#[test]
fn a_timer_armed_before_a_stop_reports_stopped_from_the_poll() {
	let mut executor = executor();
	let mut timeout = Box::pin(timeout(Duration::from_secs(3600)));
	executor
		.block_on(std::future::poll_fn(|cx| {
			assert!(timeout.as_mut().poll(cx).is_pending());
			Poll::Ready(())
		}))
		.unwrap();
	drop(executor);
	let mut cx = Context::from_waker(Waker::noop());
	let Poll::Ready(error) = std::future::Future::poll(timeout.as_mut(), &mut cx) else {
		panic!("a timer armed before the stop stayed pending");
	};
	let error = error.unwrap_err();
	assert_eq!(error.kind(), ErrorKind::Cancelled, "{error}");
}

#[test]
fn dropping_a_join_handle_detaches_the_task() {
	let mut executor = executor();
	let ran = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&ran);
	let handle = executor.spawn(async move {
		timeout(Duration::from_millis(5)).await.unwrap();
		flag.store(true, Ordering::Release);
	});
	drop(handle);
	// Longer than the task's own timer, so a task left running has the time to
	// reach the end of its body.
	executor
		.block_on(timeout(Duration::from_millis(30)))
		.unwrap()
		.unwrap();
	assert!(
		ran.load(Ordering::Acquire),
		"dropping the join handle cancelled its task"
	);
}

#[test]
fn a_task_can_drop_its_own_join_handle_while_finishing() {
	use std::cell::{Cell, RefCell};
	use std::rc::Rc;

	let mut executor = executor();
	let (_peer, socket) = socket_pair();
	let (buffer, live) = common::tracked(8);
	let payload = buffer.whole();
	drop(buffer);
	let mut receive = Box::pin(socket.recv(payload));
	let handle = Rc::new(RefCell::new(None::<completion_async::JoinHandle<()>>));
	let finish = Rc::new(Cell::new(false));
	let wake = Rc::new(RefCell::new(None::<Waker>));
	let own_handle = Rc::clone(&handle);
	let flag = Rc::clone(&finish);
	let task_wake = Rc::clone(&wake);
	*handle.borrow_mut() = Some(executor.spawn(std::future::poll_fn(move |cx| {
		if flag.get() {
			drop(own_handle.borrow_mut().take());
			return Poll::Ready(());
		}
		*task_wake.borrow_mut() = Some(cx.waker().clone());
		assert!(receive.as_mut().poll(cx).is_pending());
		Poll::Pending
	})));
	executor
		.block_on(timeout(Duration::from_millis(1)))
		.unwrap()
		.unwrap();
	finish.set(true);
	wake.borrow().as_ref().unwrap().wake_by_ref();
	// The task's own end drops its in-flight receive, which the reactor reaps on
	// a later pass.
	executor.block_on(async {}).unwrap();
	executor
		.block_on(timeout(Duration::from_millis(10)))
		.unwrap()
		.unwrap();
	assert!(!live.alive(), "the finished task kept its I/O buffer");
}

#[test]
fn a_panicking_task_releases_in_flight_io_and_leaves_the_executor_usable() {
	use std::cell::Cell;
	use std::rc::Rc;

	let mut executor = executor();
	let (_peer, socket) = socket_pair();
	let (buffer, live) = common::tracked(8);
	let payload = buffer.whole();
	drop(buffer);
	let mut receive = Box::pin(socket.recv(payload));
	let panic_next = Rc::new(Cell::new(false));
	let flag = Rc::clone(&panic_next);
	let (handoff, handed) = std::sync::mpsc::channel();
	let mut task = executor.spawn(std::future::poll_fn(move |cx| {
		if flag.get() {
			panic!("task failed while waiting for I/O");
		}
		handoff.send(cx.waker().clone()).unwrap();
		assert!(receive.as_mut().poll(cx).is_pending());
		Poll::Pending::<()>
	}));
	executor
		.block_on(timeout(Duration::from_millis(1)))
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
	// Unwinding dropped the job and released its I/O as a detached cancel; a
	// pass over the reactor reaps that cancel.
	executor
		.block_on(timeout(Duration::from_millis(10)))
		.unwrap()
		.unwrap();
	assert!(!live.alive(), "unwinding kept the I/O buffer");
	let gone = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		std::pin::Pin::new(&mut task).poll(&mut Context::from_waker(Waker::noop()))
	}));
	assert!(gone.is_err(), "joining a failed task parked forever");
	let next = executor.spawn(async { Rc::new(42) });
	assert_eq!(*executor.block_on(next).unwrap(), 42);
}

#[test]
fn task_destructors_observe_other_joins_as_gone_during_executor_teardown() {
	use std::cell::{Cell, RefCell};
	use std::rc::Rc;

	struct Observe {
		tasks: Rc<RefCell<Vec<completion_async::JoinHandle<()>>>>,
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
		tasks.borrow_mut().push(executor.spawn(async move {
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
	let (buffer, live) = common::tracked(8);
	let (_peer, task) = stalled_task(&mut executor, &buffer);
	drop(executor);
	drop(buffer);
	assert!(!live.alive(), "executor teardown kept the buffer");
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
	let scope = executor.scope();
	let mut to_spawn = Some(flag);
	executor
		.block_on(std::future::poll_fn(move |_| {
			if let Some(flag) = to_spawn.take() {
				drop(scope.spawn(async move { flag.store(true, Ordering::Release) }));
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
	let task = executor.spawn(async move { flag.store(true, Ordering::Release) });
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
