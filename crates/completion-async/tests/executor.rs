//! The executor: which wakes reach a thread parked in `block_on`, and which
//! tasks a pass over the loop runs.
//!
//! A wake on the driver thread itself is seen by the poll that follows it
//! whatever the reactor does, so the off-thread tests drive their executor on a
//! thread of its own: only a wake raised from another thread can show whether
//! it reached the wait the driver is parked in.
//!
//! The free functions — `block_on`, `spawn` — run on the thread's default
//! executor, which each test thread gets fresh.
//!
//! What a drop does — cancel the operation and detach it, without waiting for
//! the backend to let go of the caller's memory — is a reactor concern the
//! async layer cannot reach: its tests live in the low crate.

mod common;

use std::future::Future;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use completion_async::{Buffer, File, StreamSocket, block_on, spawn, spawn_local, timeout};

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
		common::init_logging();
		block_on(handed_over(woken, handoff)).unwrap();
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
		common::init_logging();
		// What is awaited this time is a task, whose waker is the executor's
		// own: the task is made ready, and the reactor nothing.
		let task = spawn(handed_over(flag, handoff));
		block_on(task).unwrap();
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

/// A task wake makes the task runnable, not the root: until the root's own
/// waker is raised, nothing it awaits has changed, so polling it again is
/// wasted work. A task that yields a few hundred times keeps the loop busy below
/// the root; the root counts its own polls and expects only the first one and
/// the wake that ends its wait.
#[test]
fn a_woken_task_does_not_poll_the_root_future() {
	use std::cell::{Cell, RefCell};
	use std::rc::Rc;

	let root_polls = Rc::new(Cell::new(0));
	let task_polls = Rc::new(Cell::new(0));
	let stored = Rc::new(RefCell::new(None::<Waker>));
	let done = Rc::new(Cell::new(false));
	let count = Rc::clone(&root_polls);
	let busy = Rc::clone(&task_polls);
	let root = Rc::clone(&stored);
	let finished = Rc::clone(&done);
	block_on(std::future::poll_fn(move |cx| {
		count.set(count.get() + 1);
		if finished.get() {
			return Poll::Ready(());
		}
		if root.borrow().is_none() {
			*root.borrow_mut() = Some(cx.waker().clone());
			let root = cx.waker().clone();
			let busy = Rc::clone(&busy);
			let finished = Rc::clone(&finished);
			spawn_local(async move {
				for _ in 0..300 {
					// Wake once and finish on the next poll, so each turn
					// through the loop leaves the task runnable again.
					let mut first = true;
					let busy = Rc::clone(&busy);
					std::future::poll_fn(move |cx| {
						busy.set(busy.get() + 1);
						if std::mem::replace(&mut first, false) {
							cx.waker().wake_by_ref();
							Poll::Pending
						} else {
							Poll::Ready(())
						}
					})
					.await;
				}
				finished.set(true);
				root.wake();
			});
		}
		Poll::Pending
	}))
	.unwrap();
	assert_eq!(task_polls.get(), 600, "the task did not keep the loop busy");
	assert_eq!(
		root_polls.get(),
		2,
		"the root was polled without being woken"
	);
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
fn a_busy_task_does_not_starve_file_io_or_timers() {
	let stop = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&stop);
	let (done, finished) = std::sync::mpsc::channel();
	let driver = std::thread::spawn(move || {
		let directory = common::tempdir("completion-busy").unwrap();
		let path = directory.path().join("data");
		std::fs::write(&path, b"hello").unwrap();
		common::init_logging();
		let _busy = spawn(std::future::poll_fn(move |cx| {
			if flag.load(Ordering::Acquire) {
				Poll::Ready(())
			} else {
				cx.waker().wake_by_ref();
				Poll::Pending
			}
		}));
		let file = File::new(common::open(&path).unwrap());
		let mut buffer = Buffer::zeroed(5);
		let payload = buffer.clone();
		let timeout = timeout(Duration::from_millis(1));
		block_on(async {
			assert_eq!(file.read_at(payload, 0).await.unwrap(), 5);
			timeout.await.unwrap();
		})
		.unwrap();
		assert_eq!(buffer.get_mut().unwrap().to_vec(), b"hello");
		let _ = done.send(());
	});
	let completed = finished.recv_timeout(Duration::from_secs(3));
	stop.store(true, Ordering::Release);
	driver.join().unwrap();
	completed.expect("a self-waking task starved real I/O or the timer");
}

fn socket_pair() -> (std::net::TcpStream, completion_async::StreamSocket) {
	let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
	let peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (stream, _) = listener.accept().unwrap();
	stream.set_nonblocking(true).unwrap();
	(peer, completion_async::StreamSocket::new(stream))
}

/// The deadline is fixed when the timer is constructed, not when it is first
/// polled: a timer held past its duration has already come due, so the
/// first poll finds it fired rather than starting the wait then.
#[test]
fn a_timeout_held_past_its_duration_fires_on_its_first_poll() {
	common::init_logging();
	let timeout = timeout(Duration::from_millis(5));
	std::thread::sleep(Duration::from_millis(30));
	let started = std::time::Instant::now();
	block_on(timeout).unwrap().unwrap();
	assert!(
		started.elapsed() < Duration::from_millis(5),
		"the wait restarted at the first poll instead of holding the deadline"
	);
}

#[test]
fn dropping_a_join_handle_detaches_the_task() {
	common::init_logging();
	let ran = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&ran);
	let handle = spawn(async move {
		timeout(Duration::from_millis(5)).await.unwrap();
		flag.store(true, Ordering::Release);
	});
	drop(handle);
	// Longer than the task's own timer, so a task left running has the time to
	// reach the end of its body.
	block_on(timeout(Duration::from_millis(30)))
		.unwrap()
		.unwrap();
	assert!(
		ran.load(Ordering::Acquire),
		"dropping the join handle cancelled its task"
	);
}

/// A detached task's output has no receiver, so the executor drops it — and it
/// does so where the task runs, never on whichever thread last let go of the
/// task's waker.
#[test]
fn a_detached_task_drops_its_output_on_the_owning_thread() {
	use std::sync::atomic::AtomicUsize;
	use std::thread::ThreadId;

	struct DropCount {
		drops: Arc<AtomicUsize>,
		owner: ThreadId,
		on_owner: Arc<AtomicBool>,
	}

	impl Drop for DropCount {
		fn drop(&mut self) {
			self.drops.fetch_add(1, Ordering::SeqCst);
			self.on_owner
				.store(std::thread::current().id() == self.owner, Ordering::SeqCst);
		}
	}

	common::init_logging();
	let owner = std::thread::current().id();
	let drops = Arc::new(AtomicUsize::new(0));
	let on_owner = Arc::new(AtomicBool::new(false));
	let produced = Arc::new(AtomicBool::new(false));
	let finished = Arc::clone(&produced);
	let task = spawn_local({
		let drops = Arc::clone(&drops);
		let on_owner = Arc::clone(&on_owner);
		async move {
			finished.store(true, Ordering::Release);
			DropCount {
				drops,
				owner,
				on_owner,
			}
		}
	});
	drop(task);
	// One pass over the loop runs the detached task to completion.
	block_on(async {}).unwrap();
	assert!(
		produced.load(Ordering::Acquire),
		"the detached task never ran"
	);
	assert_eq!(
		drops.load(Ordering::SeqCst),
		1,
		"the detached task's output was dropped more or less than once"
	);
	assert!(
		on_owner.load(Ordering::SeqCst),
		"the output was dropped on another thread"
	);
}

#[test]
fn a_task_can_drop_its_own_join_handle_while_finishing() {
	use std::cell::{Cell, RefCell};
	use std::rc::Rc;

	common::init_logging();
	let (_peer, socket) = socket_pair();
	let (buffer, live) = common::tracked(8);
	let payload = buffer.clone();
	drop(buffer);
	let mut receive = Box::pin(socket.recv(payload));
	let handle = Rc::new(RefCell::new(None::<completion_async::JoinHandle<()>>));
	let finish = Rc::new(Cell::new(false));
	let wake = Rc::new(RefCell::new(None::<Waker>));
	let own_handle = Rc::clone(&handle);
	let flag = Rc::clone(&finish);
	let task_wake = Rc::clone(&wake);
	*handle.borrow_mut() = Some(spawn_local(std::future::poll_fn(move |cx| {
		if flag.get() {
			drop(own_handle.borrow_mut().take());
			return Poll::Ready(());
		}
		*task_wake.borrow_mut() = Some(cx.waker().clone());
		assert!(receive.as_mut().poll(cx).is_pending());
		Poll::Pending
	})));
	block_on(timeout(Duration::from_millis(1)))
		.unwrap()
		.unwrap();
	finish.set(true);
	wake.borrow().as_ref().unwrap().wake_by_ref();
	// The task's own end drops its in-flight receive, which the reactor reaps on
	// a later pass.
	block_on(async {}).unwrap();
	block_on(timeout(Duration::from_millis(10)))
		.unwrap()
		.unwrap();
	assert!(!live.alive(), "the finished task kept its I/O buffer");
}

#[test]
fn a_panicking_task_releases_in_flight_io_and_leaves_the_executor_usable() {
	use std::cell::Cell;
	use std::rc::Rc;

	common::init_logging();
	let (_peer, socket) = socket_pair();
	let (buffer, live) = common::tracked(8);
	let payload = buffer.clone();
	drop(buffer);
	let mut receive = Box::pin(socket.recv(payload));
	let panic_next = Rc::new(Cell::new(false));
	let flag = Rc::clone(&panic_next);
	let (handoff, handed) = std::sync::mpsc::channel();
	let mut task = spawn_local(std::future::poll_fn(move |cx| {
		if flag.get() {
			panic!("task failed while waiting for I/O");
		}
		handoff.send(cx.waker().clone()).unwrap();
		assert!(receive.as_mut().poll(cx).is_pending());
		Poll::Pending::<()>
	}));
	block_on(timeout(Duration::from_millis(1)))
		.unwrap()
		.unwrap();
	panic_next.set(true);
	handed.try_recv().unwrap().wake();
	let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(async {})));
	let panic = caught.expect_err("a task panic must leave block_on");
	assert_eq!(
		panic.downcast_ref::<&str>(),
		Some(&"task failed while waiting for I/O")
	);
	// Unwinding dropped the job and released its I/O as a detached cancel; a
	// pass over the reactor reaps that cancel.
	block_on(timeout(Duration::from_millis(10)))
		.unwrap()
		.unwrap();
	assert!(!live.alive(), "unwinding kept the I/O buffer");
	let gone = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		std::pin::Pin::new(&mut task).poll(&mut Context::from_waker(Waker::noop()))
	}));
	assert!(gone.is_err(), "joining a failed task parked forever");
	let next = spawn_local(async { Rc::new(42) });
	assert_eq!(*block_on(next).unwrap(), 42);
}

/// A root future that is ready on its first poll returns there and then. A task
/// it detached before returning is queued only as a consequence of that poll,
/// so the task has not been polled once by the time the loop would return.
/// `block_on` has to make one closing pass before it hands the result back, or
/// the detached task waits for the next call to run at all.
#[test]
fn block_on_polls_the_root_future_before_a_task_queued_before_it() {
	use std::cell::Cell;
	use std::rc::Rc;

	common::init_logging();
	let ran = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&ran);
	let polled = Arc::clone(&ran);
	let task = spawn(async move { flag.store(true, Ordering::Release) });
	let seen_unpolled = Rc::new(Cell::new(false));
	let observed = Rc::clone(&seen_unpolled);
	let mut first = true;
	let mut task = std::pin::pin!(task);
	block_on(std::future::poll_fn(move |cx| {
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

/// A timeout fires once its deadline has passed, not before.
#[test]
fn a_timeout_fires_at_its_deadline() {
	common::init_logging();
	let started = Instant::now();
	block_on(timeout(Duration::from_millis(50)))
		.unwrap()
		.unwrap();
	let elapsed = started.elapsed();
	assert!(
		elapsed >= Duration::from_millis(50),
		"fired early: {elapsed:?}"
	);
	assert!(elapsed < Duration::from_secs(1), "fired late: {elapsed:?}");
}

/// A timer armed on a task does not hold up a nearer one: the wait is for the
/// nearer deadline, not the far one.
#[test]
fn a_task_can_spawn_a_child_and_await_it() {
	let parent = spawn(async {
		let child = spawn(async { 21 });
		child.await * 2
	});
	let result = block_on(parent);
	assert!(matches!(result, Ok(42)), "{result:?}");
}

#[test]
fn calling_block_on_inside_a_running_block_on_panics_as_nested() {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let _ = block_on(async {
			let _ = block_on(async {});
		});
	}));
	let payload = result.expect_err("a nested call must panic");
	let message = payload
		.downcast_ref::<&str>()
		.map(|s| s.to_string())
		.or_else(|| payload.downcast_ref::<String>().cloned())
		.unwrap_or_default();
	assert!(message.contains("nested"), "{message}");
}

#[test]
fn a_handle_made_inside_block_on_submits_to_the_global_executor() {
	common::init_logging();
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (mut server, _) = listener.accept().unwrap();

	let payload = Buffer::copy_from(b"hello");
	let sent = block_on(async move {
		let socket: StreamSocket = StreamSocket::new(client);
		socket.send_all(payload).await
	})
	.unwrap();
	assert_eq!(sent.into_result().unwrap(), 5);

	let mut got = [0u8; 5];
	server.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");
}

/// `StreamSocket::connect` reaches the thread's default executor on its own: the
/// connection is made there, and the stream it hands back runs on the same one.
#[test]
fn a_connect_made_inside_block_on_uses_the_global_executor() {
	common::init_logging();
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let addr = listener.local_addr().unwrap();

	let payload = Buffer::copy_from(b"hello");
	let sent = block_on(async move {
		let stream = StreamSocket::connect(addr)
			.await
			.expect("the connect failed");
		stream.send_all(payload).await
	})
	.unwrap();
	assert_eq!(sent.into_result().unwrap(), 5);

	let (mut peer, _) = listener.accept().unwrap();
	let mut got = [0u8; 5];
	peer.read_exact(&mut got).unwrap();
	assert_eq!(&got, b"hello");
}

#[test]
fn a_timeout_made_inside_a_spawned_task_fires_under_block_on() {
	common::init_logging();
	let start = Instant::now();
	let task = spawn(async { timeout(Duration::from_millis(20)).await });
	block_on(task).unwrap().unwrap();
	assert!(start.elapsed() >= Duration::from_millis(20));
}

/// Two spawned tasks that wait on each other finish only if they run at the
/// same time, so the pool must run them on two threads.
#[test]
fn spawned_tasks_run_in_parallel_on_the_worker_pool() {
	common::init_logging();
	let barrier = Arc::new(std::sync::Barrier::new(2));
	let (left, right) = block_on(async {
		let waiting = Arc::clone(&barrier);
		let left = spawn(async move {
			waiting.wait();
			20
		});
		let waiting = Arc::clone(&barrier);
		let right = spawn(async move {
			waiting.wait();
			22
		});
		(left.await, right.await)
	})
	.unwrap();
	assert_eq!(left + right, 42);
}

/// A spawned task does its I/O on the thread that polls it, and its handle
/// hands the result back to whichever thread awaits it.
#[test]
fn a_spawned_task_completes_across_threads() {
	common::init_logging();
	let (mut peer, socket) = socket_pair();
	peer.write_all(b"hello").unwrap();
	let task = spawn(async move {
		let mut buffer = Buffer::zeroed(5);
		socket
			.recv_exact(buffer.clone())
			.await
			.into_result()
			.unwrap();
		buffer.get_mut().unwrap().to_vec()
	});
	let received = std::thread::spawn(move || block_on(task).unwrap())
		.join()
		.unwrap();
	assert_eq!(received, b"hello");
}
