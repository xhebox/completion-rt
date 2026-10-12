//! Scopes: tasks that hold a borrow instead of a shared owner.
mod common;

use std::future::Future as _;
use std::io::{self, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use completion_async::{Config, ErrorKind, Executor, Facade, File, OpenOptions, Timeout};

fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// The shape a hypervisor needs: the tasks read memory that outlives the scope,
/// which `spawn` cannot express — its tasks are `'static` and would have to own
/// it.
///
/// What a task may borrow is memory outside the scope call. Memory owned by the
/// body itself is out: the tasks live beside that future, not inside it.
#[test]
fn a_scoped_task_borrows_memory_that_outlives_the_scope() {
	let mut executor = executor();
	let payload = vec![1u8, 2, 3, 4];
	let borrow = &payload;
	let total = executor
		.scope(|scope| async move {
			let sum = scope.spawn(async move { borrow.iter().map(|b| *b as usize).sum::<usize>() });
			let len = scope.spawn(async move { borrow.len() });
			sum.await + len.await
		})
		.unwrap();
	assert_eq!(total, 14);
}

/// A transfer submitted from a scoped task runs like any other: the scope drives
/// the task, the reactor drives the operation.
#[test]
fn a_scoped_task_runs_a_transfer() {
	let directory = common::tempdir("completion-scope-io").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let mut executor = executor();
	let submitter = executor.submitter();
	let file: File = Facade::with_submitter(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
		&submitter,
	);
	let buffer = Arc::new(vec![0u8; 5]);
	let into = Arc::clone(&buffer);

	let read = executor
		.scope(move |scope| async move {
			let task = scope.spawn(async move { file.read_exact_at(into, 0).await.into_result() });
			task.await
		})
		.unwrap();

	assert_eq!(read.unwrap(), 5);
	assert_eq!(&buffer[..], b"hello");
}

/// A handle its caller never awaited does not stop the task, and the scope is
/// what ends it: the borrow it holds is released before the scope returns.
#[test]
fn the_scope_drops_the_tasks_still_in_flight() {
	struct Probe(Arc<AtomicBool>);

	impl Drop for Probe {
		fn drop(&mut self) {
			self.0.store(true, Ordering::Release);
		}
	}

	let mut executor = executor();
	let dropped = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&dropped);
	executor
		.scope(move |scope| {
			drop(scope.spawn(async move {
				let _probe = Probe(flag);
				std::future::pending::<()>().await;
			}));
			async {}
		})
		.unwrap();
	assert!(
		dropped.load(Ordering::Acquire),
		"the scope returned with a task still holding its borrow"
	);
}

/// The hypervisor case end to end: the guest's memory is borrowed, not moved
/// into an `Arc`, and the kernel reads into it.
#[test]
fn a_transfer_reads_into_lent_memory() {
	let directory = common::tempdir("completion-scope-lend").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let mut executor = executor();
	let file: File = Facade::with_submitter(common::open(&path).unwrap(), &executor.submitter());
	let guest = vec![0u8; 5];
	let lent = &guest;

	let read = executor
		.scope(|scope| async move { file.read_exact_at(scope.lend(lent), 0).await.into_result() })
		.unwrap();

	assert_eq!(read.unwrap(), 5);
	assert_eq!(&guest[..], b"hello");
}

/// A forgotten transfer never runs its destructor, so nothing on the future's
/// side waits for the kernel. The scope's exit has to: otherwise the receive
/// stays armed on the borrow, and bytes arriving after the scope land in memory
/// the caller has taken back.
#[test]
fn the_scope_waits_out_a_forgotten_transfer_on_lent_memory() {
	let (left, mut right) = pair();
	let mut executor = executor();
	let socket: Facade<TcpStream> = Facade::with_submitter(left, &executor.submitter());
	let guest = vec![0u8; 5];
	let lent = &guest;

	executor
		.scope(|scope| async move {
			let mut receive = Box::pin(socket.recv(scope.lend(lent)));
			// One poll submits it; nothing has arrived, so it stays in flight.
			std::future::poll_fn(|cx| {
				assert!(receive.as_mut().poll(cx).is_pending());
				Poll::Ready(())
			})
			.await;
			std::mem::forget(receive);
		})
		.unwrap();

	right.write_all(b"late!").unwrap();
	// Drive the reactor long enough for an armed receive to complete.
	let timers = executor.timers();
	executor
		.block_on(Timeout::with_timers(Duration::from_millis(100), &timers))
		.unwrap()
		.unwrap();
	assert_eq!(
		&guest[..],
		&[0; 5],
		"the kernel wrote after the scope returned"
	);
}

/// The barrier drives only its own reactor, so lent memory submitted to another
/// would escape it: the submission is refused.
#[test]
fn lent_memory_is_refused_by_another_reactor() {
	let directory = common::tempdir("completion-scope-foreign").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let mut executor = executor();
	let other = self::executor();
	let file: File = Facade::with_submitter(common::open(&path).unwrap(), &other.submitter());
	let guest = vec![0u8; 5];
	let lent = &guest;

	let read = executor
		.scope(|scope| async move { file.read_at(scope.lend(lent), 0).await.into_result() })
		.unwrap();

	match read {
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::InvalidInput)
		}
		other => panic!("a foreign reactor took lent memory: {other:?}"),
	}
}

fn pair() -> (TcpStream, TcpStream) {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (server, _) = listener.accept().unwrap();
	(client, server)
}
