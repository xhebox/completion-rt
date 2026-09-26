//! A task set's end: the cancel it fires and the wait that follows.
//!
//! `join` is private, so the wait the tests care about is reached through
//! [`Tasks::shutdown`], which is the cancel-and-wait pair a device stop is.

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use completion_rt::{Cancel, Config, Executor, Tasks};

fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

#[test]
fn shutdown_fires_the_cancel_its_tasks_end_on_before_waiting() {
	let mut executor = executor();
	let tasks = Tasks::new(&executor.spawner());
	let cancel = Cancel::new();
	let finished = Rc::new(Cell::new(false));
	let task_cancel = cancel.clone();
	let task_finished = Rc::clone(&finished);
	tasks.spawn(async move {
		task_cancel.wait().await;
		task_finished.set(true);
	});
	executor.block_on(tasks.shutdown(&cancel)).unwrap();
	assert!(finished.get(), "the shutdown waited the task out");
}

#[test]
fn join_runs_a_task_the_cancel_reached_before_its_first_poll() {
	let (done, joined) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let mut executor = executor();
		let tasks = Tasks::new(&executor.spawner());
		let cancel = Cancel::new();
		let ran = Rc::new(Cell::new(false));
		let task_cancel = cancel.clone();
		let task_ran = Rc::clone(&ran);
		let spawner = tasks.clone();
		executor
			.block_on(async move {
				// The task is spawned, and the cancel fires, before the
				// executor ever polls it: it has no waker anywhere, so the
				// wake-up the wait raises is the only one there is.
				spawner.spawn(async move {
					task_cancel.wait().await;
					task_ran.set(true);
				});
				spawner.shutdown(&cancel).await;
			})
			.unwrap();
		let _ = done.send(ran.get());
	});
	let ran = joined
		.recv_timeout(Duration::from_secs(10))
		.expect("the shutdown ran the task it was handed");
	assert!(ran, "the shutdown waited the task out");
}

/// The wait reaches past the tasks the set held when it started: a task that
/// spawns its successor as the cancel reaches it hands the wait a task it did
/// not have, and the successor is left running if the wait returns without it.
#[test]
fn shutdown_waits_for_a_task_spawned_while_it_waits() {
	let (done, joined) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let mut executor = executor();
		let tasks = Tasks::new(&executor.spawner());
		let cancel = Cancel::new();
		let submitter = executor.submitter();
		let finished = Rc::new(Cell::new(false));

		// Ends on the cancel, handing the set a successor whose own end is a
		// reactor timer: only a wait that loops back over the set outlasts it.
		let successor_set = tasks.clone();
		let successor_finished = Rc::clone(&finished);
		let task_cancel = cancel.clone();
		tasks.spawn(async move {
			task_cancel.wait().await;
			successor_set.spawn(async move {
				let ((), result) = submitter
					.timeout(Duration::from_millis(50))
					.expect("the reactor arms the timer")
					.await;
				result.expect("the timer completed");
				successor_finished.set(true);
			});
		});

		executor.block_on(tasks.shutdown(&cancel)).unwrap();
		let _ = done.send(finished.get());
	});
	let finished = joined
		.recv_timeout(Duration::from_secs(10))
		.expect("the shutdown returned");
	assert!(
		finished,
		"the shutdown returned while the successor it spawned was still running"
	);
}
