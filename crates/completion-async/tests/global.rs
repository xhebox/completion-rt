//! The thread's global executor, as seen from outside the crate.
//!
//! The harness runs each test on its own thread, so every test starts with a
//! fresh executor.

use std::panic::{AssertUnwindSafe, catch_unwind};

use completion_async::{block_on, spawn, try_spawn};

#[test]
fn a_task_spawned_with_spawn_is_run_by_block_on() {
	let task = spawn(async { 7 });
	let result = block_on(task);
	assert!(matches!(result, Ok(7)), "{result:?}");
}

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
fn a_task_spawned_with_try_spawn_is_run_by_block_on() {
	let task = try_spawn(async { 7 }).expect("the global executor is open");
	let result = block_on(task);
	assert!(matches!(result, Ok(7)), "{result:?}");
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
