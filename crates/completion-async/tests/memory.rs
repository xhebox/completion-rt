//! Memory handed to a file operation: an owned buffer needs no `Arc`, and a
//! shared one is passed as a clone of its `Arc`.
mod common;

use std::sync::Arc;

use completion_async::{Config, Executor, Facade, File, OpenOptions};

/// The async driver the file operations run on: it owns the reactor, so
/// `block_on` both polls the operation and drives the backend under it.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

#[test]
fn an_owned_buffer_and_a_shared_one_both_move_through_a_file() {
	let directory = common::tempdir("completion-memory-by-value").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
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

	// The operation takes the Vec itself: nothing to wrap, nothing to clone.
	executor
		.block_on(file.write_all_at(b"payload".to_vec(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(std::fs::read(&path).unwrap(), b"payload");

	// The caller keeps its handle on the buffer by passing an Arc clone.
	let sink = Arc::new(vec![0u8; 7]);
	executor
		.block_on(file.read_exact_at(Arc::clone(&sink), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(&sink[..], b"payload");
}
