//! Memory handed to an operation: a space bound to the executor once, or lent
//! to a scope, and the span of it each operation names.
mod common;
use common::Buffer;

use std::io;
use std::sync::Arc;

use completion_async::{Config, ErrorKind, Executor, Facade, File, OpenOptions};

/// The async driver the file operations run on: it owns the reactor, so
/// `block_on` both polls the operation and drives the backend under it.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// An empty file in a scratch directory, open for reading and writing on
/// `executor`.
fn scratch(tag: &str, executor: &Executor) -> (common::Scratch, std::path::PathBuf, File) {
	let directory = common::tempdir(tag).unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let file = Facade::with_executor(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
		executor,
	);
	(directory, path, file)
}

#[test]
fn a_bound_space_moves_bytes_through_a_file() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-bind", &executor);
	let source = Arc::new(Buffer::from_slice(b"payload"));
	let sink = Arc::new(Buffer::new(7));
	let source_space = executor.bind(source);
	let sink_space = executor.bind(sink.clone());

	executor
		.block_on(file.write_all_at(source_space.whole(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(std::fs::read(&path).unwrap(), b"payload");

	executor
		.block_on(file.read_exact_at(sink_space.whole(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(sink.to_vec(), b"payload");
}

/// A span names ranges of its space; one past the end is refused when the
/// operation is submitted, not by a panic, and the memory is left alone.
#[test]
fn a_span_past_its_space_fails_as_invalid_input() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-range", &executor);
	std::fs::write(&path, b"hello").unwrap();
	let buffer = Arc::new(Buffer::new(4));
	let space = executor.bind(buffer.clone());

	let outcome = executor
		.block_on(file.read_at(space.span(2, 4), 0))
		.unwrap();

	assert_eq!(outcome.value, 0);
	match outcome.into_result() {
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::InvalidInput)
		}
		other => panic!("a span past its space was taken: {other:?}"),
	}
	assert_eq!(buffer.to_vec(), &[0u8; 4]);
}

/// A space belongs to the executor it was bound to: an operation on another
/// executor's facade refuses it.
#[test]
fn a_space_bound_to_another_executor_is_refused() {
	let mut executor = executor();
	let other = self::executor();
	let (_directory, path, file) = scratch("completion-memory-foreign", &executor);
	std::fs::write(&path, b"hello").unwrap();
	let buffer = Arc::new(Buffer::new(5));
	let space = other.bind(buffer.clone());

	let read = executor.block_on(file.read_at(space.whole(), 0)).unwrap();

	match read.into_result() {
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::InvalidInput)
		}
		other => panic!("a foreign executor took the space: {other:?}"),
	}
	assert_eq!(buffer.to_vec(), &[0u8; 5]);
}

/// A gathered span reads and writes its ranges in order, as if they were one.
#[test]
fn a_gathered_span_moves_both_ranges() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-gather", &executor);
	let source = Arc::new(Buffer::from_slice(b"head....tail"));
	let source = executor.bind(source);

	let written = executor
		.block_on(file.write_all_at(source.gather([(0, 4), (8, 4)]), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(written, 8);
	assert_eq!(std::fs::read(&path).unwrap(), b"headtail");

	let sink = Arc::new(Buffer::from_slice(&[b'.'; 12]));
	let space = executor.bind(sink.clone());
	let read = executor
		.block_on(file.read_exact_at(space.gather([(8, 4), (0, 4)]), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read, 8);
	assert_eq!(sink.to_vec(), b"tail....head");
}

/// A skipped span starts past the bytes it skips, and a fill through it stops
/// at its end.
#[test]
fn read_exact_at_fills_a_skipped_span() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-skip", &executor);
	std::fs::write(&path, b"world").unwrap();
	let sink = Arc::new(Buffer::from_slice(b"hello, ....."));
	let space = executor.bind(sink.clone());

	let read = executor
		.block_on(file.read_exact_at(space.whole().skip(7), 0))
		.unwrap()
		.into_result()
		.unwrap();

	assert_eq!(read, 5);
	assert_eq!(sink.to_vec(), b"hello, world");
}
