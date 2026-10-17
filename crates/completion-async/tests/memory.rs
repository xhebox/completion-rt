//! Memory handed to an operation: a [`Buffer`], the [`Span`] of it each
//! operation names, and the [`BufferPool`] that hands pre-faulted blocks back
//! for the next one.
mod common;

use std::io;
use std::sync::Arc;

use completion_async::{
	Buffer, BufferPool, Config, ErrorKind, Executor, Extent, Facade, File, IoVec, Memory,
	OpenOptions,
};

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
fn a_bound_buffer_moves_bytes_through_a_file() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-bind", &executor);
	let source = Buffer::from_slice(b"payload");
	let sink = Buffer::zeroed(7);

	executor
		.block_on(file.write_all_at(source.whole(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(std::fs::read(&path).unwrap(), b"payload");

	executor
		.block_on(file.read_exact_at(sink.whole(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(sink.to_vec(), b"payload");
}

/// A span names ranges of its buffer; one past the end is refused when the
/// operation is submitted, not by a panic, and the memory is left alone.
#[test]
fn a_span_past_its_buffer_fails_as_invalid_input() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-range", &executor);
	std::fs::write(&path, b"hello").unwrap();
	let buffer = Buffer::zeroed(4);

	let outcome = executor
		.block_on(file.read_at(buffer.span(2, 4), 0))
		.unwrap();

	assert_eq!(outcome.value, 0);
	match outcome.into_result() {
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::InvalidInput)
		}
		other => panic!("a span past its buffer was taken: {other:?}"),
	}
	assert_eq!(buffer.to_vec(), &[0u8; 4]);
}

/// A gathered span reads and writes its ranges in order, as if they were one.
#[test]
fn a_gathered_span_moves_both_ranges() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-gather", &executor);
	let source = Buffer::from_slice(b"head....tail");

	let written = executor
		.block_on(file.write_all_at(source.gather([(0, 4), (8, 4)]), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(written, 8);
	assert_eq!(std::fs::read(&path).unwrap(), b"headtail");

	let sink = Buffer::from_slice(&[b'.'; 12]);
	let read = executor
		.block_on(file.read_exact_at(sink.gather([(8, 4), (0, 4)]), 0))
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
	let sink = Buffer::from_slice(b"hello, .....");

	let read = executor
		.block_on(file.read_exact_at(sink.whole().skip(7), 0))
		.unwrap()
		.into_result()
		.unwrap();

	assert_eq!(read, 5);
	assert_eq!(sink.to_vec(), b"hello, world");
}

/// A pooled block carries a transfer like any other buffer, and comes back to
/// the pool once the operation and the caller are done with it.
#[test]
fn a_pooled_buffer_moves_bytes_through_a_file_and_returns_to_the_pool() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-pool", &executor);
	let pool = BufferPool::new(4096, 1);
	assert_eq!(pool.available(), 1);

	let buffer = pool.get();
	buffer.write(0, b"pooled");
	let written = executor
		.block_on(file.write_all_at(buffer.span(0, 6), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(written, 6);
	assert_eq!(pool.available(), 0, "the checked-out block is out");
	drop(buffer);
	assert_eq!(pool.available(), 1, "the block went back to the pool");

	let read = pool.get();
	executor
		.block_on(file.read_exact_at(read.span(0, 6), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read.slice_to_vec(0..6), b"pooled");
	let _ = path;
}

/// A [`Memory`] the test owns, to prove [`Buffer::new`] carries a custom
/// implementation through a real transfer.
struct Custom(Vec<u8>);

// SAFETY: the bytes outlive the memory and only reach the kernel through
// `iovec`, which refuses an extent that does not fit them.
unsafe impl Memory for Custom {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec> {
		let start = usize::try_from(extent.offset)
			.map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
		let end = start
			.checked_add(extent.len)
			.filter(|&end| end <= self.0.len())
			.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
		// SAFETY: `start..end` is inside the vector, which the buffer keeps alive.
		Ok(unsafe { IoVec::new(self.0.as_ptr().add(start).cast_mut(), end - start) })
	}

	fn len(&self) -> usize {
		self.0.len()
	}
}

#[test]
fn a_custom_memory_reads_through_a_file() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-custom", &executor);
	std::fs::write(&path, b"custom").unwrap();
	let buffer = Buffer::new(Arc::new(Custom(vec![0; 6])));

	executor
		.block_on(file.read_exact_at(buffer.whole(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(buffer.to_vec(), b"custom");
}
