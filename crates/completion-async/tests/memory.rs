//! Memory handed to an operation: a [`Buffer`], the [`Span`] of it each
//! operation names, and the [`BufferPool`] that hands pre-faulted blocks back
//! for the next one.
mod common;

use std::io;
use std::sync::Arc;

use completion_async::{
	Buffer, BufferPool, Config, Error, ErrorKind, Executor, File, IoVec, Memory, OpenOptions,
	Result,
};

/// The async driver the file operations run on: it owns the reactor, so
/// `block_on` both polls the operation and drives the backend under it.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

/// An empty file in a scratch directory, open for reading and writing on
/// `executor`.
fn scratch(tag: &str) -> (common::Scratch, std::path::PathBuf, File) {
	let directory = common::tempdir(tag).unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);
	(directory, path, file)
}

/// A span names ranges of its buffer; one past the end is refused when the
/// operation is submitted, not by a panic, and the memory is left alone.
#[test]
fn a_span_past_its_buffer_fails_as_invalid_input() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-range");
	std::fs::write(&path, b"hello").unwrap();
	let buffer = Buffer::zeroed(4);

	let result = executor
		.block_on(file.read_at(buffer.span(2, 4), 0))
		.unwrap();
	match result {
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::InvalidInput)
		}
		other => panic!("a span past its buffer was taken: {other:?}"),
	}
	assert_eq!(
		buffer.whole().to_vec(),
		&[0u8; 4],
		"the memory was written to"
	);
}

/// A span at an offset moves only its own range: a write sends those bytes
/// alone, and a read fills that range and leaves the rest of the buffer alone.
#[test]
fn a_span_at_an_offset_moves_only_its_range() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-offset");
	let source = Buffer::from(b"..head..");

	let written = executor
		.block_on(file.write_all_at(source.span(2, 4), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(written, 4);
	assert_eq!(std::fs::read(&path).unwrap(), b"head");

	let sink = Buffer::from(&[b'.'; 8]);
	let read = executor
		.block_on(file.read_exact_at(sink.span(3, 4), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read, 4);
	assert_eq!(sink.whole().to_vec(), b"...head.");
}

/// A skipped span starts past the bytes it skips, and a fill through it stops
/// at its end.
#[test]
fn read_exact_at_fills_a_skipped_span() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-skip");
	std::fs::write(&path, b"world").unwrap();
	let sink = Buffer::from(b"hello, .....");

	let read = executor
		.block_on(file.read_exact_at(sink.whole().skip(7), 0))
		.unwrap()
		.into_result()
		.unwrap();

	assert_eq!(read, 5);
	assert_eq!(sink.whole().to_vec(), b"hello, world");
}

/// A pooled block carries a transfer like any other buffer, and comes back to
/// the pool once the operation and the caller are done with it.
#[test]
fn a_pooled_buffer_moves_bytes_through_a_file_and_returns_to_the_pool() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-pool");
	let pool = BufferPool::new(4096, 1);
	assert_eq!(pool.available(), 1);

	let buffer = pool.get();
	buffer.span(0, 6).copy_from_slice(b"pooled");
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
	assert_eq!(read.span(0, 6).to_vec(), b"pooled");
	let _ = path;
}

/// A [`Memory`] the test owns, to prove [`Buffer::new`] carries a custom
/// implementation through a real transfer.
struct Custom(Vec<u8>);

// SAFETY: the bytes outlive the memory and only reach the kernel through
// `resolve`, which refuses an extent that does not fit them.
unsafe impl Memory for Custom {
	fn resolve(&self, offset: usize, len: usize) -> Result<IoVec> {
		let fits = offset
			.checked_add(len)
			.is_some_and(|end| end <= self.0.len());
		if !fits {
			return Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput)));
		}
		// SAFETY: `offset..offset + len` is inside the vector, which the buffer
		// keeps alive.
		Ok(IoVec {
			ptr: unsafe { self.0.as_ptr().add(offset).cast_mut() },
			len,
		})
	}

	fn len(&self) -> usize {
		self.0.len()
	}
}

#[test]
fn a_custom_memory_reads_through_a_file() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-custom");
	std::fs::write(&path, b"custom").unwrap();
	let buffer = Buffer::new(Arc::new(Custom(vec![0; 6])));

	executor
		.block_on(file.read_exact_at(buffer.whole(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(buffer.whole().to_vec(), b"custom");
}

/// A whole buffer and a plain slice are both what a transfer names, so neither
/// needs an explicit `whole()` on the way in.
#[test]
fn a_transfer_takes_the_bytes_it_names() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-into");

	let written = executor
		.block_on(file.write_all_at(&b"payload"[..], 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(written, 7);
	assert_eq!(std::fs::read(&path).unwrap(), b"payload");

	let sink = Buffer::zeroed(7);
	let read = executor
		.block_on(file.read_exact_at(&sink, 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read, 7);
	assert_eq!(sink.whole().to_vec(), b"payload");

	let from_vec = Buffer::zeroed(7);
	executor
		.block_on(file.read_exact_at(from_vec.clone(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(from_vec.whole().to_vec(), b"payload");
}

/// A transfer named by a borrow owns the bytes it was handed: the borrow may go
/// before the operation is awaited.
#[test]
fn a_transfer_outlives_the_span_it_was_named_by() {
	let mut executor = executor();
	let (_directory, path, file) = scratch("completion-memory-outlive");

	let write = {
		let source = Buffer::from(b"payload");
		let write = file.write_all_at(&source, 0);
		// The borrow goes before the operation runs; the span it made keeps the
		// bytes alive.
		drop(source);
		write
	};
	executor.block_on(write).unwrap().into_result().unwrap();
	assert_eq!(std::fs::read(&path).unwrap(), b"payload");

	let write = {
		let payload = b"slice".to_vec();
		let write = file.write_all_at(&payload[..], 0);
		drop(payload);
		write
	};
	executor.block_on(write).unwrap().into_result().unwrap();
	assert_eq!(std::fs::read(&path).unwrap()[..5], b"slice"[..]);
}
