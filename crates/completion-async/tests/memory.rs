//! Memory handed to an operation: a [`Buffer`] names the range each operation
//! transfers through.
mod common;

use std::io;
use std::sync::Arc;

use completion_async::{
	Buffer, Error, ErrorKind, File, IoVec, Memory, OpenOptions, Result, block_on,
};

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

/// A buffer names a range; one past the end is refused when the operation is
/// submitted, not by a panic, and the memory is left alone.
#[test]
fn a_buffer_past_its_range_fails_as_invalid_input() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-range");
	std::fs::write(&path, b"hello").unwrap();
	let mut buffer = Buffer::zeroed(4);

	let result = block_on(file.read_at(buffer.slice(2, 4), 0)).unwrap();
	match result {
		Err(error) if error.kind() == ErrorKind::Failed => {
			assert_eq!(
				io::Error::try_from(error).unwrap().kind(),
				io::ErrorKind::InvalidInput
			)
		}
		other => panic!("a buffer past its range was taken: {other:?}"),
	}
	assert_eq!(
		buffer.get_mut().unwrap().to_vec(),
		&[0u8; 4],
		"the memory was written to"
	);
}

/// A buffer at an offset moves only its own range: a write sends those bytes
/// alone, and a read fills that range and leaves the rest of the buffer alone.
#[test]
fn a_buffer_at_an_offset_moves_only_its_range() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-offset");
	let source = Buffer::copy_from(b"..head..");

	let written = block_on(file.write_all_at(source.slice(2, 4), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(written, 4);
	assert_eq!(std::fs::read(&path).unwrap(), b"head");

	let mut sink = Buffer::copy_from(&[b'.'; 8]);
	let read = block_on(file.read_exact_at(sink.slice(3, 4), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read, 4);
	assert_eq!(sink.get_mut().unwrap().to_vec(), b"...head.");
}

/// A skipped view starts past the bytes it skips, and a fill through it stops
/// at its end.
#[test]
fn read_exact_at_fills_a_skipped_view() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-skip");
	std::fs::write(&path, b"world").unwrap();
	let mut sink = Buffer::copy_from(b"hello, .....");

	let read = block_on(file.read_exact_at(sink.clone().skip(7), 0))
		.unwrap()
		.into_result()
		.unwrap();

	assert_eq!(read, 5);
	assert_eq!(sink.get_mut().unwrap().to_vec(), b"hello, world");
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

	fn get_mut(&mut self, offset: usize, len: usize) -> Option<&mut [u8]> {
		self.0.get_mut(offset..offset.checked_add(len)?)
	}
}

#[test]
fn a_custom_memory_reads_through_a_file() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-custom");
	std::fs::write(&path, b"custom").unwrap();
	let mut buffer = Buffer::new(Arc::new(Custom(vec![0; 6])));

	block_on(file.read_exact_at(buffer.clone(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(buffer.get_mut().unwrap().to_vec(), b"custom");
}

/// A buffer is what a transfer names, whole or sliced, and a clone keeps the
/// bytes alive for it.
#[test]
fn a_transfer_takes_the_bytes_it_names() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-into");

	let written = block_on(file.write_all_at(Buffer::copy_from(b"payload"), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(written, 7);
	assert_eq!(std::fs::read(&path).unwrap(), b"payload");

	let mut sink = Buffer::zeroed(7);
	let read = block_on(file.read_exact_at(sink.clone(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read, 7);
	assert_eq!(sink.get_mut().unwrap().to_vec(), b"payload");

	let mut from_vec = Buffer::zeroed(7);
	block_on(file.read_exact_at(from_vec.clone(), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(from_vec.get_mut().unwrap().to_vec(), b"payload");
}

/// A transfer holds a handle on the bytes it was handed: the caller's buffer may
/// go before the operation is awaited.
#[test]
fn a_transfer_outlives_the_buffer_it_was_named_by() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-outlive");

	let write = {
		let source = Buffer::copy_from(b"payload");
		let write = file.write_all_at(source.clone(), 0);
		// The caller's handle goes before the operation runs; the transfer's own
		// clone keeps the bytes alive.
		drop(source);
		write
	};
	block_on(write).unwrap().into_result().unwrap();
	assert_eq!(std::fs::read(&path).unwrap(), b"payload");
}
