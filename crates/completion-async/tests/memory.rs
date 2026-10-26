//! Memory handed to an operation: a [`Buffer`] shares its bytes between clones,
//! and a custom [`AsIoVec`] names a range inside memory the operation does not
//! own.
mod common;

use assert2::assert;
use std::sync::Arc;

use completion_async::{AsIoVec, Buffer, File, IoVec, OpenOptions, block_on};

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

/// Memory the test owns: the operation reaches the range `offset..offset + len`
/// inside `bytes`, which stays put for as long as any clone is alive.
#[derive(Clone)]
struct Middle {
	bytes: Arc<Vec<u8>>,
	offset: usize,
	len: usize,
}

impl Middle {
	fn range(&self) -> &[u8] {
		&self.bytes[self.offset..self.offset + self.len]
	}
}

// SAFETY: the `Arc` keeps `bytes` alive, and `offset..offset + len` is inside
// it.
unsafe impl AsIoVec for Middle {
	fn as_iovec(&self) -> IoVec {
		IoVec {
			ptr: unsafe { self.bytes.as_ptr().add(self.offset).cast_mut() },
			len: self.len,
		}
	}
}

/// A custom [`AsIoVec`] reads into and writes from its own range, not the whole
/// allocation it points into.
#[test]
fn a_custom_as_iovec_reads_and_writes_through_its_range() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-custom");
	std::fs::write(&path, b"abcdefgh").unwrap();

	let sink = Middle {
		bytes: Arc::new(vec![b'.'; 10]),
		offset: 3,
		len: 4,
	};
	block_on(file.read_exact_at(sink.clone(), 0))
		.unwrap()
		.unwrap();
	assert!(sink.range() == b"abcd");
	assert!(sink.bytes.as_slice() == b"...abcd...");

	let source = Middle {
		bytes: Arc::new(b"..WXYZ..".to_vec()),
		offset: 2,
		len: 4,
	};
	block_on(file.write_all_at(source.clone(), 4))
		.unwrap()
		.unwrap();
	assert!(std::fs::read(&path).unwrap() == b"abcdWXYZ");
}

/// A clone of a buffer is a handle on the same bytes: passing it to an
/// operation and keeping the original reads the bytes back once it lands.
#[test]
fn a_shared_buffer_reads_and_writes() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-shared");
	std::fs::write(&path, b"shared").unwrap();

	let source = Buffer::copy_from(b"shared");
	block_on(file.write_all_at(source.clone(), 0))
		.unwrap()
		.unwrap();

	let mut sink = Buffer::zeroed(6);
	block_on(file.read_exact_at(sink.clone(), 0))
		.unwrap()
		.unwrap();
	assert!(sink.get_mut().unwrap() == b"shared");
}

/// The bytes are exclusive only while no other handle names them: another clone
/// or an in-flight operation refuses `get_mut`, and completion gives it back.
#[test]
fn a_buffer_is_exclusive_only_once_nothing_else_names_it() {
	common::init_logging();
	let (_directory, path, file) = scratch("completion-memory-exclusive");
	std::fs::write(&path, b"hello").unwrap();

	let mut buffer = Buffer::zeroed(5);
	let read = file.read_at(buffer.clone(), 0);
	assert!(
		buffer.get_mut().is_none(),
		"the in-flight operation still names the bytes"
	);
	block_on(read).unwrap().unwrap();
	assert!(buffer.get_mut().unwrap() == b"hello");

	let clone = buffer.clone();
	assert!(
		buffer.get_mut().is_none(),
		"another handle still names the bytes"
	);
	drop(clone);
	assert!(buffer.get_mut().is_some());
}

/// A transfer holds the bytes it was handed: the caller's handle may go before
/// the operation is awaited.
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
	block_on(write).unwrap().unwrap();
	assert!(std::fs::read(&path).unwrap() == b"payload");
}
