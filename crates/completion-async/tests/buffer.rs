//! A [`Buffer`] names a range of [`Memory`], a slice of one a narrower range,
//! and both hand bytes back through their own range — including a buffer over
//! a custom [`Memory`].

mod common;

use std::io;
use std::sync::Arc;

use completion_async::{Buffer, Error, IoVec, Memory, Result};

#[test]
fn a_slice_names_the_range_it_was_taken_with() {
	let buffer = Buffer::zeroed(8);
	let slice = buffer.slice(2, 3);

	assert_eq!(buffer.len(), 8);
	assert_eq!(slice.offset(), 2);
	assert_eq!(slice.len(), 3);
}

#[test]
fn skip_and_take_cut_the_range_from_either_end() {
	let buffer = Buffer::zeroed(8);
	let view = buffer.slice(1, 5);

	let skipped = view.clone().skip(3);
	assert_eq!((skipped.offset(), skipped.len()), (4, 2));

	let taken = view.take(4);
	assert_eq!((taken.offset(), taken.len()), (1, 4));

	let past_end = buffer.slice(1, 5).skip(9);
	assert_eq!((past_end.offset(), past_end.len()), (6, 0));
}

#[test]
fn a_buffer_hands_out_its_bytes_while_nothing_else_refers_to_them() {
	let mut buffer = Buffer::zeroed(8);
	assert_eq!(buffer.len(), 8);

	let bytes = buffer.get_mut().unwrap();
	bytes[2..5].copy_from_slice(b"abc");
	bytes[5..8].copy_from_slice(b"xyz");
	assert_eq!(buffer.get_mut().unwrap(), b"\0\0abcxyz");

	let slice = buffer.slice(0, 1);
	assert!(
		buffer.get_mut().is_none(),
		"a slice still refers to the bytes"
	);
	drop(slice);
	assert!(buffer.get_mut().is_some());

	assert_eq!(Buffer::zeroed(0).get_mut().unwrap(), b"");
}

#[test]
fn a_slice_past_its_buffer_hands_out_nothing() {
	assert!(Buffer::zeroed(4).slice(2, 3).get_mut().is_none());
}

#[test]
fn a_slice_hands_out_the_bytes_of_its_own_range() {
	let mut view = Buffer::copy_from(&[0, 1, 2, 3, 4, 5, 6, 7]).slice(2, 4);
	assert_eq!(view.get_mut().unwrap(), &[2, 3, 4, 5]);

	view.get_mut().unwrap()[1..4].copy_from_slice(&[9, 8, 7]);
	assert_eq!(view.get_mut().unwrap(), &[2, 9, 8, 7]);

	// A sub-slice keeps its bytes relative to itself too: `skip(2)` starts
	// four bytes into the buffer.
	let mut tail = view.skip(2).take(1);
	assert_eq!(tail.get_mut().unwrap(), &[8]);
}

/// A [`Memory`] the test owns: enough to prove [`Buffer::new`] works for a
/// custom implementation, not just the built-in bytes.
struct Custom(Vec<u8>);

// SAFETY: the bytes outlive the memory, and a range that does not fit them
// is refused.
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
fn a_buffer_wraps_a_custom_memory() {
	let mut buffer = Buffer::new(Arc::new(Custom(vec![1, 2, 3, 4])));
	assert_eq!(buffer.len(), 4);
	assert_eq!(buffer.get_mut().unwrap(), &[1, 2, 3, 4]);

	buffer.get_mut().unwrap()[1..3].copy_from_slice(&[9, 9]);
	assert_eq!(buffer.get_mut().unwrap(), &[1, 9, 9, 4]);
}

/// A whole buffer needs no conversion: a `Vec<u8>` and a `Box<[u8]>` are one,
/// and a clone names the same bytes.
#[test]
fn a_buffer_is_the_whole_range_of_its_memory() {
	let buffer = Buffer::copy_from(b"hello");
	assert_eq!((buffer.offset(), buffer.len()), (0, 5));
	let mut cloned = buffer.clone();
	drop(buffer);
	assert_eq!(cloned.get_mut().unwrap(), b"hello");

	let mut from_vec = Buffer::from(b"world".to_vec());
	assert_eq!((from_vec.offset(), from_vec.len()), (0, 5));
	assert_eq!(from_vec.get_mut().unwrap(), b"world");

	let from_box: Box<[u8]> = b"boxed".to_vec().into_boxed_slice();
	let mut from_box = Buffer::from(from_box);
	assert_eq!(from_box.get_mut().unwrap(), b"boxed");
}
