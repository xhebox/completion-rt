//! A [`Buffer`] shares its bytes between clones, hands them out by exclusive
//! access, and names them as an [`AsIoVec`].

mod common;

use completion_async::{AsIoVec, Buffer};

#[test]
fn a_buffer_hands_out_its_bytes() {
	let mut buffer = Buffer::zeroed(8);
	assert_eq!(buffer.len(), 8);

	let bytes = buffer.get_mut().unwrap();
	bytes[2..5].copy_from_slice(b"abc");
	bytes[5..8].copy_from_slice(b"xyz");
	assert_eq!(buffer.get_mut().unwrap(), b"\0\0abcxyz");

	assert_eq!(Buffer::zeroed(0).get_mut().unwrap(), b"");
}

#[test]
fn a_clone_names_the_same_bytes() {
	let mut buffer = Buffer::copy_from(b"hello");
	let clone = buffer.clone();
	assert!(
		buffer.get_mut().is_none(),
		"the clone still names the bytes"
	);
	drop(clone);

	buffer.get_mut().unwrap()[1..3].copy_from_slice(&[9, 9]);
	let clone = buffer.clone();
	drop(buffer);
	assert_eq!(clone.clone().get_mut(), None);
	let mut clone = clone;
	assert_eq!(clone.get_mut().unwrap(), b"h\x09\x09lo");
}

#[test]
fn a_buffer_is_the_bytes_it_was_built_from() {
	let mut from_vec = Buffer::from(b"world".to_vec());
	assert_eq!(from_vec.len(), 5);
	assert_eq!(from_vec.get_mut().unwrap(), b"world");

	let from_box: Box<[u8]> = b"boxed".to_vec().into_boxed_slice();
	let mut from_box = Buffer::from(from_box);
	assert_eq!(from_box.get_mut().unwrap(), b"boxed");
}

#[test]
fn a_buffer_names_its_whole_range_as_an_iovec() {
	let mut buffer = Buffer::copy_from(b"hello");
	let iovec = buffer.as_iovec();
	assert_eq!(iovec.len, buffer.len());
	assert_eq!(iovec.ptr, buffer.get_mut().unwrap().as_ptr().cast_mut());
}
