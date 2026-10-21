//! A [`Buffer`] names memory, a [`Span`] a range of it, and both read and write
//! bytes back through their own range — including a custom [`Memory`].

mod common;

use std::io;
use std::sync::Arc;

use completion_backend::{Buffer, BufferPool, Error, IoVec, Memory, Result, Span};

#[test]
fn a_span_names_the_range_it_was_taken_with() {
	let buffer = Buffer::zeroed(8);
	let span = buffer.span(2, 3);

	assert_eq!(span.buffer().len(), 8);
	assert_eq!(span.offset(), 2);
	assert_eq!(span.len(), 3);
}

#[test]
fn skip_and_take_cut_the_range_from_either_end() {
	let buffer = Buffer::zeroed(8);
	let span = buffer.span(1, 5);

	let skipped = span.clone().skip(3);
	assert_eq!((skipped.offset(), skipped.len()), (4, 2));

	let taken = span.take(4);
	assert_eq!((taken.offset(), taken.len()), (1, 4));

	let past_end = buffer.span(1, 5).skip(9);
	assert_eq!((past_end.offset(), past_end.len()), (6, 0));
}

#[test]
fn a_buffer_copies_bytes_in_and_out_of_its_range() {
	let buffer = Buffer::zeroed(8);
	assert_eq!(buffer.len(), 8);

	buffer.span(2, 3).copy_from_slice(b"abc");
	buffer.span(5, 3).copy_from_slice(b"xyz");
	let mut out = [0u8; 6];
	out.copy_from_slice(&buffer.whole()[2..8]);
	assert_eq!(&out, b"abcxyz");
	assert_eq!(buffer.whole().as_ref(), b"\0\0abcxyz");

	assert_eq!(Buffer::zeroed(0).len(), 0);
	assert!(Buffer::zeroed(0).whole().is_empty());
}

#[test]
#[should_panic(expected = "outside the buffer")]
fn a_span_past_its_buffer_panics_when_dereferenced() {
	let _ = &Buffer::zeroed(4).span(2, 3)[..];
}

#[test]
fn a_span_dereferences_relative_to_itself() {
	let buffer = Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7]);
	let mut span = buffer.span(2, 4);
	assert_eq!(span.as_ref(), &[2, 3, 4, 5]);
	assert_eq!(&span[1..4], &[3, 4, 5]);

	span.as_mut()[1..4].copy_from_slice(&[9, 8, 7]);
	assert_eq!(buffer.whole().as_ref(), &[0, 1, 2, 9, 8, 7, 6, 7]);
	assert_eq!(span.as_ref(), &[2, 9, 8, 7]);

	// A sub-span keeps its bytes relative to itself too: `skip(4)` starts
	// four bytes into the buffer.
	let mut tail = buffer.whole().skip(4).take(3);
	assert_eq!(tail.as_ref(), &[8, 7, 6]);
	tail[..2].copy_from_slice(b"ab");
	assert_eq!(tail.as_ref(), &[b'a', b'b', 6]);
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
}

#[test]
fn a_buffer_wraps_a_custom_memory() {
	let buffer = Buffer::new(Arc::new(Custom(vec![1, 2, 3, 4])));
	assert_eq!(buffer.len(), 4);
	assert_eq!(buffer.whole().as_ref(), &[1, 2, 3, 4]);

	buffer.span(1, 2).copy_from_slice(&[9, 9]);
	assert_eq!(buffer.whole().as_ref(), &[1, 9, 9, 4]);
	let mut out = [0u8; 2];
	out.copy_from_slice(&buffer.span(1, 2));
	assert_eq!(out, [9, 9]);
}

#[test]
fn a_pooled_buffer_backs_a_span_like_any_other() {
	let pool = BufferPool::new(4096, 2);
	let buffer = pool.get();
	buffer.span(0, 6).copy_from_slice(b"pooled");
	let span = buffer.span(0, 6);
	assert_eq!(span.as_ref(), b"pooled");
	assert_eq!(span.buffer().len(), 4096);
}

#[test]
fn a_buffer_is_a_memory() {
	let memory: Arc<dyn Memory> = Arc::new(Buffer::from(b"hello"));
	assert_eq!(memory.len(), 5);

	let ptr = memory.resolve(1, 3).unwrap().ptr;
	let mut out = [0u8; 3];
	// SAFETY: `resolve` handed out memory inside `memory`, which stays alive for
	// this copy.
	unsafe { std::ptr::copy_nonoverlapping(ptr, out.as_mut_ptr(), out.len()) };
	assert_eq!(&out, b"ell");

	assert_eq!(
		io::Error::from(memory.resolve(4, 2).unwrap_err()).kind(),
		io::ErrorKind::InvalidInput
	);
}

/// A `Span` is what every transfer names, so a whole buffer and the byte
/// containers around it convert into one without an explicit `whole()`.
#[test]
fn a_span_converts_from_a_whole_buffer() {
	let buffer = Buffer::from(b"hello");

	let owned = Span::from(buffer.clone());
	assert_eq!((owned.offset(), owned.len()), (0, 5));
	assert_eq!(owned.to_vec(), b"hello");

	let borrowed = Span::from(&buffer);
	assert_eq!((borrowed.offset(), borrowed.len()), (0, 5));
	assert_eq!(borrowed.to_vec(), b"hello");

	let from_vec = Span::from(b"world".to_vec());
	assert_eq!((from_vec.offset(), from_vec.len()), (0, 5));
	assert_eq!(from_vec.to_vec(), b"world");

	let from_box: Box<[u8]> = b"boxed".to_vec().into_boxed_slice();
	let from_box = Span::from(from_box);
	assert_eq!(from_box.to_vec(), b"boxed");

	let from_slice = Span::from(&b"slice"[..]);
	assert_eq!(from_slice.to_vec(), b"slice");
}
