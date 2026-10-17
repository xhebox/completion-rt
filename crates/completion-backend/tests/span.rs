//! A [`Buffer`] names memory, a [`Span`] ranges of it, and both read and write
//! bytes back through their own extents — including a custom [`Memory`].

mod common;

use std::io;
use std::sync::Arc;

use completion_backend::{Buffer, BufferPool, Extent, IoVec, Memory};

fn extents(ranges: &[(u64, usize)]) -> Vec<Extent> {
	ranges
		.iter()
		.map(|&(offset, len)| Extent { offset, len })
		.collect()
}

#[test]
fn a_span_names_the_extents_it_was_taken_with() {
	let buffer = Buffer::zeroed(8);
	let span = buffer.gather([Extent { offset: 0, len: 2 }, Extent { offset: 4, len: 3 }]);

	assert_eq!(span.buffer().len(), 8);
	assert_eq!(span.extents(), &extents(&[(0, 2), (4, 3)]));
	assert_eq!(span.len(), 5);
}

#[test]
fn skip_and_take_cut_the_extents_in_order() {
	let buffer = Buffer::zeroed(8);
	let span = buffer.gather([Extent { offset: 0, len: 2 }, Extent { offset: 4, len: 3 }]);

	assert_eq!(span.clone().skip(3).extents(), &extents(&[(5, 2)]));
	assert_eq!(span.take(4).extents(), &extents(&[(0, 2), (4, 2)]));
}

#[test]
fn whole_covers_the_buffer_as_one_extent() {
	let buffer = Buffer::zeroed(8);

	assert_eq!(buffer.whole().extents(), &extents(&[(0, 8)]));
}

#[test]
fn a_buffer_copies_bytes_in_and_out_of_its_range() {
	let buffer = Buffer::zeroed(8);
	assert_eq!(buffer.len(), 8);
	assert!(!buffer.is_empty());

	buffer.copy_from(2, b"abc");
	buffer.write(5, b"xyz");
	let mut out = [0u8; 6];
	buffer.copy_to(2, &mut out);
	assert_eq!(&out, b"abcxyz");
	assert_eq!(buffer.to_vec(), b"\0\0abcxyz");
	assert_eq!(buffer.slice_to_vec(2..5), b"abc");

	assert!(Buffer::zeroed(0).is_empty());
}

#[test]
#[should_panic(expected = "outside the")]
fn a_buffer_refuses_a_range_past_its_end() {
	Buffer::zeroed(4).copy_to(2, &mut [0; 3]);
}

#[test]
fn a_span_copies_bytes_relative_to_itself() {
	let buffer = Buffer::from_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);
	let span = buffer.gather([(0u64, 2usize), (4, 3)]);
	assert_eq!(span.to_vec(), [0, 1, 4, 5, 6]);

	// The offset runs across the extents, not within one of them.
	let mut out = [0u8; 3];
	span.copy_to(1, &mut out);
	assert_eq!(out, [1, 4, 5]);

	span.copy_from(1, &[9, 8, 7, 6]);
	assert_eq!(buffer.to_vec(), [0, 9, 2, 3, 8, 7, 6, 7]);
	assert_eq!(span.to_vec(), [0, 9, 8, 7, 6]);

	// A sub-span keeps its bytes relative to itself too: `skip(4)` starts at
	// the second extent.
	let tail = buffer.whole().skip(4).take(3);
	assert_eq!(tail.to_vec(), [8, 7, 6]);
	tail.copy_from(0, b"ab");
	assert_eq!(tail.to_vec(), [b'a', b'b', 6]);
}

/// A [`Memory`] the test owns: enough to prove [`Buffer::new`] works for a
/// custom implementation, not just the built-in bytes.
struct Custom(Vec<u8>);

// SAFETY: the bytes outlive the memory, and an extent that does not fit them
// is refused.
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
fn a_buffer_wraps_a_custom_memory() {
	let buffer = Buffer::new(Arc::new(Custom(vec![1, 2, 3, 4])));
	assert_eq!(buffer.len(), 4);
	assert_eq!(buffer.to_vec(), [1, 2, 3, 4]);

	buffer.copy_from(1, &[9, 9]);
	assert_eq!(buffer.to_vec(), [1, 9, 9, 4]);
	assert_eq!(buffer.slice_to_vec(1..3), [9, 9]);
	let mut out = [0u8; 2];
	buffer.copy_to(2, &mut out);
	assert_eq!(out, [9, 4]);
}

#[test]
fn a_pooled_buffer_backs_a_span_like_any_other() {
	let pool = BufferPool::new(4096, 2);
	let buffer = pool.get();
	buffer.copy_from(0, b"pooled");
	let span = buffer.span(0, 6);
	assert_eq!(span.to_vec(), b"pooled");
	assert_eq!(span.buffer().len(), 4096);
}

#[test]
fn a_buffer_is_a_memory() {
	let memory: Arc<dyn Memory> = Arc::new(Buffer::from_slice(b"hello"));
	assert_eq!(memory.len(), 5);
	assert!(!memory.is_empty());

	let iovec = memory.iovec(Extent { offset: 1, len: 3 }).unwrap();
	assert_eq!(iovec.len(), 3);
	let mut out = [0u8; 3];
	// SAFETY: `iovec` resolved inside `memory`, which stays alive for this copy.
	unsafe { std::ptr::copy_nonoverlapping(iovec.as_ptr(), out.as_mut_ptr(), out.len()) };
	assert_eq!(&out, b"ell");

	assert_eq!(
		memory
			.iovec(Extent { offset: 4, len: 2 })
			.unwrap_err()
			.kind(),
		io::ErrorKind::InvalidInput
	);
}
