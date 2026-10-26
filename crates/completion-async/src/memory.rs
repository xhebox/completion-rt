use std::cell::UnsafeCell;
use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::Arc;

use completion_backend::IoVec;

/// Memory an I/O operation can transfer through.
///
/// Implement it for memory the buffer types cannot own, such as a range inside
/// a larger allocation.
///
/// # Safety
/// The returned range must stay valid, and writable by the kernel through `&self`,
/// for as long as `self` is alive.
pub unsafe trait AsIoVec: Send + 'static {
	fn as_iovec(&self) -> IoVec;
}

/// Bytes for I/O transfers.
///
/// A `Buffer` implements [`AsIoVec`], and every operation takes its buffer by
/// value. Cloning is cheap and shares the bytes: pass `buffer.clone()` to an
/// operation and keep the original to read the bytes back afterwards.
///
/// Use [`Buffer::zeroed`], [`Buffer::copy_from`], or `Buffer::from` a `Vec<u8>`
/// / `Box<[u8]>` to allocate one, and [`Buffer::get_mut`] to read or write it
/// once no other handle names the bytes.
#[derive(Clone)]
pub struct Buffer(Arc<Vec<UnsafeCell<u8>>>);

// SAFETY: `&Buffer` only exposes its length and a raw pointer derived from
// `UnsafeCell::raw_get`; no `&[u8]` or `&mut [u8]` is ever created while
// shared, and `Buffer::get_mut` requires `Arc::get_mut` exclusivity.
unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Buffer {
	/// Allocates `len` zeroed bytes.
	pub fn zeroed(len: usize) -> Buffer {
		Buffer::from(vec![0u8; len])
	}

	/// Allocates a buffer holding a copy of `bytes`.
	pub fn copy_from(bytes: &[u8]) -> Buffer {
		Buffer::from(bytes.to_vec())
	}

	/// Byte length of the buffer.
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Returns the bytes for reading or writing when no other `Buffer` handle
	/// (including one an in-flight operation holds) names them; otherwise
	/// `None`.
	pub fn get_mut(&mut self) -> Option<&mut [u8]> {
		let cells = Arc::get_mut(&mut self.0)?.as_mut_slice();
		// SAFETY: `UnsafeCell<u8>` has the same layout as `u8`, and `Arc::get_mut`
		// guarantees exclusive access to the allocation.
		Some(unsafe { &mut *(cells as *mut [UnsafeCell<u8>] as *mut [u8]) })
	}
}

impl fmt::Debug for Buffer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Buffer").field("len", &self.len()).finish()
	}
}

// SAFETY: the `Arc` keeps the allocation in place without resizing for as long
// as the `Buffer` is alive, and `UnsafeCell::raw_get` yields a pointer writable
// by the kernel through `&self`.
unsafe impl AsIoVec for Buffer {
	fn as_iovec(&self) -> IoVec {
		IoVec {
			ptr: UnsafeCell::raw_get(self.0.as_ptr()),
			len: self.0.len(),
		}
	}
}

impl From<Vec<u8>> for Buffer {
	#[expect(clippy::arc_with_non_send_sync)]
	fn from(bytes: Vec<u8>) -> Buffer {
		let mut bytes = ManuallyDrop::new(bytes);
		let (ptr, len, capacity) = (bytes.as_mut_ptr(), bytes.len(), bytes.capacity());
		// SAFETY: `UnsafeCell<u8>` is `#[repr(transparent)]` over `u8`.
		let cells = unsafe { Vec::from_raw_parts(ptr.cast::<UnsafeCell<u8>>(), len, capacity) };
		Buffer(Arc::new(cells))
	}
}

impl From<Box<[u8]>> for Buffer {
	fn from(bytes: Box<[u8]>) -> Buffer {
		Buffer::from(bytes.into_vec())
	}
}

#[cfg(test)]
mod tests {
	use assert2::assert;
	use insta::assert_debug_snapshot;

	use super::{AsIoVec, Buffer};

	#[test]
	fn a_buffer_hands_out_its_bytes() {
		let mut buffer = Buffer::zeroed(8);
		assert!(buffer.len() == 8);

		let bytes = buffer.get_mut().unwrap();
		bytes[2..5].copy_from_slice(b"abc");
		bytes[5..8].copy_from_slice(b"xyz");
		assert!(buffer.get_mut().unwrap() == b"\0\0abcxyz");

		assert!(Buffer::zeroed(0).get_mut().unwrap() == b"");
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
		assert!(clone.clone().get_mut().is_none());
		let mut clone = clone;
		assert!(clone.get_mut().unwrap() == b"h\x09\x09lo");
	}

	#[test]
	fn a_buffer_is_the_bytes_it_was_built_from() {
		let mut from_vec = Buffer::from(b"world".to_vec());
		assert!(from_vec.len() == 5);
		assert!(from_vec.get_mut().unwrap() == b"world");
		assert_debug_snapshot!(from_vec, @r#"
		Buffer {
		    len: 5,
		}
		"#);

		let from_box: Box<[u8]> = b"boxed".to_vec().into_boxed_slice();
		let mut from_box = Buffer::from(from_box);
		assert!(from_box.get_mut().unwrap() == b"boxed");
	}

	#[test]
	fn a_buffer_names_its_whole_range_as_an_iovec() {
		let mut buffer = Buffer::copy_from(b"hello");
		let iovec = buffer.as_iovec();
		assert!(iovec.len == buffer.len());
		assert!(iovec.ptr == buffer.get_mut().unwrap().as_ptr().cast_mut());
	}
}
