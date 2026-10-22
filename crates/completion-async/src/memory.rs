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
pub unsafe trait AsIoVec: Send + Sync + 'static {
	fn as_iovec(&self) -> IoVec;
}

/// Host bytes the kernel may write through `&self`: each byte is an
/// `UnsafeCell`, and no `&[u8]` to them is ever handed out.
struct Bytes(Vec<UnsafeCell<u8>>);

impl Bytes {
	fn new(bytes: Vec<u8>) -> Bytes {
		let mut bytes = ManuallyDrop::new(bytes);
		let (ptr, len, capacity) = (bytes.as_mut_ptr(), bytes.len(), bytes.capacity());
		// SAFETY: `UnsafeCell<u8>` is `repr(transparent)` over `u8`, so the
		// allocation's layout is unchanged.
		Bytes(unsafe { Vec::from_raw_parts(ptr.cast::<UnsafeCell<u8>>(), len, capacity) })
	}
}

impl Bytes {
	fn len(&self) -> usize {
		self.0.len()
	}

	fn as_mut_slice(&mut self) -> &mut [u8] {
		let cells = self.0.as_mut_slice();
		// SAFETY: `UnsafeCell<u8>` is `repr(transparent)` over `u8`, and
		// `&mut self` guarantees exclusive access.
		unsafe { &mut *(cells as *mut [UnsafeCell<u8>] as *mut [u8]) }
	}
}

// SAFETY: every access to the bytes goes through a pointer `as_iovec` hands to
// an operation; the caller orders those accesses, as for any other `AsIoVec`.
unsafe impl Sync for Bytes {}

// SAFETY: the vector is never resized, so its elements stay put for the
// lifetime of `self`, and `as_iovec` names the whole of it.
unsafe impl AsIoVec for Bytes {
	fn as_iovec(&self) -> IoVec {
		IoVec {
			ptr: UnsafeCell::raw_get(self.0.as_ptr()),
			len: self.0.len(),
		}
	}
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
pub struct Buffer(Arc<Bytes>);

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
		Arc::get_mut(&mut self.0).map(Bytes::as_mut_slice)
	}
}

impl fmt::Debug for Buffer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Buffer").field("len", &self.len()).finish()
	}
}

// SAFETY: the `Arc` keeps the bytes in place for as long as the buffer is
// alive, and every clone names the same range.
unsafe impl AsIoVec for Buffer {
	fn as_iovec(&self) -> IoVec {
		self.0.as_iovec()
	}
}

impl From<Vec<u8>> for Buffer {
	fn from(bytes: Vec<u8>) -> Buffer {
		Buffer(Arc::new(Bytes::new(bytes)))
	}
}

impl From<Box<[u8]>> for Buffer {
	fn from(bytes: Box<[u8]>) -> Buffer {
		Buffer::from(bytes.into_vec())
	}
}
