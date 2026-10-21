use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::mem::ManuallyDrop;
use std::sync::Arc;

use completion_backend::{Error, IoVec, Result};

fn check_extent(offset: usize, len: usize, bound: usize) -> Result<()> {
	match offset.checked_add(len) {
		Some(end) if end <= bound => Ok(()),
		_ => Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
	}
}

/// Backing memory for I/O operations.
///
/// # Safety
/// [`Memory::resolve`] must return a pointer and length that remain valid for
/// as long as the `Memory` value is alive, or fail with
/// [`InvalidInput`](io::ErrorKind::InvalidInput) if the range is out of bounds.
pub unsafe trait Memory: Send + Sync {
	fn resolve(&self, offset: usize, len: usize) -> Result<IoVec>;

	fn len(&self) -> usize;

	/// Returns a mutable slice when `&mut self` guarantees exclusive access.
	///
	/// Shared-memory implementations (guest RAM, shared mmap) should keep the
	/// default `None`.
	fn get_mut(&mut self, offset: usize, len: usize) -> Option<&mut [u8]> {
		let _ = (offset, len);
		None
	}
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

// SAFETY: every access to the bytes goes through a pointer `resolve` hands to
// an operation; the caller orders those accesses, as for any other `Memory`.
unsafe impl Sync for Bytes {}

// SAFETY: the vector is never resized, so its elements stay put for the
// lifetime of `self`, and `check_extent` bounds-checks every range.
unsafe impl Memory for Bytes {
	fn resolve(&self, offset: usize, len: usize) -> Result<IoVec> {
		check_extent(offset, len, self.0.len())?;
		Ok(IoVec {
			ptr: UnsafeCell::raw_get(self.0[offset..].as_ptr()),
			len,
		})
	}

	fn len(&self) -> usize {
		self.0.len()
	}

	fn get_mut(&mut self, offset: usize, len: usize) -> Option<&mut [u8]> {
		check_extent(offset, len, self.0.len()).ok()?;
		if len == 0 {
			return Some(&mut []);
		}
		let cells = &mut self.0[offset..offset + len];
		// SAFETY: `UnsafeCell<u8>` is `repr(transparent)` over `u8`, and
		// `&mut self` guarantees exclusive access.
		Some(unsafe { &mut *(cells as *mut [UnsafeCell<u8>] as *mut [u8]) })
	}
}

/// A byte range backed by reference-counted memory, used for I/O transfers.
///
/// Cloning is cheap (`Arc` clone) and refers to the same memory. Use
/// [`Buffer::zeroed`], [`Buffer::copy_from`], or `Buffer::from` a `Vec<u8>`
/// / `Box<[u8]>` for host buffers, or wrap a custom [`Memory`] with
/// [`Buffer::new`]. [`slice`](Buffer::slice), [`skip`](Buffer::skip), and
/// [`take`](Buffer::take) return narrower views without copying.
#[derive(Clone)]
pub struct Buffer {
	pub(crate) memory: Arc<dyn Memory>,
	offset: usize,
	len: usize,
}

impl Buffer {
	/// Wraps `memory`, spanning its full length.
	pub fn new(memory: Arc<dyn Memory>) -> Buffer {
		let len = memory.len();
		Buffer {
			memory,
			offset: 0,
			len,
		}
	}

	/// Allocates `len` zeroed bytes.
	pub fn zeroed(len: usize) -> Buffer {
		Buffer::from(vec![0u8; len])
	}

	/// Allocates a buffer holding a copy of `bytes`.
	pub fn copy_from(bytes: &[u8]) -> Buffer {
		Buffer::from(bytes.to_vec())
	}

	/// Byte length of this view.
	pub fn len(&self) -> usize {
		self.len
	}

	/// Byte offset of this view within the underlying [`Memory`].
	pub fn offset(&self) -> usize {
		self.offset
	}

	/// Resolves this view's range into a raw pointer for I/O.
	pub(crate) fn resolve(&self) -> Result<IoVec> {
		self.memory.resolve(self.offset, self.len)
	}

	/// Moves the `Arc<dyn Memory>` out for keeping it alive in an `Entry`.
	pub(crate) fn into_memory(self) -> Arc<dyn Memory> {
		self.memory
	}

	/// Returns a narrower view starting at `offset` within this view.
	pub fn slice(&self, offset: usize, len: usize) -> Buffer {
		Buffer {
			memory: self.memory.clone(),
			offset: self.offset + offset,
			len,
		}
	}

	/// Returns a view with the first `bytes` removed.
	pub fn skip(self, bytes: usize) -> Buffer {
		let skipped = bytes.min(self.len);
		Buffer {
			memory: self.memory,
			offset: self.offset + skipped,
			len: self.len - skipped,
		}
	}

	/// Returns a view truncated to at most the first `bytes`.
	pub fn take(self, bytes: usize) -> Buffer {
		Buffer {
			memory: self.memory,
			offset: self.offset,
			len: self.len.min(bytes),
		}
	}

	/// Returns the bytes if no other `Buffer` refers to this memory and no
	/// operations are in flight; otherwise `None`.
	pub fn get_mut(&mut self) -> Option<&mut [u8]> {
		let (offset, len) = (self.offset, self.len);
		Arc::get_mut(&mut self.memory)?.get_mut(offset, len)
	}
}

impl fmt::Debug for Buffer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Buffer")
			.field("offset", &self.offset)
			.field("len", &self.len)
			.finish()
	}
}

impl From<Vec<u8>> for Buffer {
	fn from(bytes: Vec<u8>) -> Buffer {
		let len = bytes.len();
		Buffer {
			memory: Arc::new(Bytes::new(bytes)),
			offset: 0,
			len,
		}
	}
}

impl From<Box<[u8]>> for Buffer {
	fn from(bytes: Box<[u8]>) -> Buffer {
		Buffer::from(bytes.into_vec())
	}
}

impl<M: Memory + 'static> From<Arc<M>> for Buffer {
	fn from(memory: Arc<M>) -> Buffer {
		Buffer::new(memory)
	}
}

impl From<Arc<dyn Memory>> for Buffer {
	fn from(memory: Arc<dyn Memory>) -> Buffer {
		Buffer::new(memory)
	}
}
