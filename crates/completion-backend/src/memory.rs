use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::fmt;
use std::io;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};

/// Resolved pointer and length within a [`Memory`] region.
#[derive(Clone, Copy, Debug)]
pub struct IoVec {
	/// Pointer to the first byte of the range.
	pub ptr: *mut u8,
	/// Byte length of the range.
	pub len: usize,
}

// SAFETY: `IoVec` is a raw pointer into a `Memory` kept alive by the caller
// for the duration of an I/O operation.
unsafe impl Send for IoVec {}

/// Backing memory that resolves `(offset, len)` ranges into raw pointers for I/O.
///
/// # Safety
/// [`Memory::resolve`] must return a pointer and length that remain valid for as
/// long as the `Memory` value is alive, or fail with
/// [`InvalidInput`](io::ErrorKind::InvalidInput) if the range is out of bounds.
pub unsafe trait Memory: Send + Sync {
	fn resolve(&self, offset: usize, len: usize) -> Result<IoVec>;

	fn len(&self) -> usize;
}

fn check_extent(offset: usize, len: usize, bound: usize) -> Result<()> {
	match offset.checked_add(len) {
		Some(end) if end <= bound => Ok(()),
		_ => Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
	}
}

struct Bytes {
	ptr: *mut u8,
	len: usize,
}

impl Bytes {
	fn new(bytes: Box<[u8]>) -> Bytes {
		let len = bytes.len();
		Bytes {
			ptr: Box::into_raw(bytes) as *mut u8,
			len,
		}
	}
}

// SAFETY: `Bytes` owns its heap allocation outright and never exposes `&[u8]`
// directly; `resolve` bounds-checks every range.
unsafe impl Send for Bytes {}
// SAFETY: `&Bytes` only accesses the allocation through bounds-checked `resolve`.
unsafe impl Sync for Bytes {}

impl Drop for Bytes {
	fn drop(&mut self) {
		// SAFETY: `ptr` and `len` came from `Box::<[u8]>::into_raw` in `Bytes::new`.
		drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(self.ptr, self.len)) });
	}
}

// SAFETY: `ptr..ptr + len` is valid for the lifetime of `self` and
// `check_extent` bounds-checks every resolved range.
unsafe impl Memory for Bytes {
	fn resolve(&self, offset: usize, len: usize) -> Result<IoVec> {
		check_extent(offset, len, self.len)?;
		// SAFETY: `offset..offset + len` was bounds-checked above.
		Ok(IoVec {
			ptr: unsafe { self.ptr.add(offset) },
			len,
		})
	}

	fn len(&self) -> usize {
		self.len
	}
}

/// Reference-counted handle to a [`Memory`] region.
///
/// Cloning a `Buffer` is cheap (`Arc` clone) and refers to the same underlying
/// memory. Create a host buffer with [`Buffer::zeroed`] or `Buffer::from(...)`,
/// or wrap a custom [`Memory`] implementation with [`Buffer::new`]. Use
/// [`Buffer::span`] or [`Buffer::whole`] to select a byte range for an I/O
/// operation.
#[derive(Clone)]
pub struct Buffer(Arc<dyn Memory>);

impl Buffer {
	/// Wraps `memory` in a [`Buffer`].
	pub fn new(memory: Arc<dyn Memory>) -> Buffer {
		Buffer(memory)
	}

	/// Allocates `len` zeroed bytes.
	pub fn zeroed(len: usize) -> Buffer {
		Buffer::from(vec![0u8; len])
	}

	/// Total byte length of the underlying [`Memory`].
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Selects `len` bytes starting at `offset`. Bounds are checked when an
	/// operation is submitted.
	pub fn span(&self, offset: usize, len: usize) -> Span {
		Span {
			buffer: self.clone(),
			offset,
			len,
		}
	}

	/// Selects the entire `[0, self.len())` range.
	pub fn whole(&self) -> Span {
		self.span(0, self.len())
	}
}

impl fmt::Debug for Buffer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Buffer").field("len", &self.len()).finish()
	}
}

// SAFETY: delegates to the inner `Arc<dyn Memory>`, which lives at least as
// long as `self`.
unsafe impl Memory for Buffer {
	fn resolve(&self, offset: usize, len: usize) -> Result<IoVec> {
		self.0.resolve(offset, len)
	}

	fn len(&self) -> usize {
		Buffer::len(self)
	}
}

impl From<Vec<u8>> for Buffer {
	fn from(bytes: Vec<u8>) -> Buffer {
		Buffer::from(bytes.into_boxed_slice())
	}
}

impl From<Box<[u8]>> for Buffer {
	fn from(bytes: Box<[u8]>) -> Buffer {
		Buffer(Arc::new(Bytes::new(bytes)))
	}
}

impl From<&[u8]> for Buffer {
	fn from(bytes: &[u8]) -> Buffer {
		Buffer::from(bytes.to_vec())
	}
}

impl<const N: usize> From<&[u8; N]> for Buffer {
	fn from(bytes: &[u8; N]) -> Buffer {
		Buffer::from(&bytes[..])
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

/// A byte range within a [`Buffer`] passed to a read, write, send, or receive
/// operation.
///
/// A [`Buffer`], `Vec<u8>`, `Box<[u8]>`, or `&[u8]` converts directly into a
/// `Span` covering its entire length.
#[derive(Clone)]
pub struct Span {
	buffer: Buffer,
	offset: usize,
	len: usize,
}

impl Span {
	/// Returns the [`Buffer`] this span slices into.
	pub fn buffer(&self) -> &Buffer {
		&self.buffer
	}

	/// Returns the byte offset in the [`Buffer`] where this span starts.
	pub fn offset(&self) -> usize {
		self.offset
	}

	pub(crate) fn resolve(&self) -> Result<IoVec> {
		self.buffer.0.resolve(self.offset, self.len)
	}

	/// Byte length of this span.
	pub fn len(&self) -> usize {
		self.len
	}

	/// Returns a new [`Span`] with the first `bytes` removed.
	pub fn skip(self, bytes: usize) -> Span {
		let skipped = bytes.min(self.len);
		Span {
			buffer: self.buffer,
			offset: self.offset + skipped,
			len: self.len - skipped,
		}
	}

	/// Returns a new [`Span`] truncated to at most the first `bytes`.
	pub fn take(self, bytes: usize) -> Span {
		Span {
			buffer: self.buffer,
			offset: self.offset,
			len: self.len.min(bytes),
		}
	}
}

impl Deref for Span {
	type Target = [u8];

	fn deref(&self) -> &[u8] {
		let vec = self
			.resolve()
			.expect("the span's range is outside the buffer");
		if vec.len == 0 {
			return &[];
		}
		// SAFETY: `resolve` returns `vec.len` valid bytes within `self.buffer`,
		// which remains alive for `&self`.
		unsafe { std::slice::from_raw_parts(vec.ptr, vec.len) }
	}
}

impl DerefMut for Span {
	fn deref_mut(&mut self) -> &mut [u8] {
		let vec = self
			.resolve()
			.expect("the span's range is outside the buffer");
		if vec.len == 0 {
			return &mut [];
		}
		// SAFETY: `resolve` returns `vec.len` valid bytes within `self.buffer`,
		// which remains alive for `&mut self`.
		unsafe { std::slice::from_raw_parts_mut(vec.ptr, vec.len) }
	}
}

impl AsRef<[u8]> for Span {
	fn as_ref(&self) -> &[u8] {
		self
	}
}

impl AsMut<[u8]> for Span {
	fn as_mut(&mut self) -> &mut [u8] {
		self
	}
}

impl From<Buffer> for Span {
	fn from(buffer: Buffer) -> Span {
		buffer.whole()
	}
}

impl From<&Buffer> for Span {
	fn from(buffer: &Buffer) -> Span {
		buffer.whole()
	}
}

impl From<Vec<u8>> for Span {
	fn from(bytes: Vec<u8>) -> Span {
		Buffer::from(bytes).whole()
	}
}

impl From<Box<[u8]>> for Span {
	fn from(bytes: Box<[u8]>) -> Span {
		Buffer::from(bytes).whole()
	}
}

impl From<&[u8]> for Span {
	fn from(bytes: &[u8]) -> Span {
		Buffer::from(bytes).whole()
	}
}

const PAGE_SIZE: usize = 4096;

/// Pool of reusable, page-aligned, pre-faulted [`Buffer`]s.
///
/// The pool allocates a single 4096-byte-aligned arena and touches every page
/// up front so transfers do not fault pages in during I/O. When `block_size` is
/// a multiple of 4096, every block is page-aligned (suitable for `O_DIRECT`).
/// Blocks return to the pool automatically when the last [`Buffer`] or [`Span`]
/// referencing them is dropped.
///
/// ```
/// # use completion_backend::BufferPool;
/// let pool = BufferPool::new(4096, 16);
/// let buffer = pool.get();
/// assert_eq!(buffer.len(), 4096);
/// drop(buffer);
/// assert_eq!(pool.available(), 16);
/// ```
#[derive(Clone)]
pub struct BufferPool(Arc<PoolInner>);

struct PoolInner {
	ptr: *mut u8,
	total: usize,
	block_size: usize,
	capacity: usize,
	free: Mutex<Vec<usize>>,
}

// SAFETY: the arena is owned by `PoolInner`, each checked-out slot covers a
// disjoint block, and the free list is synchronized by `Mutex`.
unsafe impl Send for PoolInner {}
// SAFETY: `&PoolInner` only exposes memory through bounds-checked `resolve`.
unsafe impl Sync for PoolInner {}

impl Drop for PoolInner {
	fn drop(&mut self) {
		// SAFETY: allocated with this layout in `BufferPool::new`.
		unsafe {
			dealloc(
				self.ptr,
				Layout::from_size_align(self.total, PAGE_SIZE).unwrap(),
			)
		}
	}
}

// SAFETY: `check_extent` bounds-checks every range against `self.total`.
unsafe impl Memory for PoolInner {
	fn resolve(&self, offset: usize, len: usize) -> Result<IoVec> {
		check_extent(offset, len, self.total)?;
		// SAFETY: `offset..offset + len` was bounds-checked above.
		Ok(IoVec {
			ptr: unsafe { self.ptr.add(offset) },
			len,
		})
	}

	fn len(&self) -> usize {
		self.total
	}
}

struct Slot {
	pool: Arc<PoolInner>,
	offset: usize,
}

impl Drop for Slot {
	fn drop(&mut self) {
		self.pool
			.free
			.lock()
			.unwrap()
			.push(self.offset / self.pool.block_size);
	}
}

// SAFETY: `self.pool` keeps the arena alive and `check_extent` restricts every
// resolved range to this slot's block.
unsafe impl Memory for Slot {
	fn resolve(&self, offset: usize, len: usize) -> Result<IoVec> {
		check_extent(offset, len, self.pool.block_size)?;
		// SAFETY: `self.offset + offset..self.offset + offset + len` lies within
		// this slot's block inside the arena.
		Ok(IoVec {
			ptr: unsafe { self.pool.ptr.add(self.offset + offset) },
			len,
		})
	}

	fn len(&self) -> usize {
		self.pool.block_size
	}
}

impl BufferPool {
	/// Allocates a pool of `capacity` blocks of `block_size` bytes each.
	///
	/// The arena is aligned to 4096 bytes; each block is page-aligned when
	/// `block_size` is a multiple of 4096.
	///
	/// # Panics
	/// Panics if `block_size` or `capacity` is zero, if their product overflows
	/// `isize::MAX`, or if allocation fails.
	pub fn new(block_size: usize, capacity: usize) -> BufferPool {
		assert!(block_size > 0, "a pool block holds at least one byte");
		assert!(capacity > 0, "a pool holds at least one block");
		let total = block_size
			.checked_mul(capacity)
			.filter(|&total| total <= isize::MAX as usize)
			.expect("a pool's blocks must fit one allocation");
		let layout =
			Layout::from_size_align(total, PAGE_SIZE).expect("the pool layout is representable");
		// SAFETY: `total > 0`.
		let ptr = unsafe { alloc_zeroed(layout) };
		if ptr.is_null() {
			handle_alloc_error(layout);
		}
		// `alloc_zeroed` may return lazily mapped zero pages (`mmap`), which would
		// fault on first I/O. Touch each page up front to fault it in now.
		// SAFETY: `ptr` is valid for `total` writable bytes and `total > 0`.
		unsafe {
			for offset in (0..total).step_by(PAGE_SIZE) {
				ptr.add(offset).write_volatile(0);
			}
			ptr.add(total - 1).write_volatile(0);
		}
		BufferPool(Arc::new(PoolInner {
			ptr,
			total,
			block_size,
			capacity,
			free: Mutex::new((0..capacity).rev().collect()),
		}))
	}

	/// Checks out a pooled block, or returns `None` if all blocks are in use.
	pub fn try_get(&self) -> Option<Buffer> {
		let slot = self.0.free.lock().unwrap().pop()?;
		Some(self.buffer(slot))
	}

	/// Checks out a pooled block, falling back to a standalone zeroed [`Buffer`]
	/// if all pooled blocks are in use.
	pub fn get(&self) -> Buffer {
		match self.try_get() {
			Some(buffer) => buffer,
			None => Buffer::zeroed(self.block_size()),
		}
	}

	fn buffer(&self, slot: usize) -> Buffer {
		Buffer::new(Arc::new(Slot {
			offset: slot * self.0.block_size,
			pool: Arc::clone(&self.0),
		}))
	}

	/// Byte length of each block.
	pub fn block_size(&self) -> usize {
		self.0.block_size
	}

	/// Total number of blocks in the pool.
	pub fn capacity(&self) -> usize {
		self.0.capacity
	}

	/// Number of blocks currently available in the pool.
	pub fn available(&self) -> usize {
		self.0.free.lock().unwrap().len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn address(buffer: &Buffer) -> usize {
		buffer.0.resolve(0, 1).unwrap().ptr as usize
	}

	#[test]
	fn a_pooled_block_is_page_aligned_and_comes_back_to_the_same_address() {
		let pool = BufferPool::new(4096, 2);
		let first = pool.get();
		let second = pool.get();
		assert_eq!(address(&first) % PAGE_SIZE, 0);
		assert_eq!(address(&second) % PAGE_SIZE, 0);

		let start = address(&first);
		drop(first);
		let again = pool.get();
		assert_eq!(
			address(&again),
			start,
			"the block the pool took back is the one it hands out"
		);
	}
}
