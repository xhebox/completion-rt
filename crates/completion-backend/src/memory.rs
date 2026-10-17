//! Reference-counted buffers ([`Buffer`]), byte-range views ([`Span`]), and a
//! pre-faulted, reusable [`BufferPool`] for completion transfers.

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::fmt;
use std::io;
use std::ops::Range;
use std::ptr;
use std::sync::{Arc, Mutex};

use smallvec::SmallVec;

/// A contiguous `(offset, len)` byte range within a [`Buffer`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Extent {
	pub offset: u64,
	pub len: usize,
}

impl From<(u64, usize)> for Extent {
	fn from((offset, len): (u64, usize)) -> Extent {
		Extent { offset, len }
	}
}

/// Raw pointer and length for a single I/O buffer, ABI-compatible with
/// `libc::iovec`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IoVec {
	ptr: *mut u8,
	len: usize,
}

impl IoVec {
	/// Wraps `ptr` and `len` into an [`IoVec`].
	///
	/// # Safety
	/// `ptr..ptr.add(len)` must remain valid for reads and writes while any
	/// operation referencing this [`IoVec`] is in flight.
	pub unsafe fn new(ptr: *mut u8, len: usize) -> Self {
		Self { ptr, len }
	}

	/// Number of bytes this range names.
	pub fn len(&self) -> usize {
		self.len
	}

	/// Whether the range names no bytes. A zero-length range is a normal
	/// boundary — an empty span resolves to one — not an error.
	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	/// The first byte of the range.
	///
	/// Reading or writing through it races a kernel, another thread, or a guest
	/// while a transfer is in flight: a caller copies to or from memory of its
	/// own, and only while the [`Memory`] the range came from is alive.
	pub fn as_ptr(&self) -> *mut u8 {
		self.ptr
	}
}

impl fmt::Debug for IoVec {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("IoVec")
			.field("ptr", &self.ptr)
			.field("len", &self.len)
			.finish()
	}
}

unsafe impl Send for IoVec {}

/// Backing memory that resolves [`Extent`]s into raw [`IoVec`]s for I/O.
///
/// # Safety
/// - Because the kernel, another thread, or a VM guest may concurrently read or
///   write this memory during I/O, implementations must not expose `&[u8]` or
///   `&mut [u8]` references that could alias in-flight transfers.
/// - [`Memory::iovec`] must return a pointer and length that remain valid for
///   as long as the `Memory` value is alive, or fail with
///   [`InvalidInput`](io::ErrorKind::InvalidInput) if `extent` is out of bounds.
pub unsafe trait Memory: Send + Sync {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec>;

	fn len(&self) -> usize;

	/// Whether the memory holds no bytes. A zero-length memory is a normal
	/// boundary, not an error.
	fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

/// `extent` as a `(start, len)` pair inside `bound`, or an `InvalidInput` error
/// when it does not fit. The length is returned separately: an extent of zero
/// bytes still names a start.
fn check_extent(extent: Extent, bound: usize) -> io::Result<(usize, usize)> {
	let start =
		usize::try_from(extent.offset).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
	let end = start
		.checked_add(extent.len)
		.filter(|&end| end <= bound)
		.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
	Ok((start, end - start))
}

/// Heap-backed [`Memory`] owning the bytes of one `Box<[u8]>`.
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

	/// The bytes as a `Vec`, taking the allocation rather than dropping it.
	fn into_vec(self) -> Vec<u8> {
		let this = std::mem::ManuallyDrop::new(self);
		// SAFETY: `ptr` and `len` come from `Box::<[u8]>::into_raw`, and the
		// box is rebuilt here instead of being freed by `Drop`, which the
		// `ManuallyDrop` above skips.
		unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(this.ptr, this.len)).into_vec() }
	}
}

// SAFETY: the allocation is owned outright and never handed out as a slice;
// `iovec` bounds-checks every extent, and the bytes are plain storage that a
// kernel may write while another thread reads.
unsafe impl Send for Bytes {}
// SAFETY: see the `Send` impl: `&Bytes` only reaches memory through `iovec`.
unsafe impl Sync for Bytes {}

impl Drop for Bytes {
	fn drop(&mut self) {
		// SAFETY: `ptr` and `len` come from `Box::<[u8]>::into_raw` and are
		// turned back into a box only here. A zero-length box needs no
		// deallocation but is still a valid box.
		drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(self.ptr, self.len)) });
	}
}

// SAFETY: `ptr..ptr + len` is the box's own allocation, which lives as long as
// `&self`; `check_extent` keeps every resolved range inside it.
unsafe impl Memory for Bytes {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec> {
		let (start, len) = check_extent(extent, self.len)?;
		// SAFETY: `start..start + len` is inside the allocation, checked above.
		Ok(unsafe { IoVec::new(self.ptr.add(start), len) })
	}

	fn len(&self) -> usize {
		self.len
	}
}

/// Reference-counted handle to a [`Memory`] region.
///
/// Cloning a `Buffer` is cheap (`Arc` clone) and refers to the same underlying
/// memory. Create a host buffer with [`Buffer::zeroed`], [`Buffer::from_slice`],
/// or `Buffer::from(vec)`, or wrap a custom [`Memory`] implementation with
/// [`Buffer::new`]. Use [`Buffer::span`], [`Buffer::whole`], or
/// [`Buffer::gather`] to select byte ranges for an I/O operation.
#[derive(Clone)]
pub struct Buffer(Inner);

/// What a [`Buffer`] holds.
///
/// A block this crate allocated is kept apart from a caller's [`Memory`] so the
/// bytes can be handed back whole once nothing else holds them
/// ([`Buffer::try_into_vec`]); a caller's memory has no such shape to recover.
#[derive(Clone)]
enum Inner {
	Owned(Arc<Bytes>),
	Custom(Arc<dyn Memory>),
}

impl Inner {
	fn memory(&self) -> &dyn Memory {
		match self {
			Inner::Owned(bytes) => bytes.as_ref(),
			Inner::Custom(memory) => memory.as_ref(),
		}
	}
}

impl Buffer {
	/// Wraps `memory` in a [`Buffer`].
	pub fn new(memory: Arc<dyn Memory>) -> Buffer {
		Buffer(Inner::Custom(memory))
	}

	/// Allocates `len` zeroed bytes.
	pub fn zeroed(len: usize) -> Buffer {
		Buffer::from(vec![0u8; len])
	}

	/// Copies `bytes` into a new [`Buffer`].
	pub fn from_slice(bytes: &[u8]) -> Buffer {
		Buffer::from(bytes.to_vec())
	}

	/// The bytes of a block this crate allocated, once this is the last holder.
	///
	/// `Err(self)` when another `Buffer`, or a [`Span`] taken from one, is still
	/// alive — the bytes are still theirs — and also for a buffer wrapped around
	/// a caller's [`Memory`], whose shape cannot be handed back as a `Vec`.
	/// Nothing is copied on the way out: this is how a transfer's bytes leave
	/// without a second trip through memory that a read already filled.
	pub fn try_into_vec(self) -> Result<Vec<u8>, Buffer> {
		match self.0 {
			Inner::Owned(bytes) => match Arc::try_unwrap(bytes) {
				Ok(bytes) => Ok(bytes.into_vec()),
				Err(bytes) => Err(Buffer(Inner::Owned(bytes))),
			},
			custom @ Inner::Custom(_) => Err(Buffer(custom)),
		}
	}

	/// Total byte length of the underlying [`Memory`].
	pub fn len(&self) -> usize {
		self.0.memory().len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// Selects `len` bytes starting at `offset`. Bounds are checked when an
	/// operation is submitted.
	pub fn span(&self, offset: u64, len: usize) -> Span {
		self.gather([Extent { offset, len }])
	}

	/// Selects the entire `[0, self.len())` range.
	pub fn whole(&self) -> Span {
		self.span(0, self.len())
	}

	/// Combines multiple `(offset, len)` [`Extent`]s into a single scatter/gather [`Span`].
	pub fn gather<E: Into<Extent>>(&self, extents: impl IntoIterator<Item = E>) -> Span {
		Span {
			buffer: self.clone(),
			extents: extents.into_iter().map(Into::into).collect(),
		}
	}

	/// The [`Extent`] `offset..offset + len` names, panicking when it is outside
	/// the buffer.
	fn extent(&self, offset: usize, len: usize) -> Extent {
		let end = offset
			.checked_add(len)
			.expect("the range must not overflow a usize");
		assert!(
			end <= self.len(),
			"the range {offset}..{end} is outside the {}-byte buffer",
			self.len()
		);
		Extent {
			offset: offset as u64,
			len,
		}
	}

	/// Copies `dst.len()` bytes starting at `offset` into `dst`.
	///
	/// Panics if the range is outside the buffer or the [`Memory`] refuses it.
	/// Reads go through [`Memory::iovec`] rather than a `&[u8]`, so an in-flight
	/// transfer through the same memory is never aliased by a reference.
	pub fn copy_to(&self, offset: usize, dst: &mut [u8]) {
		let iovec = self
			.0
			.memory()
			.iovec(self.extent(offset, dst.len()))
			.expect("the memory resolves the extent just checked");
		// SAFETY: `iovec` names `dst.len()` readable bytes, in bounds and alive
		// for `&self`; `dst` cannot overlap it, coming from the caller.
		unsafe { ptr::copy_nonoverlapping(iovec.as_ptr(), dst.as_mut_ptr(), dst.len()) }
	}

	/// Copies `src.len()` bytes from `src` into the buffer starting at `offset`.
	///
	/// Panics if the range is outside the buffer or the [`Memory`] refuses it.
	/// Writes go through [`Memory::iovec`], like [`Buffer::copy_to`].
	pub fn copy_from(&self, offset: usize, src: &[u8]) {
		let iovec = self
			.0
			.memory()
			.iovec(self.extent(offset, src.len()))
			.expect("the memory resolves the extent just checked");
		// SAFETY: `iovec` names `src.len()` writable bytes, in bounds and alive
		// for `&self`; `src` cannot overlap it, coming from the caller.
		unsafe { ptr::copy_nonoverlapping(src.as_ptr(), iovec.as_ptr(), src.len()) }
	}

	/// Writes `src` at `offset`: [`Buffer::copy_from`] under the name callers
	/// reach for.
	pub fn write(&self, offset: usize, src: &[u8]) {
		self.copy_from(offset, src);
	}

	/// Copies the entire buffer into a new `Vec<u8>`.
	pub fn to_vec(&self) -> Vec<u8> {
		self.slice_to_vec(0..self.len())
	}

	/// Copies `range` into a new `Vec<u8>`.
	///
	/// Panics if `range.start > range.end` or `range.end` is past the buffer.
	pub fn slice_to_vec(&self, range: Range<usize>) -> Vec<u8> {
		assert!(
			range.start <= range.end,
			"the range starts at {} and ends before it at {}",
			range.start,
			range.end
		);
		assert!(
			range.end <= self.len(),
			"the range ends at {} past the {}-byte buffer",
			range.end,
			self.len()
		);
		let mut out = vec![0; range.end - range.start];
		self.copy_to(range.start, &mut out);
		out
	}
}

impl fmt::Debug for Buffer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Buffer").field("len", &self.len()).finish()
	}
}

impl From<Vec<u8>> for Buffer {
	fn from(bytes: Vec<u8>) -> Buffer {
		Buffer::from(bytes.into_boxed_slice())
	}
}

impl From<Box<[u8]>> for Buffer {
	fn from(bytes: Box<[u8]>) -> Buffer {
		Buffer(Inner::Owned(Arc::new(Bytes::new(bytes))))
	}
}

impl From<&[u8]> for Buffer {
	fn from(bytes: &[u8]) -> Buffer {
		Buffer::from(bytes.to_vec())
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

/// One or more byte ranges within a [`Buffer`] passed to a read, write, send, or
/// receive operation.
#[derive(Clone)]
pub struct Span {
	buffer: Buffer,
	extents: SmallVec<[Extent; 2]>,
}

impl Span {
	/// Returns the [`Buffer`] this span slices into.
	pub fn buffer(&self) -> &Buffer {
		&self.buffer
	}

	/// Returns the ordered list of [`Extent`]s in this span.
	pub fn extents(&self) -> &[Extent] {
		&self.extents
	}

	/// Resolves every extent into `out`, which is cleared first. An empty span
	/// resolves to an empty array: nothing names a buffer.
	pub(crate) fn resolve_into(&self, out: &mut Vec<IoVec>) -> io::Result<()> {
		out.clear();
		match self.extents.as_slice() {
			[] => Ok(()),
			// A single extent resolves without the array a gathered span needs.
			[_] => self
				.resolve_single()
				.expect("a span of one extent needs no array")
				.map(|buf| out.push(buf)),
			extents => {
				out.reserve(extents.len());
				let memory = self.buffer.0.memory();
				for extent in extents {
					out.push(memory.iovec(*extent)?);
				}
				Ok(())
			}
		}
	}

	/// Resolves a span of at most one extent without an array: an empty span is
	/// one empty buffer and a single extent its one [`IoVec`]. `None` for a span
	/// that needs an array to hold it.
	pub(crate) fn resolve_single(&self) -> Option<io::Result<IoVec>> {
		match self.extents.as_slice() {
			// SAFETY: a null pointer with a zero length names no bytes.
			[] => Some(Ok(unsafe { IoVec::new(ptr::null_mut(), 0) })),
			[extent] => Some(self.buffer.0.memory().iovec(*extent)),
			_ => None,
		}
	}

	/// Total number of bytes across all extents in this span.
	pub fn len(&self) -> usize {
		self.extents
			.iter()
			.fold(0, |len, extent| len.saturating_add(extent.len))
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// Returns a new [`Span`] with the first `bytes` removed.
	pub fn skip(self, bytes: usize) -> Span {
		let mut remaining = bytes;
		let mut extents = SmallVec::new();
		for extent in &self.extents {
			if remaining >= extent.len {
				remaining -= extent.len;
				continue;
			}
			extents.push(Extent {
				offset: extent.offset + remaining as u64,
				len: extent.len - remaining,
			});
			remaining = 0;
		}
		Span { extents, ..self }
	}

	/// Returns a new [`Span`] truncated to at most the first `bytes`.
	pub fn take(self, bytes: usize) -> Span {
		let mut remaining = bytes;
		let mut extents = SmallVec::new();
		for extent in &self.extents {
			if remaining == 0 {
				break;
			}
			let len = extent.len.min(remaining);
			remaining -= len;
			extents.push(Extent {
				offset: extent.offset,
				len,
			});
		}
		Span { extents, ..self }
	}

	/// The offset into the span `offset..offset + len` names, panicking when it
	/// is outside the span.
	fn extent_range(&self, offset: usize, len: usize) -> (usize, usize) {
		let end = offset
			.checked_add(len)
			.expect("the range must not overflow a usize");
		assert!(
			end <= self.len(),
			"the range {offset}..{end} is outside the {}-byte span",
			self.len()
		);
		(offset, len)
	}

	/// Copies `dst.len()` bytes starting `offset` bytes into the span into `dst`.
	///
	/// Panics if the range is outside the span or an extent does not resolve.
	/// The bytes are read through [`Buffer::copy_to`], so a byte range straddling
	/// two gathered extents is copied one extent at a time without a reference
	/// over the underlying [`Memory`].
	pub fn copy_to(&self, offset: usize, dst: &mut [u8]) {
		self.extent_range(offset, dst.len());
		let mut copied = 0;
		let mut skip = offset;
		for extent in &self.extents {
			if copied == dst.len() {
				break;
			}
			if skip >= extent.len {
				skip -= extent.len;
				continue;
			}
			let len = (extent.len - skip).min(dst.len() - copied);
			let start = usize::try_from(extent.offset)
				.expect("an extent offset fits a usize")
				.checked_add(skip)
				.expect("the span offset stays inside a usize");
			self.buffer.copy_to(start, &mut dst[copied..copied + len]);
			copied += len;
			skip = 0;
		}
	}

	/// Copies `src.len()` bytes from `src` into the span starting `offset` bytes
	/// into it.
	///
	/// Panics if the range is outside the span or an extent does not resolve.
	pub fn copy_from(&self, offset: usize, src: &[u8]) {
		self.extent_range(offset, src.len());
		let mut copied = 0;
		let mut skip = offset;
		for extent in &self.extents {
			if copied == src.len() {
				break;
			}
			if skip >= extent.len {
				skip -= extent.len;
				continue;
			}
			let len = (extent.len - skip).min(src.len() - copied);
			let start = usize::try_from(extent.offset)
				.expect("an extent offset fits a usize")
				.checked_add(skip)
				.expect("the span offset stays inside a usize");
			self.buffer.copy_from(start, &src[copied..copied + len]);
			copied += len;
			skip = 0;
		}
	}

	/// Copies all `self.len()` bytes of the span into a `Vec<u8>`.
	pub fn to_vec(&self) -> Vec<u8> {
		let mut out = vec![0; self.len()];
		self.copy_to(0, &mut out);
		out
	}
}

/// Bytes in the pages a [`BufferPool`] aligns to and pre-faults.
const PAGE_SIZE: usize = 4096;

/// A fixed set of page-aligned, pre-faulted blocks handed out as [`Buffer`]s.
///
/// A pool avoids the allocation, the zeroing, and the page faults a fresh
/// buffer would cost on every operation: its arena is aligned to a 4096-byte
/// page (and so is every block when `block_size` is a multiple of it, which
/// `O_DIRECT` needs), and every page is touched once when the pool is created,
/// so a transfer never faults a page in. A block goes back to the pool when the
/// last [`Buffer`] — and every [`Span`] cloned from it, including one a reactor
/// holds while the kernel is still transferring through it — drops.
///
/// ```
/// # use completion_backend::BufferPool;
/// let pool = BufferPool::new(4096, 16);
/// let buffer = pool.get();
/// assert_eq!(buffer.len(), 4096);
/// drop(buffer);
/// assert_eq!(pool.available(), 16, "the block returned itself");
/// ```
#[derive(Clone)]
pub struct BufferPool(Arc<PoolInner>);

struct PoolInner {
	/// Base of the arena, aligned to [`PAGE_SIZE`].
	ptr: *mut u8,
	total: usize,
	block_size: usize,
	capacity: usize,
	/// Indices of the blocks not handed out, most recently returned first.
	free: Mutex<Vec<usize>>,
}

// SAFETY: the arena is owned by the pool and every block in it is disjoint, so
// a slot's `iovec` hands out memory no other slot names; the free list guards
// its own state.
unsafe impl Send for PoolInner {}
// SAFETY: see the `Send` impl: `&PoolInner` only reaches memory through
// `iovec`, which bounds-checks against the arena.
unsafe impl Sync for PoolInner {}

impl Drop for PoolInner {
	fn drop(&mut self) {
		// SAFETY: the arena was allocated with this exact layout and is freed
		// once, here.
		unsafe {
			dealloc(
				self.ptr,
				Layout::from_size_align(self.total, PAGE_SIZE).unwrap(),
			)
		}
	}
}

// SAFETY: every extent is bounds-checked against the arena, which lives as long
// as `&self`.
unsafe impl Memory for PoolInner {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec> {
		let (start, len) = check_extent(extent, self.total)?;
		// SAFETY: `start..start + len` is inside the arena, checked above.
		Ok(unsafe { IoVec::new(self.ptr.add(start), len) })
	}

	fn len(&self) -> usize {
		self.total
	}
}

/// One block of a [`BufferPool`], in the shape an operation names memory.
struct Slot {
	pool: Arc<PoolInner>,
	offset: usize,
}

impl Drop for Slot {
	fn drop(&mut self) {
		// Every slot offset is a multiple of the block size, so dividing it
		// recovers the index `BufferPool::try_get` popped.
		self.pool
			.free
			.lock()
			.unwrap()
			.push(self.offset / self.pool.block_size);
	}
}

// SAFETY: the slot's range is inside the arena, which the pool keeps alive;
// `check_extent` keeps every resolved extent inside its own block.
unsafe impl Memory for Slot {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec> {
		let (start, len) = check_extent(extent, self.pool.block_size)?;
		// SAFETY: `offset + start .. offset + start + len` is inside the arena,
		// and inside this slot's own block, both checked above.
		Ok(unsafe { IoVec::new(self.pool.ptr.add(self.offset + start), len) })
	}

	fn len(&self) -> usize {
		self.pool.block_size
	}
}

impl BufferPool {
	/// Allocates a pool of `capacity` blocks of `block_size` bytes each.
	///
	/// The arena is one contiguous allocation aligned to 4096 bytes. Each block
	/// is page-aligned when `block_size` is a multiple of 4096.
	///
	/// Panics if `block_size` or `capacity` is zero, if their product overflows
	/// `isize::MAX`, or if the allocator fails.
	pub fn new(block_size: usize, capacity: usize) -> BufferPool {
		assert!(block_size > 0, "a pool block holds at least one byte");
		assert!(capacity > 0, "a pool holds at least one block");
		let total = block_size
			.checked_mul(capacity)
			.filter(|&total| total <= isize::MAX as usize)
			.expect("a pool's blocks must fit one allocation");
		let layout =
			Layout::from_size_align(total, PAGE_SIZE).expect("the pool layout is representable");
		// SAFETY: `total` is non-zero, so the layout names a real allocation.
		let ptr = unsafe { alloc_zeroed(layout) };
		if ptr.is_null() {
			handle_alloc_error(layout);
		}
		// `alloc_zeroed` may hand back lazily zeroed pages — `calloc`'s large
		// path or an `mmap` mapping — so the bytes are zero but their pages are
		// not there yet, and the first transfer into each one would take a page
		// fault. Write one byte per page now, when the cost belongs to pool
		// creation rather than to I/O.
		// SAFETY: `ptr` names `total` writable bytes; the loop and the last byte
		// both stay inside them.
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

	/// Checks out a block, or `None` if every block is in use.
	pub fn try_get(&self) -> Option<Buffer> {
		let slot = self.0.free.lock().unwrap().pop()?;
		Some(self.buffer(slot))
	}

	/// Checks out a block.
	///
	/// Falls back to a standalone zeroed [`Buffer`] when every block is in use,
	/// so a burst above `capacity` never fails or blocks; that buffer simply is
	/// not pooled.
	pub fn get(&self) -> Buffer {
		match self.try_get() {
			Some(buffer) => buffer,
			None => Buffer::zeroed(self.block_size()),
		}
	}

	/// The [`Buffer`] for the block at `slot`.
	fn buffer(&self, slot: usize) -> Buffer {
		Buffer::new(Arc::new(Slot {
			offset: slot * self.0.block_size,
			pool: Arc::clone(&self.0),
		}))
	}

	/// Byte length of one block.
	pub fn block_size(&self) -> usize {
		self.0.block_size
	}

	/// Number of blocks the pool holds.
	pub fn capacity(&self) -> usize {
		self.0.capacity
	}

	/// Number of blocks currently checked in.
	pub fn available(&self) -> usize {
		self.0.free.lock().unwrap().len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_span_of_at_most_one_extent_resolves_without_an_array() {
		let buffer = Buffer::zeroed(8);

		let empty = buffer
			.gather::<Extent>([])
			.resolve_single()
			.expect("an empty span is one empty buffer")
			.expect("and it resolves");
		assert_eq!(empty.as_ptr(), ptr::null_mut());
		assert_eq!(empty.len(), 0);

		let single = buffer.span(2, 3).resolve_single().expect("one extent");
		assert_eq!(single.expect("it resolves").len(), 3);

		let mut out = vec![empty];
		buffer
			.gather([(0, 1), (2, 3)])
			.resolve_into(&mut out)
			.unwrap();
		assert_eq!(out.len(), 2, "the array is cleared before it is filled");
		assert!(
			buffer.gather([(0, 1), (2, 3)]).resolve_single().is_none(),
			"more than one extent needs an array"
		);
	}

	#[test]
	fn an_extent_outside_the_buffer_refuses_the_span() {
		let buffer = Buffer::zeroed(8);

		assert!(buffer.span(4, 5).resolve_into(&mut Vec::new()).is_err());
		assert!(
			buffer
				.span(4, 5)
				.resolve_single()
				.expect("one extent")
				.is_err()
		);
	}

	#[test]
	fn a_span_copies_bytes_across_its_extents() {
		let buffer = Buffer::from_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);
		let span = buffer.gather([(0u64, 2usize), (4, 3)]);

		let mut out = [0u8; 5];
		span.copy_to(0, &mut out);
		assert_eq!(out, [0, 1, 4, 5, 6]);

		span.copy_from(1, &[9, 8, 7, 6]);
		assert_eq!(buffer.to_vec(), [0, 9, 2, 3, 8, 7, 6, 7]);
		assert_eq!(span.to_vec(), [0, 9, 8, 7, 6]);
	}

	/// The address a buffer's own memory starts at.
	fn address(buffer: &Buffer) -> usize {
		buffer
			.0
			.memory()
			.iovec(Extent { offset: 0, len: 1 })
			.unwrap()
			.as_ptr() as usize
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

	#[test]
	fn a_buffer_gives_its_bytes_back_once_it_is_the_only_holder() {
		let buffer = Buffer::from_slice(b"hello");
		let held = buffer.clone();
		let buffer = buffer
			.try_into_vec()
			.expect_err("a clone still holds the bytes");
		drop(held);
		assert_eq!(buffer.try_into_vec().ok(), Some(b"hello".to_vec()));

		let buffer = Buffer::from_slice(b"pooled");
		let span = buffer.span(0, 6);
		let buffer = buffer
			.try_into_vec()
			.expect_err("a span still names the bytes");
		assert_eq!(span.to_vec(), b"pooled", "the span still reads them");
		drop(span);
		assert_eq!(buffer.try_into_vec().ok(), Some(b"pooled".to_vec()));
	}

	#[test]
	fn a_caller_s_memory_has_no_bytes_to_hand_back() {
		/// A [`Memory`] the test owns, in no shape this crate can recover.
		struct Custom(Vec<u8>);

		// SAFETY: the bytes outlive the memory, and `check_extent` refuses an
		// extent that does not fit them.
		unsafe impl Memory for Custom {
			fn iovec(&self, extent: Extent) -> io::Result<IoVec> {
				let (start, len) = check_extent(extent, self.0.len())?;
				// SAFETY: `start..start + len` is inside the vector, which the
				// buffer keeps alive.
				Ok(unsafe { IoVec::new(self.0.as_ptr().add(start).cast_mut(), len) })
			}

			fn len(&self) -> usize {
				self.0.len()
			}
		}

		let buffer = Buffer::new(Arc::new(Custom(b"custom".to_vec())));
		let buffer = buffer
			.try_into_vec()
			.expect_err("a caller's memory is not a block to hand back");
		assert_eq!(buffer.to_vec(), b"custom");
	}
}
