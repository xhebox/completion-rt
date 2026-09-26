//! Memory a transfer goes through: the extent, the span, and the address
//! space that resolves one into the other.

use std::io;
use std::sync::Arc;

use smallvec::SmallVec;

/// Range of a span on the address space.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Extent {
	pub offset: u64,
	pub len: usize,
}

/// Extents, inlined for at most two items.
///
/// `len` counts the extents; [`Extents::len_bytes`] is what they carry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Extents(SmallVec<[Extent; 2]>);

impl Extents {
	pub fn new() -> Extents {
		Extents(SmallVec::new())
	}

	pub fn from_slice(extents: &[Extent]) -> Extents {
		Extents(SmallVec::from_slice(extents))
	}

	pub fn push(&mut self, extent: Extent) {
		self.0.push(extent);
	}

	/// The extents that follow the first `bytes` bytes of this set: what a
	/// transfer that stopped partway still has to move.
	pub fn skip(&self, bytes: usize) -> Extents {
		let mut remaining = bytes;
		let mut out = Extents::new();
		for extent in &self.0 {
			if remaining >= extent.len {
				remaining -= extent.len;
				continue;
			}
			out.push(Extent {
				offset: extent.offset + remaining as u64,
				len: extent.len - remaining,
			});
			remaining = 0;
		}
		out
	}

	/// The bytes the extents cover, in total.
	pub fn len_bytes(&self) -> usize {
		self.0
			.iter()
			.fold(0, |len, extent| len.saturating_add(extent.len))
	}
}

impl std::ops::Deref for Extents {
	type Target = [Extent];

	fn deref(&self) -> &[Extent] {
		&self.0
	}
}

impl Extend<Extent> for Extents {
	fn extend<T: IntoIterator<Item = Extent>>(&mut self, extents: T) {
		self.0.extend(extents);
	}
}

impl FromIterator<Extent> for Extents {
	fn from_iter<T: IntoIterator<Item = Extent>>(extents: T) -> Extents {
		Extents(extents.into_iter().collect())
	}
}

impl IntoIterator for Extents {
	type Item = Extent;
	type IntoIter = smallvec::IntoIter<[Extent; 2]>;

	fn into_iter(self) -> smallvec::IntoIter<[Extent; 2]> {
		self.0.into_iter()
	}
}

impl<'a> IntoIterator for &'a Extents {
	type Item = &'a Extent;
	type IntoIter = std::slice::Iter<'a, Extent>;

	fn into_iter(self) -> std::slice::Iter<'a, Extent> {
		self.iter()
	}
}

/// One span the kernel reads or writes, ABI-compatible with `libc::iovec`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Span {
	ptr: *mut u8,
	len: usize,
}

impl Span {
	pub unsafe fn new(ptr: *mut u8, len: usize) -> Self {
		Self { ptr, len }
	}

	pub fn len(&self) -> usize {
		self.len
	}

	pub fn as_ptr(&self) -> *mut u8 {
		self.ptr
	}
}

impl std::fmt::Debug for Span {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Span")
			.field("ptr", &self.ptr)
			.field("len", &self.len)
			.finish()
	}
}

unsafe impl Send for Span {}

/// A submission's resolved spans, in a heap allocation its queued ring entry
/// points at.
pub type Spans = Vec<Span>;

/// A buffer an operation transfers through, shared with the backend rather
/// than owned by it.
///
/// # Safety
/// Implementations must allow external writes through a shared reference without breaking it.
pub unsafe trait Memory: Send + Sync {
	fn span(&self, extent: Extent) -> io::Result<Span>;

	/// How many bytes the address space holds.
	fn len(&self) -> usize;
}

/// Spans named by `extents`, resolved in order.
///
/// Resolving a list is the backend's business, not a caller's; the trait
/// keeps the one extent a caller names.
pub fn spans<T: Memory + ?Sized>(memory: &T, extents: &[Extent]) -> io::Result<Spans> {
	extents.iter().map(|extent| memory.span(*extent)).collect()
}

unsafe impl<T: Memory + ?Sized> Memory for Arc<T> {
	fn span(&self, extent: Extent) -> io::Result<Span> {
		(**self).span(extent)
	}

	fn len(&self) -> usize {
		(**self).len()
	}
}

unsafe impl Memory for Vec<u8> {
	fn len(&self) -> usize {
		Vec::len(self)
	}

	fn span(&self, extent: Extent) -> io::Result<Span> {
		let end = extent
			.offset
			.checked_add(extent.len as u64)
			.filter(|end| *end <= self.len() as u64)
			.ok_or_else(|| {
				io::Error::new(io::ErrorKind::InvalidInput, "range outside the buffer")
			})?;
		let _ = end;
		Ok(unsafe {
			Span::new(
				self.as_ptr().add(extent.offset as usize).cast_mut(),
				extent.len,
			)
		})
	}
}

unsafe impl Memory for Box<[u8]> {
	fn len(&self) -> usize {
		<[u8]>::len(self)
	}

	fn span(&self, extent: Extent) -> io::Result<Span> {
		let end = extent
			.offset
			.checked_add(extent.len as u64)
			.filter(|end| *end <= self.len() as u64)
			.ok_or_else(|| {
				io::Error::new(io::ErrorKind::InvalidInput, "range outside the buffer")
			})?;
		let _ = end;
		Ok(unsafe {
			Span::new(
				self.as_ptr().add(extent.offset as usize).cast_mut(),
				extent.len,
			)
		})
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;

	/// A ring entry points at a submission's span array while the entry waits
	/// in the ring, so that address has to survive the value moving — into the
	/// in-flight table, and every rehash after that. An inline container moves
	/// its elements with itself, which is why [`Spans`] is a heap container.
	#[test]
	fn a_span_array_keeps_its_address_across_a_move() {
		let span = || unsafe { Span::new(std::ptr::null_mut(), 1) };
		let mut spans: Spans = Spans::new();
		spans.push(span());
		let address = spans.as_ptr();

		let mut table: HashMap<u64, Spans> = HashMap::new();
		table.insert(1, spans);
		// Enough entries to grow the table several times over.
		for token in 2..64 {
			let mut other: Spans = Spans::new();
			other.push(span());
			table.insert(token, other);
		}

		assert_eq!(
			address,
			table.get(&1).expect("the entry").as_ptr(),
			"the address an entry captured must not move with the value"
		);
	}
}
