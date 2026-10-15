//! Memory a transfer goes through: the extent, the iovec, and the address
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

impl From<(u64, usize)> for Extent {
	fn from((offset, len): (u64, usize)) -> Extent {
		Extent { offset, len }
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Extents(SmallVec<[Extent; 2]>);

impl Extents {
	pub(crate) fn new() -> Extents {
		Extents(SmallVec::new())
	}

	pub fn from_slice(extents: &[Extent]) -> Extents {
		Extents(SmallVec::from_slice(extents))
	}

	pub(crate) fn push(&mut self, extent: Extent) {
		self.0.push(extent);
	}

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

/// One range the kernel reads or writes, ABI-compatible with `libc::iovec`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IoVec {
	ptr: *mut u8,
	len: usize,
}

impl IoVec {
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

impl std::fmt::Debug for IoVec {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("IoVec")
			.field("ptr", &self.ptr)
			.field("len", &self.len)
			.finish()
	}
}

unsafe impl Send for IoVec {}

pub(crate) type IoVecs = Vec<IoVec>;

/// An address space operations transfer through, shared with the backend
/// rather than owned by it: a guest's RAM or a mapping.
///
/// # Safety
/// The contents may change under any holder at any time — the kernel, another
/// thread, a guest — so nothing may read or write them through an ordinary
/// Rust reference: only through a resolved [`IoVec`], or a volatile copy.
///
/// [`iovec`](Memory::iovec) resolves an extent to an address that stays valid
/// and keeps naming the same bytes for as long as the memory is bound or lent,
/// and fails with [`InvalidInput`](io::ErrorKind::InvalidInput) for an extent
/// outside the memory.
pub unsafe trait Memory: Send + Sync {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec>;

	fn len(&self) -> usize;
}

pub(crate) fn iovecs<T: Memory + ?Sized>(memory: &T, extents: &[Extent]) -> io::Result<IoVecs> {
	extents.iter().map(|extent| memory.iovec(*extent)).collect()
}

unsafe impl<T: Memory + ?Sized> Memory for Arc<T> {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec> {
		(**self).iovec(extent)
	}

	fn len(&self) -> usize {
		(**self).len()
	}
}

unsafe impl<T: Memory + ?Sized> Memory for &T {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec> {
		(**self).iovec(extent)
	}

	fn len(&self) -> usize {
		(**self).len()
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;

	/// A ring entry points at a submission's span array while the entry waits
	/// in the ring, so that address has to survive the value moving — into the
	/// in-flight table, and every rehash after that. An inline container moves
	/// its elements with itself, which is why [`IoVecs`] is a heap container.
	#[test]
	fn an_iovec_array_keeps_its_address_across_a_move() {
		let iovec = || unsafe { IoVec::new(std::ptr::null_mut(), 1) };
		let mut iovecs: IoVecs = IoVecs::new();
		iovecs.push(iovec());
		let address = iovecs.as_ptr();

		let mut table: HashMap<u64, IoVecs> = HashMap::new();
		table.insert(1, iovecs);
		// Enough entries to grow the table several times over.
		for token in 2..64 {
			let mut other: IoVecs = IoVecs::new();
			other.push(iovec());
			table.insert(token, other);
		}

		assert_eq!(
			address,
			table.get(&1).expect("the entry").as_ptr(),
			"the address an entry captured must not move with the value"
		);
	}
}
