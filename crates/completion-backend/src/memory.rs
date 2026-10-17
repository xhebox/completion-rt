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

	pub(crate) fn len(&self) -> usize {
		self.len
	}

	pub(crate) fn as_ptr(&self) -> *mut u8 {
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
/// and keeps naming the same bytes for as long as the memory is bound, and
/// fails with [`InvalidInput`](io::ErrorKind::InvalidInput) for an extent
/// outside the memory.
pub unsafe trait Memory: Send + Sync {
	fn iovec(&self, extent: Extent) -> io::Result<IoVec>;

	fn len(&self) -> usize;
}

/// The memory an operation transfers through: bound once ([`Space::new`]),
/// and sliced into [`Span`]s for each transfer.
///
/// A handle: clones are cheap and name the same memory.
#[derive(Clone)]
pub struct Space(Arc<dyn Memory>);

impl Space {
	/// A space over `memory`, kept alive until the space and every operation
	/// naming it are gone.
	pub fn new(memory: Arc<dyn Memory>) -> Space {
		Space(memory)
	}

	/// The bytes the memory holds.
	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// `len` bytes at `offset`. The range is checked when an operation is
	/// submitted, not here.
	pub fn span(&self, offset: u64, len: usize) -> Span {
		self.gather([Extent { offset, len }])
	}

	/// All of the memory.
	pub fn whole(&self) -> Span {
		self.span(0, self.len())
	}

	/// The ranges `extents`, in order, as one span: an operation reads or
	/// writes them as if they were contiguous.
	pub fn gather<E: Into<Extent>>(&self, extents: impl IntoIterator<Item = E>) -> Span {
		Span {
			space: self.clone(),
			extents: extents.into_iter().map(Into::into).collect(),
		}
	}
}

/// The ranges of a [`Space`] one operation transfers through.
///
/// It holds the space, so it lives as long as the memory, not as long as the
/// handle it was taken from.
#[derive(Clone)]
pub struct Span {
	space: Space,
	extents: SmallVec<[Extent; 2]>,
}

impl Span {
	/// The space the ranges are of.
	pub fn space(&self) -> &Space {
		&self.space
	}

	/// The ranges, in order.
	pub fn extents(&self) -> &[Extent] {
		&self.extents
	}

	/// The addresses the span's extents resolve to, in order.
	pub(crate) fn resolve(&self) -> io::Result<IoVecs> {
		let memory = self.space.0.as_ref();
		self.extents
			.iter()
			.map(|extent| memory.iovec(*extent))
			.collect()
	}

	/// The bytes the span covers.
	pub fn len(&self) -> usize {
		self.extents
			.iter()
			.fold(0, |len, extent| len.saturating_add(extent.len))
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// The span without its first `bytes`.
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

	/// The span's first `bytes`, or all of it if it is shorter.
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
