//! The memory an operation transfers through: a space bound or lent once, and
//! the span of it each operation names. The low layer holds the data; these add
//! the lifetime a lent space carries.

use std::marker::PhantomData;

use completion_rt::Extent;

/// Memory bound to an executor ([`Executor::bind`](crate::Executor::bind)) or
/// lent to a scope ([`Scope::lend`](crate::Scope::lend)), for its operations
/// to name spans of.
///
/// A handle: clones are cheap and name the same memory. It stays on the
/// executor's thread, and an operation on another executor refuses it.
#[derive(Clone)]
pub struct Space<'a> {
	inner: completion_rt::Space,
	borrow: PhantomData<&'a ()>,
}

impl<'a> Space<'a> {
	pub(crate) fn new(inner: completion_rt::Space) -> Space<'a> {
		Self {
			inner,
			borrow: PhantomData,
		}
	}

	/// The bytes the memory holds.
	pub fn len(&self) -> usize {
		self.inner.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// `len` bytes at `offset`. The range is checked when an operation is
	/// submitted, not here.
	pub fn span(&self, offset: u64, len: usize) -> Span<'a> {
		Span::new(self.inner.span(offset, len))
	}

	/// All of the memory.
	pub fn whole(&self) -> Span<'a> {
		Span::new(self.inner.whole())
	}

	/// The ranges `extents`, in order, as one span: an operation reads or
	/// writes them as if they were contiguous.
	pub fn gather<E: Into<Extent>>(&self, extents: impl IntoIterator<Item = E>) -> Span<'a> {
		Span::new(self.inner.gather(extents))
	}
}

/// The ranges of a [`Space`] one operation transfers through.
///
/// It holds the space, so it lives as long as the memory, not as long as the
/// [`Space`] handle it was taken from.
#[derive(Clone)]
pub struct Span<'a> {
	inner: completion_rt::Span,
	borrow: PhantomData<&'a ()>,
}

impl<'a> Span<'a> {
	fn new(inner: completion_rt::Span) -> Span<'a> {
		Self {
			inner,
			borrow: PhantomData,
		}
	}

	/// The low layer's span, which an operation is submitted with.
	pub(crate) fn low(&self) -> &completion_rt::Span {
		&self.inner
	}

	/// The bytes the span covers.
	pub fn len(&self) -> usize {
		self.inner.len()
	}

	pub fn is_empty(&self) -> bool {
		self.inner.is_empty()
	}

	/// The span without its first `bytes`.
	pub fn skip(self, bytes: usize) -> Span<'a> {
		Span::new(self.inner.skip(bytes))
	}

	/// The span's first `bytes`, or all of it if it is shorter.
	pub fn take(self, bytes: usize) -> Span<'a> {
		Span::new(self.inner.take(bytes))
	}
}
