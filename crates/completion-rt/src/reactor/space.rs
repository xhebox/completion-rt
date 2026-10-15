//! The memory operations name, and the leases borrowed memory is lent under.

use std::cell::Cell;
use std::io;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::error::Error;
use crate::memory::{Extent, Extents, IoVecs, Memory, iovecs};

use super::Reactor;
use super::Shared;
use super::token::InFlightState;

/// Groups the operations naming memory a scope borrowed, so the scope can wait
/// until the kernel has let go of all of them before the borrow ends.
pub struct Lease {
	id: u64,
	/// Set by [`Reactor::settle`]; a submission under a closed lease is
	/// refused, so nothing joins after the barrier has started counting. Shared
	/// with the spaces lent under the lease.
	closed: Rc<Cell<bool>>,
	/// The reactor the lease belongs to: only its own reactor is driven by the
	/// barrier, so a submission elsewhere could outlive it.
	shared: Rc<Shared>,
}

impl Reactor {
	/// Opens a lease for the operations a scope makes on memory it borrowed.
	/// Submissions under it are refused once it is [`settle`](Reactor::settle)d,
	/// and the scope waits on `settle` before the borrow ends.
	pub fn lease(&self) -> Lease {
		Lease {
			id: self.shared.mint(),
			closed: Rc::new(Cell::new(false)),
			shared: Rc::clone(&self.shared),
		}
	}

	/// Closes `lease` to new submissions and asks every operation under it to
	/// give up. Returns whether one is still pending or in the kernel; the caller
	/// drives the reactor and asks again until it is not.
	pub fn settle(&self, lease: &Lease) -> bool {
		let (busy, pending) = {
			let mut events = self.shared.events.borrow_mut();
			lease.closed.set(true);
			let (mut busy, mut pending) = (false, false);
			for entry in events.entries.values_mut() {
				if entry.lease != Some(lease.id) {
					continue;
				}
				if entry.state != InFlightState::Done {
					entry.give_up.ask();
					busy = true;
					pending |= entry.state == InFlightState::Pending;
				}
			}
			(busy, pending)
		};
		// A pending entry is settled by the drain alone, with no completion to wake
		// the wait after it; one in the kernel is what the wait sleeps for.
		if pending {
			self.shared.park.unpark();
		}
		busy
	}

	/// Whether `space` may name memory for an operation on this reactor.
	pub(super) fn admit(&self, space: &Space) -> Result<(), Error> {
		// A lease's barrier drives only its own reactor, and only that reactor's
		// entries hold the space: a submission elsewhere could outlive both.
		if !std::ptr::eq(space.0.reactor.as_ptr(), Rc::as_ptr(&self.shared)) {
			return Err(Error::from(io::Error::new(
				io::ErrorKind::InvalidInput,
				"memory submitted to a reactor other than the one it was bound or lent on",
			)));
		}
		// Checked before the extents are resolved against memory whose borrow may
		// have ended.
		if space.closed() {
			return Err(lease_closed());
		}
		Ok(())
	}

	/// A space over `memory`, kept alive until the space and every operation
	/// naming it are gone.
	pub fn bind(&self, memory: Arc<dyn Memory>) -> Space {
		Space(Rc::new(SpaceInner {
			memory: Arc::as_ptr(&memory),
			_owner: Some(memory),
			lease: None,
			reactor: Rc::downgrade(&self.shared),
		}))
	}

	/// A space over borrowed `memory`, its operations grouped under `lease`.
	///
	/// # Safety
	/// The caller settles `lease` ([`Reactor::settle`] until it reports nothing
	/// outstanding) before `memory`'s borrow ends.
	///
	/// # Panics
	/// If `lease` was opened on another reactor.
	pub unsafe fn lend(&self, memory: &dyn Memory, lease: &Lease) -> Space {
		assert!(
			Rc::ptr_eq(&lease.shared, &self.shared),
			"a lease lends only on the reactor that opened it"
		);
		// SAFETY: only the lifetime bound changes. The caller settles the lease
		// before the borrow ends, so every operation under it has left the kernel
		// by then, and a closed lease refuses the space before its extents are
		// resolved.
		let memory: *const (dyn Memory + 'static) =
			unsafe { std::mem::transmute(memory as *const dyn Memory) };
		Space(Rc::new(SpaceInner {
			memory,
			_owner: None,
			lease: Some((lease.id, Rc::clone(&lease.closed))),
			reactor: Rc::downgrade(&self.shared),
		}))
	}
}

pub(super) fn lease_closed() -> Error {
	Error::from(io::Error::new(
		io::ErrorKind::BrokenPipe,
		"lent memory submitted after its scope ended",
	))
}

/// The memory a reactor's operations name: bound to it once
/// ([`Reactor::bind`]) or lent under a lease ([`Reactor::lend`]). An operation
/// names [`Extents`] of it.
///
/// A handle: clones are cheap and name the same memory. It stays on the
/// reactor's thread.
#[derive(Clone)]
pub struct Space(Rc<SpaceInner>);

struct SpaceInner {
	/// Valid while `_owner` is held, or until the lease is settled.
	memory: *const (dyn Memory + 'static),
	_owner: Option<Arc<dyn Memory>>,
	/// The lease's id and its closed flag.
	lease: Option<(u64, Rc<Cell<bool>>)>,
	/// Weak: the reactor's entries hold the space.
	reactor: Weak<Shared>,
}

impl Space {
	fn memory(&self) -> &dyn Memory {
		// SAFETY: see `SpaceInner::memory`; a lent space is only read before its
		// lease closes.
		unsafe { &*self.0.memory }
	}

	/// The bytes the memory holds.
	pub fn len(&self) -> usize {
		self.memory().len()
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

	pub(super) fn lease(&self) -> Option<u64> {
		self.0.lease.as_ref().map(|(id, _)| *id)
	}

	pub(super) fn closed(&self) -> bool {
		self.0
			.lease
			.as_ref()
			.is_some_and(|(_, closed)| closed.get())
	}

	pub(super) fn resolve(&self, extents: &Extents) -> io::Result<IoVecs> {
		iovecs(self.memory(), extents)
	}
}

/// The ranges of a [`Space`] one operation transfers through.
///
/// It holds the space, so it lives as long as the memory, not as long as the
/// handle it was taken from.
#[derive(Clone)]
pub struct Span {
	space: Space,
	extents: Extents,
}

impl Span {
	/// The space the ranges are of.
	pub fn space(&self) -> &Space {
		&self.space
	}

	/// The ranges, in order.
	pub fn extents(&self) -> &Extents {
		&self.extents
	}

	/// The bytes the span covers.
	pub fn len(&self) -> usize {
		self.extents.len_bytes()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// The span without its first `bytes`.
	pub fn skip(self, bytes: usize) -> Span {
		Span {
			extents: self.extents.skip(bytes),
			..self
		}
	}

	/// The span's first `bytes`, or all of it if it is shorter.
	pub fn take(self, bytes: usize) -> Span {
		let mut remaining = bytes;
		let mut extents = Extents::new();
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
