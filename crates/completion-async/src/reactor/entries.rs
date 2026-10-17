//! Generational table tracking in-flight operations and timers by `u64` id.
//!
//! [`SlotMap`] hands out a key per insertion that packs a slot index and a
//! generation, and moves the generation on whenever the slot is taken again.
//! Stale keys or duplicate completions therefore cannot match a newly inserted
//! entry. The key's FFI form is the id the backend carries: an occupied slot's
//! version has its low bit set, so that id is never `0` and — the index staying
//! below `u32::MAX` — never `u64::MAX`, the two tokens `io_uring` reserves.

use std::sync::Arc;
use std::task::Waker;
use std::time::Instant;

use completion_backend::{AsDescriptor, Buffer, Error, Response};
use slotmap::{DefaultKey, Key, KeyData, SlotMap};

/// State of an in-flight operation or timer in the reactor table.
pub(super) struct Entry {
	pub(super) response: Option<Result<Response, Error>>,
	pub(super) waker: Option<Waker>,
	/// True if the future was dropped before taking the result; the entry will be
	/// removed as soon as the backend reports completion.
	pub(super) detached: bool,
	pub(super) cancelled: bool,
	pub(super) timer: Option<Instant>,
	/// Keeps the descriptor open while the operation is in flight in the backend.
	pub(super) _descriptor: Option<Arc<dyn AsDescriptor>>,
	/// Keeps the backing memory alive while the operation is in flight in the backend.
	pub(super) buffer: Option<Buffer>,
}

impl Entry {
	pub(super) fn is_done(&self) -> bool {
		self.response.is_some()
	}
}

/// The operations and timers the reactor holds.
pub(super) struct Entries {
	slots: SlotMap<DefaultKey, Entry>,
}

impl Entries {
	pub(super) fn new() -> Entries {
		Entries {
			slots: SlotMap::new(),
		}
	}

	/// Puts `entry` in a slot and returns the id that names it. A slot the
	/// reactor let go of earlier is used again, one generation on.
	pub(super) fn insert(&mut self, entry: Entry) -> u64 {
		self.slots.insert(entry).data().as_ffi()
	}

	/// The entry `id` names, mutably.
	pub(super) fn get_mut(&mut self, id: u64) -> Option<&mut Entry> {
		self.slots.get_mut(key(id))
	}

	/// Takes the entry `id` names out of the table. The slot goes back one
	/// generation on: an id the operation left behind names nothing here.
	pub(super) fn remove(&mut self, id: u64) -> Option<Entry> {
		self.slots.remove(key(id))
	}

	/// Writes a result into the entry `id` names, which is where its holder
	/// reads it back from, and hands back whoever waits to be told.
	///
	/// `None` when the id names nothing, or names an operation that already has
	/// its result: a mechanism can report the same operation twice. It is also
	/// `None` when the holder is not waiting yet, and when the holder gave the
	/// result up, in which case the operation ends here.
	pub(super) fn land(&mut self, id: u64, response: Result<Response, Error>) -> Option<Waker> {
		let entry = self.slots.get_mut(key(id))?;
		if entry.is_done() {
			return None;
		}
		if !entry.detached {
			entry.response = Some(response);
			entry.buffer = None;
			return entry.waker.take();
		}
		// The holder gave the result up: nothing waits for it, and the
		// operation ends here rather than sitting in the table until it is
		// taken.
		self.slots.remove(key(id));
		None
	}

	/// Every operation and timer the table holds.
	pub(super) fn iter(&self) -> impl Iterator<Item = (u64, &Entry)> {
		self.slots
			.iter()
			.map(|(key, entry)| (key.data().as_ffi(), entry))
	}
}

/// The key `id` names. An id the table never handed out, or one whose slot has
/// been taken again since, names no entry.
fn key(id: u64) -> DefaultKey {
	DefaultKey::from(KeyData::from_ffi(id))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn entry() -> Entry {
		Entry {
			response: None,
			waker: None,
			detached: false,
			cancelled: false,
			timer: None,
			_descriptor: None,
			buffer: None,
		}
	}

	/// The slot an id names, the part the generation shares across a reuse.
	fn index(id: u64) -> u64 {
		KeyData::from_ffi(id).as_ffi() & u64::from(u32::MAX)
	}

	/// The generation an id carries, which moves on every reuse.
	fn version(id: u64) -> u64 {
		KeyData::from_ffi(id).as_ffi() >> 32
	}

	/// A slot going back and forth is one index with a new generation, so the id
	/// of the operation that had it names nothing afterwards.
	#[test]
	fn a_reused_slot_is_a_new_id_and_the_old_one_names_nothing() {
		let mut entries = Entries::new();
		let first = entries.insert(entry());
		assert!(entries.get_mut(first).is_some());
		assert_eq!(entries.remove(first).map(|_| ()), Some(()));
		assert!(
			entries.get_mut(first).is_none(),
			"a removed id names nothing"
		);

		let second = entries.insert(entry());
		assert_ne!(first, second);
		assert!(entries.get_mut(second).is_some());
		assert!(
			entries.get_mut(first).is_none(),
			"the id of the operation that had the slot is not the new one"
		);
	}

	/// A slot is taken again one generation on: the id changes, the index stays,
	/// and the old id no longer resolves.
	#[test]
	fn a_slot_spends_one_generation_per_operation() {
		let mut entries = Entries::new();
		let first = entries.insert(entry());
		entries.remove(first).unwrap();
		let second = entries.insert(entry());
		assert_eq!(index(first), index(second), "the same slot held both");
		assert_eq!(
			version(second),
			version(first) + 2,
			"vacating and taking the slot each move the generation"
		);
		assert!(
			entries.get_mut(first).is_none(),
			"the id of the operation that had the slot is not the new one"
		);
	}

	/// No id is ever a token the ring keeps for itself, however often the slot
	/// is reused.
	#[test]
	fn an_id_is_never_zero_or_the_futures_token() {
		let mut entries = Entries::new();
		let mut id = entries.insert(entry());
		for _ in 0..64 {
			assert_ne!(id, 0);
			assert_ne!(id, u64::MAX);
			entries.remove(id).unwrap();
			id = entries.insert(entry());
		}
	}

	/// Ids keep naming their own operation while the table is under way, and
	/// only that one.
	#[test]
	fn every_id_names_one_operation_until_it_is_removed() {
		let mut entries = Entries::new();
		let ids: Vec<u64> = (0..8).map(|_| entries.insert(entry())).collect();
		for (index, id) in ids.iter().enumerate() {
			assert!(entries.get_mut(*id).is_some(), "id {index} lost its entry");
		}
		entries.remove(ids[3]).unwrap();
		assert!(entries.get_mut(ids[3]).is_none());
		for (index, id) in ids.iter().enumerate() {
			if index != 3 {
				assert!(entries.get_mut(*id).is_some(), "id {index} lost its entry");
			}
		}
	}

	/// A landing writes the result where the holder reads it, and hands back
	/// the waker to be woken.
	#[test]
	fn a_landed_result_waits_for_its_holder() {
		let mut entries = Entries::new();
		let id = entries.insert(entry());
		let waker = Waker::noop().clone();
		entries.get_mut(id).unwrap().waker = Some(waker.clone());
		let landed_waker = entries
			.land(id, Ok(Response::Done))
			.expect("someone was waiting for the result");
		assert!(landed_waker.will_wake(&waker));
		let landed = entries.get_mut(id).unwrap();
		assert!(matches!(landed.response, Some(Ok(Response::Done))));
		assert!(landed.is_done());
		assert!(landed.waker.is_none(), "the waker was taken to be woken");
	}

	/// A landing on an operation whose holder gave the result up ends the
	/// operation: no result is kept, and nobody is told one arrived.
	#[test]
	fn a_landed_result_nobody_waits_for_ends_the_operation() {
		let mut entries = Entries::new();
		let id = entries.insert(entry());
		let held = entries.get_mut(id).unwrap();
		held.detached = true;
		held.waker = Some(Waker::noop().clone());
		assert!(entries.land(id, Ok(Response::Done)).is_none());
		assert!(
			entries.get_mut(id).is_none(),
			"the operation ends with the result nobody wanted"
		);
	}

	/// A mechanism can report the same operation twice, and can report an
	/// operation the reactor has let go of: neither is a result.
	#[test]
	fn a_second_landing_or_a_stale_id_is_not_a_result() {
		let mut entries = Entries::new();
		let id = entries.insert(entry());
		assert!(entries.land(id, Ok(Response::Count(1))).is_none());
		assert!(
			entries.land(id, Ok(Response::Count(2))).is_none(),
			"the result is already in"
		);
		entries.remove(id).unwrap();
		assert!(
			entries.land(id, Ok(Response::Done)).is_none(),
			"the id names nothing"
		);
	}

	/// A table that handed every id back names no operation again, and every
	/// slot is vacant.
	#[test]
	fn a_table_with_nothing_in_it_iterates_nothing() {
		let mut entries = Entries::new();
		assert_eq!(entries.iter().count(), 0);
		let ids: Vec<u64> = (0..4).map(|_| entries.insert(entry())).collect();
		assert_eq!(entries.iter().count(), 4);
		for id in ids {
			entries.remove(id).unwrap();
		}
		assert_eq!(entries.iter().count(), 0);
	}
}
