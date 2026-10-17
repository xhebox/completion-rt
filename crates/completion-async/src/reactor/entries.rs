//! The table the reactor holds its in-flight operations and timers in: a
//! generational slab whose slots are named by the id the mechanism carries.
//!
//! An id packs the slot's index and its generation: the index is where the
//! entry lives, and the generation says which operation lived there. Reusing a
//! slot moves its generation, so an id that named an operation the reactor has
//! let go of names nothing afterwards — a completion the kernel reports late,
//! or a key a caller kept, finds no entry rather than whichever operation took
//! the slot afterwards.
//!
//! The entries live in the slab. Nothing the mechanism still reads lives in an
//! [`Entry`]: the addresses it works on are what the backend holds for the
//! operation, so an entry may move when the slab grows.

use std::sync::Arc;
use std::task::Waker;
use std::time::Instant;

use completion_backend::{AsDescriptor, Error, Response, Space};

/// The record one submitted operation or timer lives in, from submission until
/// its result is taken or it is given up on.
///
/// What an operation runs is handed straight to the backend, which holds it
/// for as long as the mechanism runs it. A timer names no descriptor and no
/// memory: its [`timer`](Entry::timer) deadline is what the reactor waits for.
pub(super) struct Entry {
	pub(super) response: Option<Result<Response, Error>>,
	/// Set while the holder waits; taken when the result lands.
	pub(super) waker: Option<Waker>,
	/// The holder went without the result; the entry goes when the reactor lets go.
	pub(super) detached: bool,
	pub(super) cancelled: bool,
	/// The deadline a timer waits for, `None` for an operation.
	pub(super) timer: Option<Instant>,
	/// The descriptor the operation names, held until the entry goes: the
	/// backend works on the number, which must stay live until then.
	pub(super) _descriptor: Option<Arc<dyn AsDescriptor>>,
	/// The memory the operation names, held until the operation lands: the
	/// backend works on resolved addresses, which must stay valid until then.
	/// It is `None` exactly when the operation names no caller memory. A
	/// released operation keeps it until the backend reports its completion.
	pub(super) space: Option<Space>,
}

impl Entry {
	pub(super) fn is_done(&self) -> bool {
		self.response.is_some()
	}
}

/// How much of an id names the slot, the rest naming its generation.
const INDEX_BITS: u32 = 32;

/// The lowest and highest generation an id may carry. Zero is left out so no id
/// is the ring's own token (`io_uring` uses 0 and `u64::MAX`), and `u32::MAX` is
/// left out because its high half would make that same `u64::MAX`.
const FIRST_GENERATION: u32 = 1;
const LAST_GENERATION: u32 = u32::MAX - 1;

/// The operations and timers the reactor holds.
pub(super) struct Entries {
	slots: Vec<Slot>,
	/// The head of the vacant slots, which thread through their own
	/// `next_free`.
	free: Option<u32>,
}

enum Slot {
	Vacant {
		generation: u32,
		next_free: Option<u32>,
	},
	Occupied {
		generation: u32,
		entry: Entry,
	},
}

impl Entries {
	pub(super) fn new() -> Entries {
		Entries {
			slots: Vec::new(),
			free: None,
		}
	}

	/// Puts `entry` in a slot and returns the id that names it. A slot the
	/// reactor let go of earlier is used again, one generation on.
	pub(super) fn insert(&mut self, entry: Entry) -> u64 {
		let Some(index) = self.free else {
			let index = u32::try_from(self.slots.len())
				.expect("no more than 2^32 operations in flight at once");
			self.slots.push(Slot::Occupied {
				generation: FIRST_GENERATION,
				entry,
			});
			return pack(FIRST_GENERATION, index);
		};
		let (generation, next_free) = match self.slots[index as usize] {
			Slot::Vacant {
				generation,
				next_free,
			} => (generation, next_free),
			Slot::Occupied { .. } => unreachable!("a slot on the free list is vacant"),
		};
		self.free = next_free;
		// The generation moves as the slot is taken, so an id an earlier
		// operation left behind names nothing here. A slot that was never used
		// and one that is taken for the first time carry the same generation,
		// and nothing outside ever saw either.
		let generation = next_generation(generation);
		self.slots[index as usize] = Slot::Occupied { generation, entry };
		pack(generation, index)
	}

	/// The entry `id` names, mutably.
	pub(super) fn get_mut(&mut self, id: u64) -> Option<&mut Entry> {
		let (index, _) = self.locate(id)?;
		match &mut self.slots[index] {
			Slot::Occupied { entry, .. } => Some(entry),
			Slot::Vacant { .. } => None,
		}
	}

	/// Takes the entry `id` names out of the table. The slot goes back with the
	/// generation it has: it moves only when the slot is taken again, so a
	/// submission that hands its entry straight back leaves the table as it
	/// was, apart from the generation that submission spent.
	pub(super) fn remove(&mut self, id: u64) -> Option<Entry> {
		let (index, generation) = self.locate(id)?;
		let previous = std::mem::replace(
			&mut self.slots[index],
			Slot::Vacant {
				generation,
				next_free: self.free,
			},
		);
		self.free = Some(index as u32);
		match previous {
			Slot::Occupied { entry, .. } => Some(entry),
			Slot::Vacant { .. } => unreachable!("locate names an occupied slot"),
		}
	}

	/// Writes a result into the entry `id` names, which is where its holder
	/// reads it back from, and hands back whoever waits to be told.
	///
	/// `None` when the id names nothing, or names an operation that already has
	/// its result: a mechanism can report the same operation twice. It is also
	/// `None` when the holder is not waiting yet, and when the holder gave the
	/// result up, in which case the operation ends here.
	pub(super) fn land(&mut self, id: u64, response: Result<Response, Error>) -> Option<Waker> {
		let (index, _) = self.locate(id)?;
		let entry = match &mut self.slots[index] {
			Slot::Occupied { entry, .. } => entry,
			Slot::Vacant { .. } => unreachable!("locate names an occupied slot"),
		};
		if entry.is_done() {
			return None;
		}
		if entry.detached {
			// The holder gave the result up: nothing waits for it, and the
			// operation ends here rather than sitting in the table until it is
			// taken.
			self.remove(id);
			return None;
		}
		entry.response = Some(response);
		entry.space = None;
		entry.waker.take()
	}

	/// Every operation and timer the table holds.
	pub(super) fn iter(&self) -> impl Iterator<Item = (u64, &Entry)> {
		self.slots
			.iter()
			.enumerate()
			.filter_map(|(index, slot)| match slot {
				Slot::Occupied { generation, entry } => {
					Some((pack(*generation, index as u32), entry))
				}
				Slot::Vacant { .. } => None,
			})
	}

	fn locate(&self, id: u64) -> Option<(usize, u32)> {
		let (generation, index) = unpack(id);
		let index = usize::try_from(index).ok()?;
		match self.slots.get(index) {
			Some(Slot::Occupied {
				generation: found, ..
			}) if *found == generation => Some((index, generation)),
			_ => None,
		}
	}
}

fn pack(generation: u32, index: u32) -> u64 {
	(u64::from(generation) << INDEX_BITS) | u64::from(index)
}

fn unpack(id: u64) -> (u32, u32) {
	((id >> INDEX_BITS) as u32, (id & u64::from(u32::MAX)) as u32)
}

/// The generation a slot carries the next time it is taken: it skips 0 (the
/// ring's own token) and `u32::MAX` (whose high half would be `u64::MAX`).
fn next_generation(generation: u32) -> u32 {
	if generation >= LAST_GENERATION {
		FIRST_GENERATION
	} else {
		generation + 1
	}
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
			space: None,
		}
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

	/// A slot goes through one generation per operation it holds, not one per
	/// insertion and one per removal: an entry handed straight back is not
	/// double-charged, and a slot is not left naming an operation twice.
	#[test]
	fn a_slot_spends_one_generation_per_operation() {
		let mut entries = Entries::new();
		let first = entries.insert(entry());
		entries.remove(first).unwrap();
		let second = entries.insert(entry());
		assert_eq!(
			unpack(second).0,
			unpack(first).0 + 1,
			"the slot took the next generation, once"
		);
		assert_eq!(unpack(first).1, unpack(second).1, "the same slot held both");
	}

	/// No id is ever a token the ring keeps for itself.
	#[test]
	fn an_id_is_never_zero_or_the_futures_token() {
		let mut entries = Entries::new();
		let mut ids = Vec::new();
		for _ in 0..64 {
			let id = entries.insert(entry());
			assert_ne!(id, 0);
			assert_ne!(id, u64::MAX);
			ids.push(id);
			if let Some(id) = ids.pop() {
				entries.remove(id).unwrap();
			}
		}
		// The generations wrap within their own range rather than reaching 0 or
		// u32::MAX.
		assert_eq!(next_generation(FIRST_GENERATION), FIRST_GENERATION + 1);
		assert_eq!(next_generation(LAST_GENERATION), FIRST_GENERATION);
		assert_eq!(next_generation(u32::MAX), FIRST_GENERATION);
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
