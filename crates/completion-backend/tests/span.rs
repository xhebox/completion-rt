//! A span names its extents and its space, so a caller can read them back after
//! deriving the span from a bound space.

mod common;
use common::Buffer;

use std::sync::Arc;

use completion_backend::{Extent, Memory, Space};

fn extents(ranges: &[(u64, usize)]) -> Vec<Extent> {
	ranges
		.iter()
		.map(|&(offset, len)| Extent { offset, len })
		.collect()
}

#[test]
fn a_span_names_the_extents_it_was_taken_with() {
	let memory: Arc<dyn Memory> = Arc::new(Buffer::new(8));
	let space = Space::new(memory);

	let span = space.gather([Extent { offset: 0, len: 2 }, Extent { offset: 4, len: 3 }]);

	assert_eq!(span.space().len(), 8);
	assert_eq!(span.extents(), &extents(&[(0, 2), (4, 3)]));
	assert_eq!(span.len(), 5);
}

#[test]
fn skip_and_take_cut_the_extents_in_order() {
	let memory: Arc<dyn Memory> = Arc::new(Buffer::new(8));
	let space = Space::new(memory);

	let span = space.gather([Extent { offset: 0, len: 2 }, Extent { offset: 4, len: 3 }]);

	assert_eq!(span.clone().skip(3).extents(), &extents(&[(5, 2)]));
	assert_eq!(span.take(4).extents(), &extents(&[(0, 2), (4, 2)]));
}

#[test]
fn whole_covers_the_memory_as_one_extent() {
	let memory: Arc<dyn Memory> = Arc::new(Buffer::new(8));
	let space = Space::new(memory);

	assert_eq!(space.whole().extents(), &extents(&[(0, 8)]));
}
