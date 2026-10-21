//! A [`BufferPool`] hands out pre-faulted blocks, takes them back when the last
//! handle drops, and never fails a burst above its capacity.

mod common;

use completion_backend::BufferPool;

#[test]
fn a_pool_hands_out_and_takes_back_its_blocks() {
	let pool = BufferPool::new(4096, 2);
	assert_eq!(pool.block_size(), 4096);
	assert_eq!(pool.capacity(), 2);
	assert_eq!(pool.available(), 2);

	let first = pool.get();
	let second = pool.get();
	assert_eq!(first.len(), 4096);
	assert_eq!(second.len(), 4096);
	assert_eq!(pool.available(), 0);
	assert!(
		pool.try_get().is_none(),
		"a block beyond the capacity must not be pooled"
	);

	drop(first);
	assert_eq!(pool.available(), 1);
	let third = pool.get();
	assert_eq!(pool.available(), 0);
	drop(third);
	drop(second);
	assert_eq!(pool.available(), 2, "every block returned");
}

#[test]
fn a_pooled_block_is_zeroed() {
	let pool = BufferPool::new(64, 1);
	let buffer = pool.get();
	assert!(buffer.whole().to_vec().iter().all(|&byte| byte == 0));
}

#[test]
fn a_span_keeps_its_block_busy_until_the_span_drops() {
	let pool = BufferPool::new(4096, 2);
	let buffer = pool.get();
	let span = buffer.whole();
	// The caller lets go of the buffer, but the span the operation names still
	// reaches the block: recycling it now would hand the same memory to another
	// operation while the kernel still writes through this span.
	drop(buffer);
	assert_eq!(pool.available(), 1, "the span keeps the block checked out");
	drop(span);
	assert_eq!(pool.available(), 2);
}

#[test]
fn a_burst_above_capacity_falls_back_to_a_standalone_buffer() {
	let pool = BufferPool::new(128, 1);
	let pooled = pool.get();
	let standalone = pool.get();
	assert_eq!(pool.available(), 0);
	assert_eq!(standalone.len(), 128, "the fallback has the block size");

	// The standalone buffer is not part of the pool, so only the pooled block
	// comes back.
	drop(standalone);
	assert_eq!(pool.available(), 0);
	drop(pooled);
	assert_eq!(pool.available(), 1);
}

#[test]
#[should_panic(expected = "at least one byte")]
fn a_pool_refuses_a_zero_block_size() {
	BufferPool::new(0, 1);
}

#[test]
#[should_panic(expected = "at least one block")]
fn a_pool_refuses_a_zero_capacity() {
	BufferPool::new(4096, 0);
}
