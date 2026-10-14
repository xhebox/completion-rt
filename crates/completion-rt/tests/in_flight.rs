//! The in-flight snapshot: what the reactor still holds, read back without
//! disturbing it.
//!
//! This drives the reactor by hand, so it lives with the low layer. Unix only:
//! the snapshot's descriptor is a raw fd, and the two operations the test keeps
//! un-landed are a blocked pipe read and a far-off timer.
#![cfg(unix)]

mod common;

use common::block_on;

use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use completion_rt::{
	Config, Extent, Extents, Handle, InFlight, InFlightState, Memory, Op, Reactor,
};

/// The buffer in the shape the engine names memory.
fn memory(buf: &Arc<Vec<u8>>) -> Arc<dyn Memory> {
	let memory: Arc<dyn Memory> = buf.clone();
	memory
}

/// Drives the reactor until an entry matches, so the test does not race the
/// reactor's first pass over the submission.
fn until_entry(reactor: &Reactor, wanted: impl Fn(&InFlight) -> bool) -> InFlight {
	for _ in 0..2_000 {
		if let Some(entry) = reactor.in_flight().into_iter().find(|entry| wanted(entry)) {
			return entry;
		}
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	panic!("no matching in-flight entry appeared");
}

/// A read that cannot complete shows up with its descriptor, state and a
/// growing age; cancelling the read and letting it settle takes it out of the
/// snapshot.
#[test]
fn in_flight_shows_a_stalled_read() {
	// The write end stays open and is never written, so the read has nothing to
	// return and waits.
	let (reader, _writer) = std::io::pipe().unwrap();
	let read_fd = reader.as_raw_fd();
	let reader = Handle::new(reader);

	let reactor = Reactor::new(Config::default()).unwrap();
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 8]);
	let read = common::submit(
		&reactor,
		&reader,
		Op::Read {
			offset: 0,
			memory: memory(&buf),
			extents: Extents::from_slice(&[Extent { offset: 0, len: 8 }]),
		},
	)
	.unwrap();

	// The read leaves Pending once the reactor has handed it to the backend,
	// where it waits on the pipe.
	let snapshot = until_entry(&reactor, |entry| {
		entry.op == "read" && entry.state == InFlightState::InFlight
	});
	assert_eq!(
		snapshot.descriptor,
		Some(read_fd),
		"the read did not report its own descriptor"
	);
	assert!(snapshot.names_memory, "a read names the caller's memory");
	assert!(!snapshot.cancel_requested, "nothing asked the read to stop");

	// The age is taken at the snapshot, so a later look is older.
	let first_age = snapshot.age;
	reactor.poll(Some(Duration::from_millis(50))).unwrap();
	let later = until_entry(&reactor, |entry| {
		entry.op == "read" && entry.state == InFlightState::InFlight
	});
	assert!(
		later.age > first_age,
		"the age did not grow: {first_age:?} then {:?}",
		later.age
	);

	// Cancelled through the cooperative path, so the read settles without the
	// drop fallback, and the snapshot no longer holds it.
	read.cancel();
	let (_, result) = block_on(&reactor, read);
	let error = result.expect_err("the read was not cancelled");
	assert!(
		reactor.is_cancelled(&error),
		"the read was not cancelled: {error:?}"
	);
	assert!(
		reactor.in_flight().iter().all(|entry| entry.op != "read"),
		"the cancelled read is still in the snapshot"
	);
}
