//! The in-flight snapshot: what the reactor still holds, read back without
//! disturbing it.
//!
//! Unix only: the snapshot's descriptor is a raw fd, and the two operations the
//! test keeps un-landed are a blocked pipe read and a far-off timer.
#![cfg(unix)]

mod common;

use common::block_on;

use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use completion_rt::{
	Cancel, Cancelled, Config, Extent, Handle, InFlight, InFlightState, Memory, Reactor, Submitter,
};

/// The buffer in the shape the engine names memory.
fn memory(buf: &Arc<Vec<u8>>) -> Arc<dyn Memory> {
	let memory: Arc<dyn Memory> = buf.clone();
	memory
}

/// Polls the snapshot until an entry matches, so the test does not race the
/// reactor's first pass over the submission.
fn until_entry(submitter: &Submitter, wanted: impl Fn(&InFlight) -> bool) -> InFlight {
	for _ in 0..2_000 {
		if let Some(entry) = submitter
			.in_flight()
			.into_iter()
			.find(|entry| wanted(entry))
		{
			return entry;
		}
		thread::sleep(Duration::from_millis(1));
	}
	panic!("no matching in-flight entry appeared");
}

/// A read that cannot complete and a timer in the far future both show up, with
/// their descriptor, state, deadline and a growing age; cancelling the read and
/// letting it settle takes it out of the snapshot.
#[test]
fn in_flight_shows_a_stalled_read_and_a_timer() {
	// The write end stays open and is never written, so the read has nothing to
	// return and waits.
	let (reader, _writer) = std::io::pipe().unwrap();
	let read_fd = reader.as_raw_fd();
	let reader = Handle::new(reader);

	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let buf: Arc<Vec<u8>> = Arc::new(vec![0; 8]);
	let read = submitter
		.read(&reader, 0, memory(&buf), [Extent { offset: 0, len: 8 }])
		.unwrap();
	// Bound, not dropped: a dropped completion takes its entry out of the
	// reactor, and the timer has to be there for the snapshot below.
	let _timer = submitter.timeout(Duration::from_secs(3_600)).unwrap();

	let stop = Arc::new(AtomicBool::new(false));
	let driven = {
		let stop = Arc::clone(&stop);
		thread::spawn(move || {
			while !stop.load(Ordering::SeqCst) {
				reactor.poll(Some(Duration::from_millis(1))).unwrap();
			}
		})
	};

	// The read leaves Pending once the reactor has handed it to the backend,
	// where it waits on the pipe.
	let snapshot = until_entry(&submitter, |entry| {
		entry.op == "read" && entry.state == InFlightState::InFlight
	});
	assert_eq!(
		snapshot.descriptor,
		Some(read_fd),
		"the read did not report its own descriptor"
	);
	assert!(snapshot.names_memory, "a read names the caller's memory");
	assert!(!snapshot.cancel_requested, "nothing asked the read to stop");
	assert!(snapshot.deadline.is_none(), "a request carries no deadline");

	let timer_snapshot = submitter
		.in_flight()
		.into_iter()
		.find(|entry| entry.op == "timeout")
		.expect("the timer is in the snapshot");
	assert_eq!(
		timer_snapshot.descriptor, None,
		"a timer names no descriptor"
	);
	assert!(
		timer_snapshot.deadline.is_some(),
		"a timer carries its deadline"
	);
	assert_eq!(
		timer_snapshot.state,
		InFlightState::Pending,
		"the far-off timer has not been reached"
	);

	// The age is taken at the snapshot, so a later look is older.
	let first_age = snapshot.age;
	thread::sleep(Duration::from_millis(50));
	let later = until_entry(&submitter, |entry| {
		entry.op == "read" && entry.state == InFlightState::InFlight
	});
	assert!(
		later.age > first_age,
		"the age did not grow: {first_age:?} then {:?}",
		later.age
	);

	// Cancelled through the cooperative path, so the read settles without the
	// drop fallback, and the snapshot no longer holds it.
	let cancel = Cancel::new();
	cancel.cancel();
	let result = block_on(read.until(&cancel));
	assert!(
		matches!(result, Err(Cancelled(()))),
		"the read was not cancelled: {result:?}"
	);
	assert!(
		submitter.in_flight().iter().all(|entry| entry.op != "read"),
		"the cancelled read is still in the snapshot"
	);

	stop.store(true, Ordering::SeqCst);
	driven.join().unwrap();
}
