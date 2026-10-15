//! Waking a parked driver from another thread.
//!
//! The executor owns the reactor, so `block_on` parks in the reactor's wait
//! once the root future is pending. A wake raised by another thread has to
//! reach that wait every time: none may be dropped in the window between the
//! root future observing the state it waits on and the driver parking.
//!
//! The signal here is a [`Cancel`]: the root future hands the other thread a
//! fresh one each round and awaits it, so firing it is the only thing that can
//! bring a parked `block_on` back. Each round carries a reactor timeout, so a
//! wake that never lands fails the round instead of hanging the test.

mod common;

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::mpsc::Sender;
use std::task::Poll;
use std::time::Duration;

use completion_async::{Cancel, Config, Executor, Timeout};

/// Rounds the other thread fires and the root future has to observe.
const ROUNDS: usize = 2_000;

/// Per-round watchdog. A round is a channel round trip between two threads, so
/// this only expires when a wake was lost — and then it bounds the failure.
const WATCHDOG: Duration = Duration::from_secs(5);

/// The root future: each round hands the other thread a fresh cancel and waits
/// for it, so the fire from that thread is the only wake that can bring a
/// parked `block_on` back.
///
/// The `armed` handshake makes the other thread wait until this round's waker
/// is registered, so every fire lands on a wait about to park — the window a
/// lost wake hides in.
///
/// Returns the round a watchdog fired in, or `None` once every round landed.
async fn rounds(
	mut watchdogs: std::vec::IntoIter<Timeout>,
	cancels: Sender<Cancel>,
	armed: Sender<()>,
	consumed: Sender<()>,
) -> Option<usize> {
	for round in 0..ROUNDS {
		let cancel = Cancel::new();
		cancels
			.send(cancel.clone())
			.expect("the other thread takes the cancel");
		let mut wait = pin!(cancel.wait());
		let mut timeout = pin!(watchdogs.next().expect("one watchdog per round"));
		let mut armed_sent = false;
		let landed = poll_fn(|cx| {
			if wait.as_mut().poll(cx).is_ready() {
				return Poll::Ready(true);
			}
			if !armed_sent {
				armed_sent = true;
				armed.send(()).expect("the other thread waits to fire");
			}
			if timeout.as_mut().poll(cx).is_ready() {
				return Poll::Ready(false);
			}
			Poll::Pending
		})
		.await;
		if !landed {
			return Some(round);
		}
		consumed
			.send(())
			.expect("the other thread waits for the round to land");
	}
	None
}

/// A parked `block_on` is woken from another thread on every round: the other
/// thread fires the cancel the root future is awaiting, and no fire may be
/// lost to the gap between the root future going pending and the reactor
/// sleeping.
#[test]
fn a_parked_block_on_is_woken_from_another_thread_every_round() {
	common::init_logging();
	let mut executor = Executor::new(Config::default()).unwrap();
	let watchdogs = (0..ROUNDS)
		.map(|_| Timeout::with_executor(WATCHDOG, &executor).unwrap())
		.collect::<Vec<_>>()
		.into_iter();

	let (cancels_tx, cancels) = std::sync::mpsc::channel::<Cancel>();
	let (armed_tx, armed_rx) = std::sync::mpsc::channel::<()>();
	let (consumed_tx, consumed_rx) = std::sync::mpsc::channel::<()>();

	let raiser = std::thread::spawn(move || {
		for _ in 0..ROUNDS {
			let Ok(cancel) = cancels.recv() else {
				return;
			};
			let Ok(()) = armed_rx.recv() else {
				return;
			};
			cancel.cancel();
			if consumed_rx.recv().is_err() {
				return;
			}
		}
	});

	let outcome = executor
		.block_on(rounds(watchdogs, cancels_tx, armed_tx, consumed_tx))
		.unwrap();
	// `block_on` returned, so the root future dropped its senders; the other
	// thread unblocks and exits whether the rounds landed or a watchdog fired.
	raiser.join().unwrap();

	if let Some(round) = outcome {
		panic!(
			"round {round} of {ROUNDS}: a cancel fired from another thread did not reach the parked block_on, which slept to the {WATCHDOG:?} watchdog"
		);
	}
}
