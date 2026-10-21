//! Waking a parked driver from another thread.
//!
//! The executor owns the reactor, so `block_on` parks in the reactor's wait
//! once the root future is pending. A wake raised by another thread has to
//! reach that wait every time: none may be dropped in the window between the
//! root future observing the state it waits on and the driver parking.
//!
//! The signal here is a [`Scope`]: the root future takes a fresh child of the
//! executor's scope each round and hands the other thread a clone, so firing it
//! is the only thing that can bring a parked `block_on` back. Each round
//! carries a reactor timeout, so a wake that never lands fails the round
//! instead of hanging the test.

mod common;

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::mpsc::Sender;
use std::task::Poll;
use std::time::Duration;

use completion_async::{Config, Executor, Scope, scope, timeout};

/// Rounds the other thread fires and the root future has to observe.
const ROUNDS: usize = 2_000;

/// Per-round watchdog. A round is a channel round trip between two threads, so
/// this only expires when a wake was lost — and then it bounds the failure.
const WATCHDOG: Duration = Duration::from_secs(5);

/// The round's wait: a timer filed with the round's scope, which a cancel resolves
/// early. It is never meant to run out.
const LONG_WAIT: Duration = Duration::from_secs(3600);

/// The root future: each round derives a fresh scope and hands the other thread
/// a clone, so the fire from that thread is the only wake that can bring a
/// parked `block_on` back.
///
/// The `armed` handshake makes the other thread wait until this round's waker
/// is registered, so every fire lands on a wait about to park — the window a
/// lost wake hides in.
///
/// Returns the round a watchdog fired in, or `None` once every round landed.
async fn rounds<F: Future<Output = completion_async::Result<()>>>(
	mut watchdogs: std::vec::IntoIter<F>,
	signals: Sender<Scope>,
	armed: Sender<()>,
	consumed: Sender<()>,
) -> Option<usize> {
	for round in 0..ROUNDS {
		let signal = scope();
		signals
			.send(signal.clone())
			.expect("the other thread takes the signal");
		// The task files its timer and only then tells the other thread to fire,
		// so the fire lands on a timer already filed with the scope, whose waker is
		// the parked driver's.
		let armed = armed.clone();
		let mut wait = pin!(async move {
			let mut armed = Some(armed);
			signal
				.spawn(async move {
					let mut timer = pin!(timeout(LONG_WAIT));
					poll_fn(move |cx| {
						let ready = timer.as_mut().poll(cx);
						if let Some(armed) = armed.take() {
							armed.send(()).expect("the other thread waits to fire");
						}
						ready
					})
					.await
				})
				.await
		});
		let mut timeout = pin!(watchdogs.next().expect("one watchdog per round"));
		let landed = poll_fn(|cx| {
			if wait.as_mut().poll(cx).is_ready() {
				return Poll::Ready(true);
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
		.map(|_| timeout(WATCHDOG))
		.collect::<Vec<_>>()
		.into_iter();

	let (signals_tx, signals) = std::sync::mpsc::channel::<Scope>();
	let (armed_tx, armed_rx) = std::sync::mpsc::channel::<()>();
	let (consumed_tx, consumed_rx) = std::sync::mpsc::channel::<()>();

	let raiser = std::thread::spawn(move || {
		for _ in 0..ROUNDS {
			let Ok(signal) = signals.recv() else {
				return;
			};
			let Ok(()) = armed_rx.recv() else {
				return;
			};
			drop(signal.cancel());
			if consumed_rx.recv().is_err() {
				return;
			}
		}
	});

	let outcome = executor
		.block_on(rounds(watchdogs, signals_tx, armed_tx, consumed_tx))
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
