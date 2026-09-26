//! A message channel bound to a reactor's wake-up: a send raises a waiting
//! receiver, a closed channel hands the value back, and the queue survives the
//! close.

mod common;

use common::block_on;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use completion_rt::{Closed, Config, Reactor, Receiver, Sender};

/// A fresh pair, and the reactor whose wake-up the channel is bound to.
fn channel() -> (Reactor, Sender<u8>, Receiver<u8>) {
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let (sender, receiver) = submitter.channel();
	(reactor, sender, receiver)
}

/// Counts the wake-ups it is raised with.
struct Count(AtomicUsize);

impl Wake for Count {
	fn wake(self: Arc<Self>) {
		self.0.fetch_add(1, Ordering::AcqRel);
	}
}

/// Polls a receiver with a waker that counts, so a test can see a send raise
/// it without a driver in between.
fn poll_once(receiver: &mut Receiver<u8>, count: &Arc<Count>) -> Poll<Option<u8>> {
	let waker = Waker::from(Arc::clone(count));
	let mut context = Context::from_waker(&waker);
	Pin::new(receiver).poll(&mut context)
}

#[test]
fn a_send_wakes_one_waiting_receiver_once() {
	let (_reactor, sender, mut receiver) = channel();
	let count = Arc::new(Count(AtomicUsize::new(0)));
	assert!(poll_once(&mut receiver, &count).is_pending());
	sender.send(1u8).unwrap();
	assert_eq!(count.0.load(Ordering::Acquire), 1);
	assert_eq!(block_on(&mut receiver), Some(1));
}

#[test]
fn a_closed_channel_hands_the_value_back() {
	let (_reactor, sender, receiver) = channel();
	sender.close();
	match sender.send(7) {
		Err(Closed(value)) => assert_eq!(value, 7),
		Ok(()) => panic!("a closed channel must not take the value"),
	}
	assert_eq!(block_on(receiver), None);
}

#[test]
fn queued_values_are_taken_in_order_after_close() {
	let (_reactor, sender, receiver) = channel();
	sender.send(1u8).unwrap();
	sender.send(2).unwrap();
	sender.close();
	assert_eq!(receiver.try_recv(), Some(1));
	assert_eq!(block_on(receiver), Some(2));
}

#[test]
fn the_last_sender_closes_the_channel() {
	let (_reactor, sender, mut receiver) = channel();
	let other = sender.clone();
	drop(sender);
	assert!(other.send(7).is_ok(), "a live sender keeps it open");
	drop(other);
	assert_eq!(block_on(&mut receiver), Some(7));
	assert_eq!(block_on(&mut receiver), None);
}
