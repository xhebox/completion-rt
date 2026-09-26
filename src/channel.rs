//! A message channel on a reactor's wake-up.
//!
//! One queue, two ends: a [`Sender`] any thread can push into, and a
//! [`Receiver`] that is a `Future` resolving to one value at a time.
//! [`Submitter::channel`](crate::Submitter::channel) binds the pair to a
//! reactor's wake-up, and every send that finds a waiting receiver raises
//! it, so the reactor's `poll` returns and the executor above it polls that
//! receiver.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::Event;

/// The channel is closed; the values that could not be sent are handed back.
#[derive(Debug)]
pub struct Closed<T>(pub T);

impl<T> std::fmt::Display for Closed<T> {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str("the channel is closed")
	}
}

impl<T: std::fmt::Debug> std::error::Error for Closed<T> {}

struct State<T> {
	queue: VecDeque<T>,
	/// One entry per waiting receiver. The id belongs to the receiver, so it
	/// replaces or drops its own entry alone.
	waiters: Vec<(u64, Waker)>,
	closed: bool,
}

impl<T> State<T> {
	fn new() -> State<T> {
		Self {
			queue: VecDeque::new(),
			waiters: Vec::new(),
			closed: false,
		}
	}
}

/// The channel: the queue, its waiters, and the wake-up they are raised on.
pub struct Shared<T> {
	state: Mutex<State<T>>,
	event: Event,
	/// Live senders; the channel closes when the last one goes.
	senders: AtomicUsize,
	next_id: AtomicU64,
}

impl<T> Shared<T> {
	/// Binds a fresh pair to `event`.
	///
	/// Reached through [`Submitter::channel`](crate::Submitter::channel),
	/// which supplies the reactor's own wake-up.
	pub(super) fn split(event: Event) -> (Sender<T>, Receiver<T>) {
		let sender = Sender(Arc::new(Self::new(event)));
		let receiver = Receiver::attach(&sender.0);
		(sender, receiver)
	}

	fn new(event: Event) -> Shared<T> {
		Self {
			state: Mutex::new(State::new()),
			event,
			senders: AtomicUsize::new(1),
			next_id: AtomicU64::new(1),
		}
	}

	/// Queues `value`, then wakes one waiting receiver.
	///
	/// The check and the queueing share one lock, so a closed channel takes
	/// nothing and hands the value back.
	fn enqueue(&self, value: T) -> Result<(), T> {
		let mut state = self.state.lock().unwrap();
		if state.closed {
			return Err(value);
		}
		state.queue.push_back(value);
		// The oldest waiter takes the value, and leaves the list: the receiver
		// registers itself again on its next poll.
		let woken = if state.waiters.is_empty() {
			None
		} else {
			Some(state.waiters.remove(0).1)
		};
		drop(state);
		if let Some(waker) = woken {
			waker.wake();
		}
		// The reactor has to look at the queue whether or not a receiver was
		// waiting when the value landed: one that is running right now has no
		// waiter registered, and the thread it runs on parks on the wake-up
		// alone — nothing else would ever tick it again.
		let _ = self.event.notify();
		Ok(())
	}

	/// Closes the channel: waiting receivers wake to their `None`.
	fn close(&self) {
		let waiters = {
			let mut state = self.state.lock().unwrap();
			if state.closed {
				return;
			}
			state.closed = true;
			std::mem::take(&mut state.waiters)
		};
		for (_, waker) in &waiters {
			waker.wake_by_ref();
		}
		let _ = self.event.notify();
	}
}

/// The send end of a [`Shared`] channel.
pub struct Sender<T>(Arc<Shared<T>>);

impl<T> Sender<T> {
	/// Queues one value; a closed channel hands it back.
	pub fn send(&self, value: T) -> Result<(), Closed<T>> {
		self.0.enqueue(value).map_err(Closed)
	}

	/// Closes the channel.
	pub fn close(&self) {
		self.0.close();
	}
}

impl<T> Clone for Sender<T> {
	fn clone(&self) -> Sender<T> {
		self.0.senders.fetch_add(1, Ordering::Relaxed);
		Sender(Arc::clone(&self.0))
	}
}

impl<T> Drop for Sender<T> {
	fn drop(&mut self) {
		if self.0.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
			self.0.close();
		}
	}
}

/// The receive end of a [`Shared`] channel.
///
/// `Clone` gives another receiver on the same queue. Each one is a `Future`
/// resolving to one queued value, then `None` once the channel is closed and
/// the queue drained.
pub struct Receiver<T> {
	id: u64,
	shared: Arc<Shared<T>>,
}

impl<T> Receiver<T> {
	/// A receiver on `shared`, with an id of its own.
	fn attach(shared: &Arc<Shared<T>>) -> Receiver<T> {
		Self {
			id: shared.next_id.fetch_add(1, Ordering::Relaxed),
			shared: Arc::clone(shared),
		}
	}

	/// Takes the next queued value, without waiting. `None` when the queue
	/// is empty, closed or not.
	pub fn try_recv(&self) -> Option<T> {
		self.shared.state.lock().unwrap().queue.pop_front()
	}

	/// Closes the channel.
	pub fn close(&self) {
		self.shared.close();
	}

	fn register(&self, waiters: &mut Vec<(u64, Waker)>, waker: &Waker) {
		match waiters.iter_mut().find(|(id, _)| *id == self.id) {
			Some(entry) => entry.1 = waker.clone(),
			None => waiters.push((self.id, waker.clone())),
		}
	}
}

impl<T> Clone for Receiver<T> {
	fn clone(&self) -> Receiver<T> {
		Self::attach(&self.shared)
	}
}

impl<T> Drop for Receiver<T> {
	fn drop(&mut self) {
		let mut state = self.shared.state.lock().unwrap();
		state.waiters.retain(|(id, _)| *id != self.id);
	}
}

impl<T> Future for Receiver<T> {
	type Output = Option<T>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
		let this = self.get_mut();
		let mut state = this.shared.state.lock().unwrap();
		loop {
			if let Some(value) = state.queue.pop_front() {
				return Poll::Ready(Some(value));
			}
			if state.closed {
				return Poll::Ready(None);
			}
			// Registering before the second look closes the window a send
			// would otherwise fall into: it saw no waiter, so it did not
			// raise the wake-up.
			this.register(&mut state.waiters, cx.waker());
			if state.queue.is_empty() && !state.closed {
				return Poll::Pending;
			}
			state.waiters.retain(|(id, _)| *id != this.id);
		}
	}
}
