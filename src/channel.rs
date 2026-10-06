//! A channel bound to a reactor's wake-up.

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

pub(super) struct Shared<T> {
	state: Mutex<State<T>>,
	event: Event,
	senders: AtomicUsize,
	next_id: AtomicU64,
}

impl<T> Shared<T> {
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

	fn enqueue(&self, value: T) -> Result<(), T> {
		let mut state = self.state.lock().unwrap();
		if state.closed {
			return Err(value);
		}
		state.queue.push_back(value);
		let woken = if state.waiters.is_empty() {
			None
		} else {
			Some(state.waiters.remove(0).1)
		};
		drop(state);
		if let Some(waker) = woken {
			waker.wake();
		}
		// Notify even without a registered receiver: a running receiver may be about to park.
		let _ = self.event.notify();
		Ok(())
	}

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

/// The sending end; dropping the last sender closes the channel.
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

/// A clonable receiver. Each poll sequence yields one queued value, or `None` once closed and drained.
pub struct Receiver<T> {
	id: u64,
	shared: Arc<Shared<T>>,
}

impl<T> Receiver<T> {
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
			this.register(&mut state.waiters, cx.waker());
			if state.queue.is_empty() && !state.closed {
				return Poll::Pending;
			}
			state.waiters.retain(|(id, _)| *id != this.id);
		}
	}
}
