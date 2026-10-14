//! The timer wheel: armed deadlines and the wakers waiting on them.
//!
//! The wheel lives in the reactor's shared state and is reached through the
//! [`Reactor`](crate::Reactor) timer methods. Timer entries are not operations: they
//! never reach the backend and never enter the reactor's operation table.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::core::stopped;
use crate::park::Park;

/// Names one armed timer: its deadline, then a serial that keeps timers with
/// the same deadline apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimerKey {
	deadline: Instant,
	serial: u64,
}

pub(crate) struct TimerWheel {
	inner: RefCell<Inner>,
	serial: Cell<u64>,
	park: Arc<Park>,
}

#[derive(Default)]
struct Inner {
	/// Armed timers waiting for their deadline, in deadline order. The waker is set by
	/// the first pending poll.
	armed: BTreeMap<TimerKey, Option<Waker>>,
	/// Armed timers that ended before their future polled again. The poll takes the
	/// entry; a cancel removes it.
	settled: HashMap<TimerKey, Settled>,
}

#[derive(Clone, Copy)]
enum Settled {
	Fired,
	Stopped,
}

impl TimerWheel {
	pub(crate) fn new(park: Arc<Park>) -> TimerWheel {
		TimerWheel {
			inner: RefCell::new(Inner::default()),
			serial: Cell::new(0),
			park,
		}
	}

	/// Arms a timer that fires at `deadline`.
	pub(crate) fn insert(&self, deadline: Instant) -> TimerKey {
		let serial = self.serial.get();
		self.serial.set(serial + 1);
		let key = TimerKey { deadline, serial };
		self.inner.borrow_mut().armed.insert(key, None);
		// The reactor may be asleep until a later deadline: wake it to recompute.
		self.park.unpark();
		key
	}

	/// Polls the timer `key`: `Ready` once it has fired, otherwise `Pending`
	/// with `cx`'s waker recorded for the reactor to wake.
	pub(crate) fn poll(&self, key: TimerKey, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		let mut inner = self.inner.borrow_mut();
		if let Some(settled) = inner.settled.remove(&key) {
			return Poll::Ready(match settled {
				Settled::Fired => Ok(()),
				Settled::Stopped => Err(stopped()),
			});
		}
		match inner.armed.get_mut(&key) {
			Some(waker) => {
				if !waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
					*waker = Some(cx.waker().clone());
				}
				Poll::Pending
			}
			None => Poll::Ready(Err(io::Error::new(
				io::ErrorKind::NotFound,
				"the timer was cancelled",
			))),
		}
	}

	/// Disarms the timer `key`, whether or not it has fired.
	pub(crate) fn cancel(&self, key: TimerKey) {
		let mut inner = self.inner.borrow_mut();
		inner.armed.remove(&key);
		inner.settled.remove(&key);
	}

	/// Caps `timeout` at the nearest armed deadline.
	pub(crate) fn within(&self, timeout: Option<Duration>) -> Option<Duration> {
		let nearest = self
			.inner
			.borrow()
			.armed
			.first_key_value()
			.map(|(key, _)| key.deadline.saturating_duration_since(Instant::now()));
		match (timeout, nearest) {
			(Some(timeout), Some(nearest)) => Some(timeout.min(nearest)),
			(timeout, nearest) => timeout.or(nearest),
		}
	}

	/// Fires every timer due at `now` and wakes its waker.
	pub(crate) fn expire(&self, now: Instant) {
		let mut wakers = Vec::new();
		{
			let mut inner = self.inner.borrow_mut();
			while let Some((&key, _)) = inner.armed.first_key_value() {
				if key.deadline > now {
					break;
				}
				let waker = inner.armed.remove(&key).flatten();
				inner.settled.insert(key, Settled::Fired);
				wakers.extend(waker);
			}
		}
		for waker in wakers {
			waker.wake();
		}
	}

	/// Ends every armed timer; their futures fail with the stopped error. Called
	/// once the reactor's `stopped` flag is set, so nothing re-arms after it.
	pub(crate) fn settle_stopped(&self) {
		let mut wakers = Vec::new();
		{
			let mut inner = self.inner.borrow_mut();
			while let Some((key, waker)) = inner.armed.pop_first() {
				inner.settled.insert(key, Settled::Stopped);
				wakers.extend(waker);
			}
		}
		for waker in wakers {
			waker.wake();
		}
	}
}
