//! The timer wheel: armed deadlines and the wakers waiting on them.
//!
//! A [`Timers`] handle is shared between the futures that arm a timer and the
//! reactor that fires it. Timers are not operations: they never reach the
//! backend and never enter the reactor's operation table.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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

/// Shares a reactor's timer wheel. Cloning it is cheap; every clone arms timers
/// on the same reactor.
#[derive(Clone)]
pub struct Timers {
	wheel: Arc<Wheel>,
}

struct Wheel {
	inner: Mutex<Inner>,
	serial: AtomicU64,
	park: Arc<Park>,
	/// Shared with the reactor: once set, no timer is armed.
	stopped: Arc<AtomicBool>,
}

#[derive(Default)]
struct Inner {
	/// Timers waiting for their deadline, in deadline order. The waker is set by
	/// the first pending poll.
	armed: BTreeMap<TimerKey, Option<Waker>>,
	/// Timers that ended before their future polled again. The poll takes the
	/// entry; a cancel removes it.
	settled: HashMap<TimerKey, Settled>,
}

#[derive(Clone, Copy)]
enum Settled {
	Fired,
	Stopped,
}

impl Timers {
	pub(crate) fn new(park: Arc<Park>, stopped: Arc<AtomicBool>) -> Timers {
		Timers {
			wheel: Arc::new(Wheel {
				inner: Mutex::new(Inner::default()),
				serial: AtomicU64::new(0),
				park,
				stopped,
			}),
		}
	}

	/// Arms a timer that fires at `deadline`. Refused once the reactor has
	/// stopped.
	pub fn insert(&self, deadline: Instant) -> io::Result<TimerKey> {
		let key = {
			let mut inner = self.wheel.inner.lock().unwrap();
			if self.wheel.stopped.load(Ordering::Acquire) {
				return Err(stopped());
			}
			let key = TimerKey {
				deadline,
				serial: self.wheel.serial.fetch_add(1, Ordering::Relaxed),
			};
			inner.armed.insert(key, None);
			key
		};
		// The reactor may be asleep until a later deadline: wake it to recompute.
		self.wheel.park.unpark();
		Ok(key)
	}

	/// Polls the timer `key`: `Ready` once it has fired, otherwise `Pending`
	/// with `cx`'s waker recorded for the reactor to wake.
	pub fn poll(&self, key: TimerKey, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		let mut inner = self.wheel.inner.lock().unwrap();
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
	pub fn cancel(&self, key: TimerKey) {
		let mut inner = self.wheel.inner.lock().unwrap();
		inner.armed.remove(&key);
		inner.settled.remove(&key);
	}

	/// Caps `timeout` at the nearest armed deadline.
	pub(crate) fn within(&self, timeout: Option<Duration>) -> Option<Duration> {
		let nearest = self
			.wheel
			.inner
			.lock()
			.unwrap()
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
			let mut inner = self.wheel.inner.lock().unwrap();
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
			let mut inner = self.wheel.inner.lock().unwrap();
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
