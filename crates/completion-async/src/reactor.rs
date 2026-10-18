//! I/O reactor managing in-flight backend operations and timers.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;
use std::time::{Duration, Instant};

use completion_backend::{AsDescriptor, Backend, Config, Error, ErrorKind, Response, Result};

mod entries;
#[cfg(test)]
mod tests;

pub(crate) use completion_backend::Op;

use entries::{Entries, Entry};

/// Key identifying an in-flight operation or timer in [`Reactor`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Key(u64);

/// Internal state of a [`Reactor`], owning the operation table, timer queue,
/// and OS [`Backend`].
///
/// Wrapped in a single `RefCell` so a `poll` pass borrows the table, timers, and
/// backend only while draining completions, before waking tasks.
struct Driver {
	/// Set by teardown: refusing new submissions and timers, and making a second
	/// teardown a no-op.
	stopped: bool,
	/// Armed deadlines, nearest first. Reached only through this borrow, so the
	/// wakers a fire raises run under the same invariant as the operations'.
	timers: BTreeSet<(Instant, u64)>,
	entries: Entries,
	backend: Backend,
}

impl Driver {
	fn new(config: Config) -> Result<Driver> {
		Ok(Driver {
			stopped: false,
			timers: BTreeSet::new(),
			entries: Entries::new(),
			backend: Backend::new(&config)?,
		})
	}

	/// Collects the backend's completions and wakes whoever waits. The whole
	/// pass runs on `&mut self`, so a waker that re-enters the reactor panics
	/// instead of touching the driver mid-flight.
	fn poll(&mut self, timeout: Option<Duration>) -> Result<()> {
		let entries = &mut self.entries;
		self.backend.poll(timeout, |id, response| {
			if let Some(waker) = entries.land(id, response) {
				waker.wake();
			}
		})
	}

	/// Whether every operation the driver holds has landed. An armed timer counts
	/// as landed: it is the reactor's own, so it holds no backend work up.
	fn is_idle(&self) -> bool {
		self.entries
			.iter()
			.all(|(_, entry)| entry.is_done() || entry.timer.is_some())
	}

	fn wait(&mut self, timeout: Option<Duration>) -> Result<()> {
		let nearest = self
			.timers
			.first()
			.map(|&(deadline, _)| deadline.saturating_duration_since(Instant::now()));
		let within = match (timeout, nearest) {
			(Some(timeout), Some(nearest)) => Some(timeout.min(nearest)),
			(timeout, nearest) => timeout.or(nearest),
		};
		self.poll(within)?;
		let now = Instant::now();
		while let Some(&(deadline, id)) = self.timers.first() {
			if deadline > now {
				break;
			}
			self.timers.pop_first();
			if let Some(waker) = self.entries.land(id, Ok(Response::Done)) {
				waker.wake();
			}
		}
		Ok(())
	}

	/// Asks the backend to cancel `id` if it has not been cancelled or settled yet.
	/// A timer is disarmed here instead, and its poll reports the cancel.
	fn cancel(&mut self, id: u64) {
		let Some(entry) = self.entries.get_mut(id) else {
			return;
		};
		if std::mem::replace(&mut entry.cancelled, true) || entry.is_done() {
			return;
		}
		if let Some(deadline) = entry.timer {
			self.timers.remove(&(deadline, id));
			if let Some(waker) = self.entries.land(id, Err(Error::cancelled())) {
				waker.wake();
			}
		} else {
			self.backend.cancel(id);
		}
	}

	fn drive_to_idle(&mut self) -> Result<()> {
		self.poll(Some(Duration::ZERO))?;
		while !self.is_idle() {
			self.poll(None)?;
		}
		Ok(())
	}

	/// The teardown behind [`Reactor::shutdown`] and the driver's own drop; only
	/// the first call does anything. It ends with the driver holding nothing, and
	/// leaves every entry whose key is not yet released to resolve.
	fn shutdown(&mut self, force: bool) -> Result<()> {
		if std::mem::replace(&mut self.stopped, true) {
			return Ok(());
		}
		// Timers stop before the drain so their futures fail with the stopped
		// error instead of re-arming.
		for (_, id) in std::mem::take(&mut self.timers) {
			if let Some(waker) = self.entries.land(id, Err(Error::stopped())) {
				waker.wake();
			}
		}
		let cut: Vec<u64> = if force {
			self.entries
				.iter()
				.filter_map(|(id, entry)| (!entry.cancelled && !entry.is_done()).then_some(id))
				.collect()
		} else {
			Vec::new()
		};
		for &id in &cut {
			self.cancel(id);
		}
		let result = self.drive_to_idle();
		for id in cut {
			if let Some(entry) = self.entries.get_mut(id)
				&& matches!(&entry.response, Some(Err(error)) if error.kind() == ErrorKind::Cancelled)
			{
				entry.response = Some(Err(Error::stopped()));
			}
		}
		result
	}
}

impl Drop for Driver {
	fn drop(&mut self) {
		let _ = self.shutdown(true);
	}
}

/// Shared handle to the single-threaded I/O driver and timer queue.
#[derive(Clone)]
pub(crate) struct Reactor(Rc<RefCell<Driver>>);

impl Reactor {
	pub(crate) fn new(config: Config) -> Result<Self> {
		Ok(Reactor(Rc::new(RefCell::new(Driver::new(config)?))))
	}

	/// Returns a thread-safe [`Waker`] that wakes a blocking [`Reactor::drive`].
	pub(crate) fn waker(&self) -> Waker {
		self.0.borrow().backend.waker()
	}

	/// Polls the backend and fires expired timers, waiting up to `timeout`.
	pub(crate) fn drive(&self, timeout: Option<Duration>) -> Result<()> {
		self.0.borrow_mut().wait(timeout)
	}

	/// Stops accepting new submissions and drives all in-flight operations to
	/// completion (cancelling them first if `force` is true).
	pub(crate) fn shutdown(&self, force: bool) -> Result<()> {
		self.0.borrow_mut().shutdown(force)
	}

	/// Returns `true` if no backend operations are currently in flight.
	#[cfg(test)]
	pub(crate) fn idle(&self) -> bool {
		self.0.borrow().is_idle()
	}

	/// Submits `op` on `fd`, keeping `fd` and any referenced [`Buffer`](completion_backend::Buffer)
	/// alive until the operation completes and its entry is removed.
	pub(crate) fn submit(&self, fd: Arc<dyn AsDescriptor>, op: Op) -> Result<Key> {
		let buffer = op
			.span()
			.filter(|span| span.len() > 0)
			.map(|span| span.buffer().clone());
		let entry = Entry {
			response: None,
			waker: None,
			detached: false,
			cancelled: false,
			timer: None,
			_descriptor: Some(Arc::clone(&fd)),
			buffer,
		};
		// The entry goes in first, and the id it gets is what the backend files
		// the operation under, so a refusal rolls the slot straight back out.
		let mut driver = self.0.borrow_mut();
		if driver.stopped {
			return Err(Error::stopped());
		}
		let id = driver.entries.insert(entry);
		if let Err(error) = driver.backend.submit(id, fd.as_descriptor(), op) {
			driver.entries.remove(id);
			return Err(error);
		}
		Ok(Key(id))
	}

	/// Registers a timer expiring at `deadline`.
	pub(crate) fn submit_timer(&self, deadline: Instant) -> Result<Key> {
		let entry = Entry {
			response: None,
			waker: None,
			detached: false,
			cancelled: false,
			timer: Some(deadline),
			_descriptor: None,
			buffer: None,
		};
		let mut driver = self.0.borrow_mut();
		if driver.stopped {
			return Err(Error::stopped());
		}
		let id = driver.entries.insert(entry);
		driver.timers.insert((deadline, id));
		Ok(Key(id))
	}

	/// Takes the completed result for `key` if ready, or registers `waker` and
	/// returns `None`.
	pub(crate) fn poll(&self, key: Key, waker: &Waker) -> Option<Result<Response>> {
		let mut driver = self.0.borrow_mut();
		// The id names the entry's own slot: one the driver let go of names
		// nothing, not whatever operation took the slot afterwards.
		let Some(entry) = driver.entries.get_mut(key.0) else {
			return Some(Err(Error::cancelled()));
		};
		if !entry.is_done() {
			if !entry.waker.as_ref().is_some_and(|w| w.will_wake(waker)) {
				entry.waker = Some(waker.clone());
			}
			return None;
		}
		let response = driver
			.entries
			.remove(key.0)
			.and_then(|mut entry| entry.response.take())
			.unwrap_or_else(|| Err(Error::cancelled()));
		Some(response)
	}

	/// Requests asynchronous cancellation of the operation or timer `key`.
	pub(crate) fn cancel(&self, key: Key) {
		self.0.borrow_mut().cancel(key.0);
	}

	/// Detaches `key` when its future is dropped.
	///
	/// If the operation is still in flight, requests cancellation and keeps its
	/// [`Buffer`](completion_backend::Buffer) and descriptor alive until the backend
	/// reports completion.
	pub(crate) fn release(&self, key: Key) {
		let id = key.0;
		let mut driver = self.0.borrow_mut();
		// A key for an entry the driver let go of names nothing: its slot may
		// already belong to another operation.
		let Some(entry) = driver.entries.get_mut(id) else {
			return;
		};
		if entry.is_done() {
			driver.entries.remove(id);
			return;
		}
		if entry.detached {
			return;
		}
		entry.detached = true;
		driver.cancel(id);
	}
}
