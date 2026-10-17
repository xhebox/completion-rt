//! The completion engine.
//!
//! A [`Reactor`] owns the target's [`Backend`], the operations in flight, and
//! the timers among them. It keeps the table of entries the backend's ids name,
//! waits for the nearest deadline, and hands each landed result to its holder.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;
use std::time::{Duration, Instant};

use completion_backend::{AsDescriptor, Backend, Config, Error, ErrorKind, Response};

mod entries;
#[cfg(test)]
mod tests;

pub(crate) use completion_backend::Op;

use entries::{Entries, Entry};

/// Names one operation or timer the reactor holds, until its result is taken or
/// it is released.
///
/// The key is plain data: dropping or forgetting it does nothing. The holder ends
/// it with [`Reactor::release`]; until then it names the same operation, and once
/// that operation is gone the key names nothing.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Key(u64);

/// The operations in flight, the timers among them, and the mechanism that
/// runs them.
///
/// One `RefCell` in the reactor holds all of it, so a drive pass borrows the
/// table, the timer set, and the backend together for the whole pass: a waker
/// that drops an operation runs into that borrow instead of freeing memory the
/// kernel can still write into.
struct Driver {
	/// Set by teardown: refusing new submissions and timers, and making a second
	/// teardown a no-op.
	stopped: bool,
	/// Armed deadlines, nearest first. Reached only through this borrow, so the
	/// wakers a fire raises run under the same invariant as the operations'.
	timers: BTreeSet<(Instant, u64)>,
	entries: Entries,
	backend: Backend,
	completed: Vec<(u64, Result<Response, Error>)>,
}

impl Driver {
	fn new(config: Config) -> Result<Driver, Error> {
		Ok(Driver {
			stopped: false,
			timers: BTreeSet::new(),
			entries: Entries::new(),
			backend: Backend::new(&config)?,
			completed: Vec::new(),
		})
	}

	/// Collects the backend's completions and wakes whoever waits. The whole
	/// pass runs on `&mut self`, so a waker that re-enters the reactor panics
	/// instead of touching the driver mid-flight.
	fn poll(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
		self.backend.poll(timeout, &mut self.completed)?;
		for (id, response) in self.completed.drain(..) {
			if let Some(waker) = self.entries.land(id, response) {
				waker.wake();
			}
		}
		Ok(())
	}

	/// Whether every operation the driver holds has landed. An armed timer counts
	/// as landed: it is the reactor's own, so it holds no backend work up.
	fn is_idle(&self) -> bool {
		self.entries
			.iter()
			.all(|(_, entry)| entry.is_done() || entry.timer.is_some())
	}

	fn wait(&mut self, timeout: Option<Duration>) -> Result<(), Error> {
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

	fn drive_to_idle(&mut self) -> Result<(), Error> {
		self.poll(Some(Duration::ZERO))?;
		while !self.is_idle() {
			self.poll(None)?;
		}
		Ok(())
	}

	/// The teardown behind [`Reactor::shutdown`] and the driver's own drop; only
	/// the first call does anything. It ends with the driver holding nothing, and
	/// leaves every entry whose key is not yet released to resolve.
	fn shutdown(&mut self, force: bool) -> Result<(), Error> {
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

/// The completion backend, the operations in flight, and the timers among them.
///
/// A handle: every clone drives and submits through the same reactor. Transfers
/// may complete short; use the facade's `*_exact` and `*_all` methods to fill a
/// range.
#[derive(Clone)]
pub(crate) struct Reactor(Rc<RefCell<Driver>>);

impl Reactor {
	/// Each reactor has its own state, they can coexist but [`Reactor::drive`] will block.
	pub(crate) fn new(config: Config) -> Result<Self, Error> {
		Ok(Reactor(Rc::new(RefCell::new(Driver::new(config)?))))
	}

	/// Send and Sync [`Waker`] that wakes a blocked [`Reactor::drive`].
	pub(crate) fn waker(&self) -> Waker {
		self.0.borrow().backend.waker()
	}

	/// Drives the [`Reactor`].
	pub(crate) fn drive(&self, timeout: Option<Duration>) -> Result<(), Error> {
		self.0.borrow_mut().wait(timeout)
	}

	/// Refuses new submissions and returns once the backend holds nothing.
	/// With `force`, in-flight submissions are cancelled.
	pub(crate) fn shutdown(&self, force: bool) -> Result<(), Error> {
		self.0.borrow_mut().shutdown(force)
	}

	/// Whether the backend holds nothing: every operation it still holds has
	/// landed, and a result waiting for its holder still counts as held.
	#[cfg(test)]
	pub(crate) fn idle(&self) -> bool {
		self.0.borrow().is_idle()
	}

	/// Submits `op` on `fd`, holding `fd`'s resource until the operation's entry goes.
	/// The entry goes once its result is taken or [`Reactor::release`] lets it go.
	///
	/// The request reaches the backend before this returns; one the backend
	/// refuses lands as the operation's own result.
	///
	/// A transfer's extents are resolved here: one outside its space fails the
	/// submission with [`InvalidInput`](std::io::ErrorKind::InvalidInput).
	///
	/// # Panics
	/// If the driver is busy: the call came from a waker the reactor is running.
	/// A waker only schedules; the poll that follows submits.
	pub(crate) fn submit(&self, fd: Arc<dyn AsDescriptor>, op: Op) -> Result<Key, Error> {
		let space = op.space();
		let entry = Entry {
			response: None,
			waker: None,
			detached: false,
			cancelled: false,
			timer: None,
			_descriptor: Some(Arc::clone(&fd)),
			space,
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

	/// Arms a timer for `deadline`, holding nothing but the deadline itself until
	/// the timer fires or is released.
	pub(crate) fn submit_timer(&self, deadline: Instant) -> Result<Key, Error> {
		let entry = Entry {
			response: None,
			waker: None,
			detached: false,
			cancelled: false,
			timer: Some(deadline),
			_descriptor: None,
			space: None,
		};
		let mut driver = self.0.borrow_mut();
		if driver.stopped {
			return Err(Error::stopped());
		}
		let id = driver.entries.insert(entry);
		driver.timers.insert((deadline, id));
		Ok(Key(id))
	}

	/// The result of the operation or timer `key` if it has landed, which takes it
	/// and ends the entry; otherwise `waker` is woken when it lands.
	///
	/// A key whose result was taken, or that was released, names nothing and
	/// reports [`ErrorKind::Cancelled`].
	pub(crate) fn poll(&self, key: Key, waker: &Waker) -> Option<Result<Response, Error>> {
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

	/// Asks the operation or timer `key` to give up, without waiting for it. An
	/// operation's result still lands, and a cancel lands as
	/// [`ErrorKind::Cancelled`]; a timer is disarmed at once, fired or not.
	pub(crate) fn cancel(&self, key: Key) {
		self.0.borrow_mut().cancel(key.0);
	}

	/// Lets the operation or timer `key` go without its result. Releasing a key
	/// whose result was already taken does nothing.
	///
	/// An operation still in flight is cancelled and detached without blocking:
	/// the reactor keeps its [`Space`](completion_backend::Space) and descriptor alive
	/// until the backend reports its completion, though the span's byte range
	/// remains in flight until then.
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
