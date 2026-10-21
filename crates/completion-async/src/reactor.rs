use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use completion_backend::{AsDescriptor, Backend, Buffer, Config, Error, Op, Response, Result};
use smallvec::SmallVec;

#[cfg(test)]
mod tests;

thread_local! {
	pub(crate) static REACTOR: RefCell<Option<Driver>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy)]
enum Key {
	Op(u64),
	Timer(TimerKey),
}

type TimerKey = (Instant, u64);

struct Entry {
	inner: Mutex<EntryInner>,
}

struct EntryInner {
	response: Option<Result<Response>>,
	wakers: SmallVec<[Waker; 2]>,
	cancelled: bool,
	/// Remains `true` after `response` has been taken so [`Slot::wait`] and
	/// scope pruning still see the entry as resolved.
	done: bool,
	/// Kept alive while the operation is in flight in the backend.
	descriptor: Option<Arc<dyn AsDescriptor>>,
	/// Kept alive while the operation is in flight in the backend.
	buffer: Option<Buffer>,
}

impl Entry {
	fn pending(descriptor: Option<Arc<dyn AsDescriptor>>, buffer: Option<Buffer>) -> Entry {
		Entry {
			inner: Mutex::new(EntryInner {
				response: None,
				wakers: SmallVec::new(),
				cancelled: false,
				done: false,
				descriptor,
				buffer,
			}),
		}
	}

	fn lock(&self) -> MutexGuard<'_, EntryInner> {
		self.inner.lock().unwrap_or_else(|p| p.into_inner())
	}

	fn complete(&self, response: Result<Response>) {
		let mut inner = self.lock();
		inner.buffer = None;
		inner.descriptor = None;
		inner.response = Some(response);
		inner.done = true;
		let wakers = std::mem::take(&mut inner.wakers);
		// Drop the lock before waking so a synchronous poll on the same thread
		// does not deadlock.
		drop(inner);
		for waker in wakers {
			waker.wake();
		}
	}

	fn is_done(&self) -> bool {
		self.lock().done
	}

	fn mark_cancelled(&self) -> bool {
		let mut inner = self.lock();
		!std::mem::replace(&mut inner.cancelled, true) && !inner.done
	}

	fn take_or_register(&self, waker: &Waker) -> Option<Result<Response>> {
		let mut inner = self.lock();
		if let Some(response) = inner.response.take() {
			return Some(response);
		}
		if !inner.wakers.iter().any(|w| w.will_wake(waker)) {
			inner.wakers.push(waker.clone());
		}
		None
	}

	fn register(&self, waker: &Waker) -> bool {
		let mut inner = self.lock();
		if inner.done {
			return true;
		}
		if !inner.wakers.iter().any(|w| w.will_wake(waker)) {
			inner.wakers.push(waker.clone());
		}
		false
	}

	fn take_response(&self) -> Option<Result<Response>> {
		self.lock().response.take()
	}
}

struct Shared {
	inner: Mutex<SharedInner>,
}

struct SharedInner {
	stopped: bool,
	/// Cancellations queued from any thread or during `drive`, drained by the
	/// driver before polling the backend.
	cancellations: Vec<u64>,
	timers: BTreeMap<TimerKey, Arc<Entry>>,
	next_timer: u64,
	waker: Waker,
}

pub(crate) struct Driver {
	backend: Backend<Arc<Entry>>,
	shared: Arc<Shared>,
}

impl Drop for Driver {
	fn drop(&mut self) {
		let _ = self.teardown(true);
	}
}

impl Shared {
	fn lock(&self) -> MutexGuard<'_, SharedInner> {
		self.inner.lock().unwrap_or_else(|p| p.into_inner())
	}

	fn queue_cancel(&self, id: u64) {
		let mut inner = self.lock();
		inner.cancellations.push(id);
		inner.waker.wake_by_ref();
	}

	fn release(&self, key: Key, entry: &Entry) {
		match key {
			Key::Op(id) => {
				if !entry.mark_cancelled() {
					return;
				}
				self.queue_cancel(id);
			}
			Key::Timer(key) => {
				self.lock().timers.remove(&key);
			}
		}
	}

	fn cancel_timer(&self, key: TimerKey) {
		let timer = self.lock().timers.remove(&key);
		if let Some(timer) = timer {
			timer.complete(Err(Error::cancelled()));
		}
	}

	fn take_cancellations(&self) -> Vec<u64> {
		std::mem::take(&mut self.lock().cancellations)
	}

	fn is_stopped(&self) -> bool {
		self.lock().stopped
	}

	fn stop(&self) -> Option<BTreeMap<TimerKey, Arc<Entry>>> {
		let mut inner = self.lock();
		if std::mem::replace(&mut inner.stopped, true) {
			return None;
		}
		Some(std::mem::take(&mut inner.timers))
	}

	fn add_timer(&self, deadline: Instant) -> Option<(Key, Arc<Entry>)> {
		let mut inner = self.lock();
		if inner.stopped {
			return None;
		}
		let entry = Arc::new(Entry::pending(None, None));
		let key = (deadline, inner.next_timer);
		inner.next_timer += 1;
		inner.timers.insert(key, Arc::clone(&entry));
		Some((Key::Timer(key), entry))
	}

	fn expire_timers(&self) {
		let mut inner = self.lock();
		let now = Instant::now();
		while let Some((&(deadline, _), _)) = inner.timers.first_key_value() {
			if deadline > now {
				break;
			}
			let (_, timer) = inner
				.timers
				.pop_first()
				.expect("the first timer is present");
			timer.complete(Ok(Response::Done));
		}
	}

	fn within(&self, timeout: Option<Duration>) -> Option<Duration> {
		let inner = self.lock();
		let nearest = inner
			.timers
			.first_key_value()
			.map(|(&(deadline, _), _)| deadline.saturating_duration_since(Instant::now()));
		match (timeout, nearest) {
			(Some(timeout), Some(nearest)) => Some(timeout.min(nearest)),
			(timeout, nearest) => timeout.or(nearest),
		}
	}
}

impl Driver {
	pub(crate) fn new(config: Config) -> Result<Self> {
		let backend = Backend::new(&config)?;
		let waker = backend.waker();
		Ok(Driver {
			backend,
			shared: Arc::new(Shared {
				inner: Mutex::new(SharedInner {
					stopped: false,
					cancellations: Vec::new(),
					timers: BTreeMap::new(),
					next_timer: 0,
					waker,
				}),
			}),
		})
	}

	pub(crate) fn waker(&self) -> Waker {
		self.backend.waker()
	}

	pub(crate) fn drive(&mut self, timeout: Option<Duration>) -> Result<()> {
		self.flush();
		let within = self.within(timeout);
		let polled = self.backend.poll(within, |_id, entry, response| {
			entry.complete(response);
		});
		self.expire_timers();
		polled
	}

	fn flush(&mut self) {
		for id in self.shared.take_cancellations() {
			self.backend.cancel(id);
		}
	}

	fn expire_timers(&self) {
		self.shared.expire_timers();
	}

	fn within(&self, timeout: Option<Duration>) -> Option<Duration> {
		self.shared.within(timeout)
	}

	pub(crate) fn shutdown(mut self, force: bool) -> Result<()> {
		self.teardown(force)
	}

	fn teardown(&mut self, force: bool) -> Result<()> {
		let Some(timers) = self.shared.stop() else {
			return Ok(());
		};
		for timer in timers.into_values() {
			timer.complete(Err(Error::cancelled()));
		}
		if force {
			self.backend.cancel_all();
		}
		let mut result = Ok(());
		while !self.is_idle() {
			if let Err(error) = self.drive(None) {
				result = Err(error);
				break;
			}
		}
		result
	}

	pub(crate) fn is_idle(&self) -> bool {
		self.backend.is_idle()
	}

	pub(crate) fn submit(&mut self, fd: Arc<dyn AsDescriptor>, op: Op) -> Slot {
		let buffer = op
			.span()
			.filter(|span| span.len() > 0)
			.map(|span| span.buffer().clone());
		let entry = Arc::new(Entry::pending(Some(Arc::clone(&fd)), buffer));
		if self.shared.is_stopped() {
			return Slot::refused(Error::stopped());
		}
		match self
			.backend
			.submit(fd.as_descriptor(), op, Arc::clone(&entry))
		{
			Ok(id) => Slot::new(Arc::clone(&self.shared), Key::Op(id), entry),
			Err(error) => Slot::refused(error),
		}
	}

	pub(crate) fn submit_timer(&self, deadline: Instant) -> Slot {
		match self.shared.add_timer(deadline) {
			Some((key, entry)) => Slot::new(Arc::clone(&self.shared), key, entry),
			None => Slot::refused(Error::stopped()),
		}
	}
}

pub(crate) fn enter<R>(
	driver: Option<Driver>,
	f: impl FnOnce(&mut Driver) -> R,
) -> (Option<Driver>, R) {
	REACTOR.with(|cell| {
		let previous = if let Some(driver) = driver {
			cell.borrow_mut().replace(driver)
		} else {
			if cell.borrow().is_none() {
				*cell.borrow_mut() = Some(
					Driver::new(Config::default())
						.expect("the thread's default reactor could not be created"),
				);
			}
			None
		};
		let result = f(cell.borrow_mut().as_mut().unwrap());
		(previous, result)
	})
}

pub(crate) fn submit(fd: Arc<dyn AsDescriptor>, op: Op) -> Slot {
	enter(None, |driver| driver.submit(fd, op)).1
}

pub(crate) fn submit_timer(deadline: Instant) -> Slot {
	enter(None, |driver| driver.submit_timer(deadline)).1
}

#[allow(private_interfaces)]
pub(crate) enum Slot {
	Live {
		shared: Arc<Shared>,
		entry: Arc<Entry>,
		key: Option<Key>,
		/// Only the primary slot (returned by submission) releases the
		/// operation on drop; cloned handles held by a [`Scope`](crate::Scope)
		/// do not.
		owner: bool,
	},
	Refused(Option<Result<Response>>),
}

impl Clone for Slot {
	fn clone(&self) -> Self {
		match self {
			Slot::Live {
				shared, entry, key, ..
			} => Slot::Live {
				shared: Arc::clone(shared),
				entry: Arc::clone(entry),
				key: *key,
				owner: false,
			},
			Slot::Refused(_) => Slot::Refused(None),
		}
	}
}

impl Drop for Slot {
	fn drop(&mut self) {
		let Slot::Live {
			shared,
			entry,
			key,
			owner: true,
		} = self
		else {
			return;
		};
		if let Some(key) = key.take() {
			shared.release(key, entry);
		}
		let response = entry.take_response();
		drop(response);
	}
}

impl Slot {
	fn new(shared: Arc<Shared>, key: Key, entry: Arc<Entry>) -> Slot {
		Slot::Live {
			shared,
			entry,
			key: Some(key),
			owner: true,
		}
	}

	pub(crate) fn refused(error: Error) -> Slot {
		Slot::Refused(Some(Err(error)))
	}

	pub(crate) fn cancel(&self) {
		let Slot::Live {
			shared, entry, key, ..
		} = self
		else {
			return;
		};
		let Some(key) = key else {
			return;
		};
		match *key {
			Key::Timer(key) => shared.cancel_timer(key),
			Key::Op(id) => {
				if !entry.mark_cancelled() {
					return;
				}
				shared.queue_cancel(id);
			}
		}
	}

	pub(crate) fn is_done(&self) -> bool {
		match self {
			Slot::Live { entry, .. } => entry.is_done(),
			Slot::Refused(_) => true,
		}
	}

	pub(crate) fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
		let entry = match self {
			Slot::Live { entry, .. } => Some(Arc::clone(entry)),
			Slot::Refused(_) => None,
		};
		poll_fn(move |cx| match &entry {
			Some(entry) => {
				if entry.register(cx.waker()) {
					return Poll::Ready(());
				}
				Poll::Pending
			}
			None => Poll::Ready(()),
		})
	}
}

impl Future for Slot {
	type Output = Result<Response>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Response>> {
		let this = self.get_mut();
		match this {
			Slot::Live { entry, key, .. } => {
				if let Some(response) = entry.take_or_register(cx.waker()) {
					*key = None;
					return Poll::Ready(response);
				}
				Poll::Pending
			}
			Slot::Refused(response) => {
				Poll::Ready(response.take().expect("a refused slot is polled only once"))
			}
		}
	}
}
