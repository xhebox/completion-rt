//! A set of tasks a thread owns, so the loop that spawned them can wait.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use async_executor::Task;

use crate::{Cancel, Event, Spawner};

/// The tasks a device runs beside its own loop, kept so the loop can wait for
/// them.
///
/// A task spawned and left alone lives until the thread's executor is dropped —
/// which is after the loop that spawned it has returned, and after the caller
/// has been told the device stopped. It may still be reading or writing the
/// memory that loop handed it. Keeping the handle instead lets
/// [`Tasks::shutdown`] return only once every task has finished, which is what
/// a device needs before it reports a stop or a reset as done.
///
/// Cloning gives another handle on the same set: a device that hands its work
/// to a backend keeps one set, wherever the task is spawned from.
#[derive(Clone)]
pub struct Tasks {
	inner: Rc<TasksInner>,
}

struct TasksInner {
	spawner: Spawner,
	/// The reactor's own wake-up: a task the set has not polled yet has no
	/// waker registered anywhere, and the thread parks on the reactor between
	/// polls, so raising this is what makes it run.
	event: Event,
	tasks: RefCell<Vec<Task<()>>>,
}

impl Tasks {
	/// A set of tasks on `spawner`'s thread.
	pub fn new(spawner: &Spawner) -> Tasks {
		Tasks {
			inner: Rc::new(TasksInner {
				spawner: spawner.clone(),
				event: spawner.event(),
				tasks: RefCell::new(Vec::new()),
			}),
		}
	}

	/// Spawns `future` on the thread and keeps its handle.
	pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
		let mut tasks = self.inner.tasks.borrow_mut();
		tasks.retain(|task| !task.is_finished());
		tasks.push(self.inner.spawner.spawn(future));
	}

	/// Ends the set: fires `cancel` and waits for every task, including any
	/// spawned while waiting.
	///
	/// The two go together — a task's owner is the one that fires what the task
	/// ends on, and every device stop is this pair — so they are one call. A
	/// task nothing ends waits forever here.
	pub async fn shutdown(&self, cancel: &Cancel) {
		cancel.cancel();
		self.join().await;
	}

	/// Waits for every task in the set, and for any spawned while waiting.
	async fn join(&self) {
		loop {
			// The borrow is taken per task and released before the wait: a
			// task that spawns its successor does it while this runs.
			let task = self.inner.tasks.borrow_mut().pop();
			let Some(task) = task else {
				return;
			};
			// A task the set has not polled yet is woken by nothing: the
			// thread parked on the reactor has to be told to run it.
			let _ = self.inner.event.notify();
			task.await;
		}
	}
}

impl Drop for TasksInner {
	fn drop(&mut self) {
		// The handles went with the set: a task still in it goes on until the
		// thread's executor is dropped, on the memory its loop handed it.
		let unfinished = self
			.tasks
			.borrow()
			.iter()
			.filter(|task| !task.is_finished())
			.count();
		if unfinished != 0 {
			log::debug!(unfinished; "a task set was dropped with tasks still running");
		}
	}
}
