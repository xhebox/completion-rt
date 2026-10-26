use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use crossbeam_queue::SegQueue;

use completion_backend::{
	AsDescriptor, Backend, Config, Error, Op, Response, Result, Waker as BackendWaker,
};

static GLOBAL_REACTOR: OnceLock<Shared> = OnceLock::new();

pub(crate) fn is_installed() -> bool {
	GLOBAL_REACTOR.get().is_some()
}

pub(crate) fn submit<D: AsDescriptor + 'static, M: Send + 'static>(
	fd: D,
	op: Op,
	memory: M,
) -> RemoteSlot<D, M> {
	let entry = Arc::new(Mutex::new(Entry {
		hold: Some((fd, memory)),
		state: State::Pending {
			id: None,
			waker: None,
		},
	}));
	send(Command::Submit(op, Arc::clone(&entry) as Arc<dyn EntryOps>));
	RemoteSlot(entry)
}

fn send(cmd: Command) {
	if let Some(shared) = GLOBAL_REACTOR.get() {
		shared.commands.push(cmd);
		shared.waker.wake();
	}
}

/// Spawns a dedicated background reactor thread built from `config` and routes
/// all subsequent I/O operations to it.
///
/// Once installed, the background reactor is always preferred over any
/// thread-local runtime for I/O submissions, allowing [`File`](crate::File),
/// [`Stream`](crate::Stream), [`StreamSocket`](crate::StreamSocket),
/// [`DatagramSocket`](crate::DatagramSocket), and [`Listener`](crate::Listener)
/// futures to be `.await`ed directly inside foreign executors such as `tokio`
/// or `smol`.
///
/// Like [`init`](crate::init), this can only succeed once per process: calling
/// it again fails with [`io::ErrorKind::AlreadyExists`].
#[fastrace::trace]
pub fn init_reactor(config: Config) -> Result<()> {
	static INIT_LOCK: Mutex<()> = Mutex::new(());

	let _guard = INIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
	if GLOBAL_REACTOR.get().is_some() {
		return Err(io::Error::new(
			io::ErrorKind::AlreadyExists,
			"a background reactor is already installed",
		)
		.into());
	}
	let mut backend = Backend::new(&config)?;
	let waker = backend.waker();
	std::thread::Builder::new()
		.name("completion-reactor".into())
		.spawn(move || {
			let shared = GLOBAL_REACTOR.wait();
			loop {
				while let Some(cmd) = shared.commands.pop() {
					match cmd {
						Command::Submit(op, entry) => entry.submit_to(op, &mut backend),
						Command::Cancel(id) => backend.cancel(id),
					}
				}
				if let Err(error) = backend.poll(None, |_id, entry, response| {
					entry.complete(response);
				}) {
					log::warn!(error:%; "the background completion reactor poll failed");
				}
			}
		})
		.map_err(Error::from)?;
	drop(GLOBAL_REACTOR.set(Shared {
		commands: SegQueue::new(),
		waker,
	}));
	Ok(())
}

enum Command {
	Submit(Op, Arc<dyn EntryOps>),
	Cancel(u64),
}

struct Shared {
	commands: SegQueue<Command>,
	waker: BackendWaker,
}

trait EntryOps: Send + Sync {
	fn submit_to(self: Arc<Self>, op: Op, backend: &mut Backend<Arc<dyn EntryOps>>);
	fn complete(&self, response: Result<Response>);
}

struct Entry<D, M> {
	hold: Option<(D, M)>,
	state: State,
}

enum State {
	Pending {
		id: Option<u64>,
		waker: Option<Waker>,
	},
	Done(Result<Response>),
}

impl<D: AsDescriptor + 'static, M: Send + 'static> EntryOps for Mutex<Entry<D, M>> {
	fn submit_to(self: Arc<Self>, op: Op, backend: &mut Backend<Arc<dyn EntryOps>>) {
		let mut entry = self.lock().unwrap();
		let Some((descriptor, _)) = &entry.hold else {
			return;
		};
		let fd = descriptor.as_descriptor();
		match backend.submit(fd, op, Arc::clone(&self) as Arc<dyn EntryOps>) {
			Ok(submitted) => {
				if let State::Pending { id, .. } = &mut entry.state {
					*id = Some(submitted);
				}
			}
			Err(error) => {
				drop(entry);
				self.complete(Err(error));
			}
		}
	}

	fn complete(&self, response: Result<Response>) {
		let mut entry = self.lock().unwrap();
		let prev = std::mem::replace(&mut entry.state, State::Done(response));
		drop(entry);
		if let State::Pending {
			waker: Some(waker), ..
		} = prev
		{
			waker.wake();
		}
	}
}

pub(crate) struct RemoteSlot<D, M>(Arc<Mutex<Entry<D, M>>>);

impl<D, M> Future for RemoteSlot<D, M> {
	type Output = (Result<Response>, D, M);

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let mut entry = self.0.lock().unwrap();
		if let State::Pending { waker, .. } = &mut entry.state {
			if waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
				*waker = Some(cx.waker().clone());
			}
			return Poll::Pending;
		}
		let (descriptor, memory) = entry.hold.take().expect("polled after completion");
		let State::Done(response) = std::mem::replace(
			&mut entry.state,
			State::Pending {
				id: None,
				waker: None,
			},
		) else {
			unreachable!()
		};
		Poll::Ready((response, descriptor, memory))
	}
}

impl<D, M> Drop for RemoteSlot<D, M> {
	fn drop(&mut self) {
		let mut entry = self.0.lock().unwrap();
		if entry.hold.is_none() {
			return;
		}
		match entry.state {
			State::Pending { id: None, .. } => entry.hold = None,
			State::Pending { id: Some(id), .. } => {
				drop(entry);
				send(Command::Cancel(id));
			}
			State::Done(_) => {}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::future::Future;
	use std::io::PipeReader;
	use std::pin::Pin;
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::sync::{Arc, Mutex};
	use std::task::{Context, Poll, Wake, Waker};

	use assert2::assert;
	use completion_backend::Response;

	use super::{Entry, EntryOps, RemoteSlot, State};

	struct Flag(Arc<AtomicBool>);

	impl Wake for Flag {
		fn wake(self: Arc<Self>) {
			self.0.store(true, Ordering::SeqCst);
		}
	}

	fn flag_waker() -> (Waker, Arc<AtomicBool>) {
		let flag = Arc::new(AtomicBool::new(false));
		(Waker::from(Arc::new(Flag(Arc::clone(&flag)))), flag)
	}

	fn new_entry<D, M>(fd: D, memory: M) -> Arc<Mutex<Entry<D, M>>> {
		Arc::new(Mutex::new(Entry {
			hold: Some((fd, memory)),
			state: State::Pending {
				id: None,
				waker: None,
			},
		}))
	}

	#[test]
	fn entry_and_remote_slot_lifecycle_and_cancellation() {
		let (waker, woken) = flag_waker();
		let fd: Arc<PipeReader> = Arc::new(std::io::pipe().unwrap().0);

		// 1. Pending -> Done -> polled
		let entry = new_entry(Arc::clone(&fd), 42usize);
		let mut slot = RemoteSlot(Arc::clone(&entry));
		let mut cx = Context::from_waker(&waker);
		assert!(Pin::new(&mut slot).poll(&mut cx).is_pending());
		entry.complete(Ok(Response::Count(5)));
		assert!(woken.load(Ordering::SeqCst));
		assert!(
			let Poll::Ready((Ok(Response::Count(5)), returned_fd, 42)) =
				Pin::new(&mut slot).poll(&mut cx)
		);
		drop(returned_fd);
		assert!(Arc::strong_count(&fd) == 1);
		drop(slot);

		// 2. Dropping a queued slot (`id: None`) releases descriptor immediately
		let queued = new_entry(Arc::clone(&fd), 99usize);
		let slot = RemoteSlot(Arc::clone(&queued));
		assert!(Arc::strong_count(&fd) == 2);
		drop(slot);
		assert!(Arc::strong_count(&fd) == 1);

		// 3. Dropping an in-flight slot (`id: Some`) keeps descriptor until `Entry` drops
		let in_flight = new_entry(Arc::clone(&fd), 0usize);
		if let State::Pending { id, .. } = &mut in_flight.lock().unwrap().state {
			*id = Some(77);
		}
		drop(RemoteSlot(Arc::clone(&in_flight)));
		assert!(Arc::strong_count(&fd) == 2);
		drop(in_flight);
		assert!(Arc::strong_count(&fd) == 1);
	}
}
