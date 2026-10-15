//! File operations: the read, write, and sync.

use std::fs;
use std::io;
use std::path::Path;

use completion_rt::{Handle, Op as LowOp, Reactor};

use crate::Executor;
use crate::completion::submit;
use crate::global;
use crate::net::{Kind, Op, Transfer};
use crate::space::Span;

/// `std::fs::OpenOptions` plus direct I/O, and on Windows the overlapped flag
/// the iocp backend needs.
pub struct OpenOptions {
	inner: std::fs::OpenOptions,
	direct: bool,
}

impl OpenOptions {
	pub fn new() -> OpenOptions {
		OpenOptions {
			inner: std::fs::OpenOptions::new(),
			direct: false,
		}
	}

	pub fn read(mut self, read: bool) -> OpenOptions {
		self.inner.read(read);
		self
	}

	pub fn write(mut self, write: bool) -> OpenOptions {
		self.inner.write(write);
		self
	}

	pub fn append(mut self, append: bool) -> OpenOptions {
		self.inner.append(append);
		self
	}

	pub fn truncate(mut self, truncate: bool) -> OpenOptions {
		self.inner.truncate(truncate);
		self
	}

	pub fn create(mut self, create: bool) -> OpenOptions {
		self.inner.create(create);
		self
	}

	pub fn create_new(mut self, create_new: bool) -> OpenOptions {
		self.inner.create_new(create_new);
		self
	}

	pub fn direct(mut self) -> OpenOptions {
		self.direct = true;
		self
	}

	pub fn open(self, path: &Path) -> io::Result<fs::File> {
		let OpenOptions { inner, direct } = self;
		#[cfg(not(any(
			target_os = "linux",
			target_os = "android",
			windows,
			target_vendor = "apple"
		)))]
		if direct {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"direct I/O is not implemented on this platform",
			));
		}
		#[cfg(any(target_os = "linux", target_os = "android"))]
		let inner = {
			use std::os::unix::fs::OpenOptionsExt;
			let mut inner = inner;
			if direct {
				inner.custom_flags(rustix::fs::OFlags::DIRECT.bits() as _);
			}
			inner
		};
		#[cfg(windows)]
		let inner = {
			use std::os::windows::fs::OpenOptionsExt;
			use windows_sys::Win32::Storage::FileSystem::{
				FILE_FLAG_NO_BUFFERING, FILE_FLAG_OVERLAPPED,
			};
			let mut inner = inner;
			// A completion port only owns handles opened for overlapped I/O.
			let mut flags = FILE_FLAG_OVERLAPPED;
			if direct {
				flags |= FILE_FLAG_NO_BUFFERING;
			}
			inner.custom_flags(flags);
			inner
		};
		let file = inner.open(path)?;
		#[cfg(target_vendor = "apple")]
		if direct {
			// No O_DIRECT here; F_NOCACHE is the flag that skips the cache.
			rustix::fs::fcntl_nocache(&file, true)?;
		}
		Ok(file)
	}
}

/// An owned resource with async operations submitted through its
/// [`Executor`](crate::Executor).
///
/// Which operations a facade has is its resource type, named where the
/// operations are: a `File` is read at an offset and synced, a `TcpStream` is
/// received and sent on. A socket is not read at an offset or synced:
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn read(socket: &Facade<std::net::TcpStream>) {
/// 	let _ = socket.read_at(vec![0u8; 4], 0);
/// }
/// ```
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn sync(socket: &Facade<std::net::TcpStream>) {
/// 	let _ = socket.sync();
/// }
/// ```
///
/// Nor is a pipe synced:
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn sync(pipe: &Facade<std::io::PipeWriter>) {
/// 	let _ = pipe.sync();
/// }
/// ```
///
/// Nor does a resource with a transfer have a readiness wait: the backend arms
/// the readiness itself when a call would block. Only [`Signal`](crate::Signal)
/// waits.
///
/// ```compile_fail
/// # use completion_async::Facade;
/// fn wait(socket: &Facade<std::net::TcpStream>) {
/// 	let _ = socket.wait_readable();
/// }
/// ```
pub struct Facade<S> {
	handle: Handle<S>,
	reactor: Reactor,
}

impl<S> Facade<S> {
	/// Wraps `resource`, its operations going to this thread's global
	/// executor.
	///
	/// See [`Facade::with_executor`] for the resource's requirements.
	#[fastrace::trace]
	pub fn new(resource: impl Into<S>) -> Facade<S> {
		Self::from_parts(Handle::new(resource.into()), global::reactor())
	}

	/// Wraps `resource`, its operations going to `executor`.
	///
	/// On Windows the resource must be opened for overlapped I/O: a file
	/// through [`OpenOptions`], which asks for `FILE_FLAG_OVERLAPPED`. A handle
	/// opened without that flag cannot be owned by a completion port, so its
	/// operations fail with the invalid-parameter error the association gives.
	#[fastrace::trace]
	pub fn with_executor(resource: impl Into<S>, executor: &Executor) -> Facade<S> {
		Self::from_parts(Handle::new(resource.into()), executor.reactor().clone())
	}

	/// Wraps a `handle` the caller already holds, its operations going to this
	/// thread's global executor; the facade shares the handle rather than taking
	/// it over.
	pub fn bind(handle: Handle<S>) -> Facade<S> {
		Self::from_parts(handle, global::reactor())
	}

	/// Wraps a `handle` the caller already holds, its operations going to
	/// `executor`; the facade shares the handle rather than taking it over.
	pub fn bind_with_executor(handle: Handle<S>, executor: &Executor) -> Facade<S> {
		Self::from_parts(handle, executor.reactor().clone())
	}

	pub(super) fn from_parts(handle: Handle<S>, reactor: Reactor) -> Facade<S> {
		Self { handle, reactor }
	}

	pub(super) fn reactor(&self) -> &Reactor {
		&self.reactor
	}

	pub(super) fn handle(&self) -> &Handle<S> {
		&self.handle
	}

	/// Waits for all other facade clones and operations to release the resource; returns `None` if another take is already waiting.
	pub async fn take(self) -> Option<S> {
		self.handle.take().await
	}

	/// [`take`](Facade::take) without waiting: `Err(self)` while another clone
	/// or an operation still names the resource.
	pub fn try_take(self) -> Result<S, Facade<S>> {
		match self.handle.try_take() {
			Ok(resource) => Ok(resource),
			Err(handle) => Err(Self {
				handle,
				reactor: self.reactor,
			}),
		}
	}
}

impl<S> Clone for Facade<S> {
	fn clone(&self) -> Facade<S> {
		Self {
			handle: self.handle.clone(),
			reactor: self.reactor.clone(),
		}
	}
}

pub type File = Facade<fs::File>;

/// The four read and write methods of a resource read and written at an
/// offset, expanded one impl per resource type named at the invocation.
///
/// A macro rather than a bound: the types are a closed list, and naming one
/// here is what gives it these methods. A resource type of the caller's own
/// converts into one of them — [`Facade::new`] takes a value that converts
/// into the resource.
macro_rules! positional {
	($($resource:ty),* $(,)?) => {
		$(
			impl Facade<$resource> {
				/// Reads into `span` at `fdoff`, in one call: a short count is what the
				/// call moved, not an error. [`read_exact_at`](Self::read_exact_at)
				/// loops.
				pub fn read_at<'a>(&self, span: Span<'a>, fdoff: u64) -> Transfer<'a, $resource> {
					Transfer::new(self, Kind::Read { fdoff }, span)
				}

				/// Reads into `span` at `fdoff`, over as many calls as it takes to fill
				/// it.
				///
				/// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) when the file ends
				/// first.
				pub fn read_exact_at<'a>(&self, span: Span<'a>, fdoff: u64) -> Transfer<'a, $resource> {
					Transfer::new(self, Kind::ReadAll { fdoff }, span)
				}

				/// Writes `span` at `fdoff`, in one call: a short count is what the call
				/// moved, not an error. [`write_all_at`](Self::write_all_at) loops.
				pub fn write_at<'a>(&self, span: Span<'a>, fdoff: u64) -> Transfer<'a, $resource> {
					Transfer::new(self, Kind::Write { fdoff }, span)
				}

				/// Writes `span` at `fdoff`, over as many calls as it takes to write it
				/// all.
				///
				/// [`WriteZero`](io::ErrorKind::WriteZero) when a call writes nothing.
				pub fn write_all_at<'a>(&self, span: Span<'a>, fdoff: u64) -> Transfer<'a, $resource> {
					Transfer::new(self, Kind::WriteAll { fdoff }, span)
				}
			}
		)*
	};
}

positional!(
	std::fs::File,
	std::io::Stdin,
	std::io::Stdout,
	std::io::Stderr,
	std::io::PipeReader,
	std::io::PipeWriter,
);

#[cfg(unix)]
positional!(
	std::os::fd::OwnedFd,
	std::process::ChildStdin,
	std::process::ChildStdout,
	std::process::ChildStderr,
);

#[cfg(windows)]
positional!(
	std::os::windows::io::OwnedHandle,
	completion_rt::OwnedDescriptor,
);

impl Facade<fs::File> {
	pub fn sync(&self) -> Op<fs::File> {
		Op::new(self, |reactor, handle| {
			submit(reactor, handle, LowOp::Fsync)
		})
	}

	/// Reports the resource's pending deferred write-back errors — `ENOSPC`,
	/// `EIO` and the like — without forcing anything to disk.
	///
	/// This is not [`Write::flush`](std::io::Write::flush): it names no buffer
	/// of the caller's, and it does not make the data durable — [`sync`](Self::sync)
	/// is what does that. What it reports is the error the filesystem had
	/// deferred and would otherwise surface at the resource's next close.
	pub fn flush(&self) -> Op<fs::File> {
		Op::new(self, |reactor, handle| {
			submit(reactor, handle, LowOp::Flush)
		})
	}

	/// Backs `len` bytes at `offset` with storage, so a write there does not
	/// fail for space. A range past the end grows the file to the range's end,
	/// reading as zeros; the file never shrinks.
	pub fn allocate(&self, offset: u64, len: u64) -> Op<fs::File> {
		Op::new(self, move |reactor, handle| {
			submit(reactor, handle, LowOp::Allocate { offset, len })
		})
	}

	/// Zeroes `len` bytes at `offset`, freeing their storage where the
	/// filesystem can; the length stays.
	///
	/// On Windows the storage is freed only in a file marked sparse.
	pub fn punch(&self, offset: u64, len: u64) -> Op<fs::File> {
		Op::new(self, move |reactor, handle| {
			submit(reactor, handle, LowOp::Punch { offset, len })
		})
	}
}
