//! File operations: the read, write, and sync.

use std::fs;
use std::io;
use std::marker::PhantomData;
use std::panic::Location;
use std::path::Path;
use std::sync::Arc;

use completion_rt::{AsDescriptor, Extent, Extents, Handle, Lease, Memory, Submitter};

use crate::net::{Kind, Op, Transfer, whole};

/// Memory handed to an operation: owned by it, or lent by a
/// [`Scope`](crate::Scope).
pub struct Lent<'a> {
	pub(crate) memory: Arc<dyn Memory>,
	pub(crate) lease: Option<Arc<Lease>>,
	borrow: PhantomData<&'a ()>,
}

impl<'a> Lent<'a> {
	/// Memory a scope lends under `lease`; the caller vouches that `memory`
	/// outlives every operation submitted under it.
	pub(crate) fn lent(memory: Arc<dyn Memory>, lease: Arc<Lease>) -> Lent<'a> {
		Lent {
			memory,
			lease: Some(lease),
			borrow: PhantomData,
		}
	}
}

/// What an operation takes as its memory.
///
/// Any `'static` [`Memory`] — a `Vec<u8>`, an `Arc` of one — is moved in. A
/// borrow goes through [`Scope::lend`](crate::Scope::lend), which ties the
/// operation to the scope.
pub trait IntoMemory<'a> {
	fn into_memory(self) -> Lent<'a>;
}

impl<T: Memory + 'static> IntoMemory<'static> for T {
	fn into_memory(self) -> Lent<'static> {
		Lent {
			memory: Arc::new(self),
			lease: None,
			borrow: PhantomData,
		}
	}
}

impl<'a> IntoMemory<'a> for Lent<'a> {
	fn into_memory(self) -> Lent<'a> {
		self
	}
}

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

/// An owned resource with async operations submitted through a
/// [`Submitter`](crate::Submitter).
pub struct Facade<S> {
	handle: Handle<S>,
	submitter: Submitter,
}

impl<S> Facade<S> {
	/// Wraps `resource`, its operations going to `submitter`.
	///
	/// On Windows the resource must be opened for overlapped I/O: a file
	/// through [`OpenOptions`], which asks for `FILE_FLAG_OVERLAPPED`. A handle
	/// opened without that flag cannot be owned by a completion port, so its
	/// operations fail with the invalid-parameter error the association gives.
	pub fn new(resource: impl Into<S>, submitter: &crate::Submitter) -> Facade<S> {
		Self {
			handle: Handle::new(resource.into()),
			submitter: submitter.0.clone(),
		}
	}

	/// Wraps a `handle` the caller already holds, its operations going to
	/// `submitter`; the facade shares the handle rather than taking it over.
	pub fn bind(handle: Handle<S>, submitter: &crate::Submitter) -> Facade<S> {
		Self {
			handle,
			submitter: submitter.0.clone(),
		}
	}

	pub(super) fn submitter(&self) -> &Submitter {
		&self.submitter
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
				submitter: self.submitter,
			}),
		}
	}
}

impl<S> Clone for Facade<S> {
	fn clone(&self) -> Facade<S> {
		Self {
			handle: self.handle.clone(),
			submitter: self.submitter.clone(),
		}
	}
}

pub type File = Facade<fs::File>;

impl<S: AsDescriptor + Send + Sync + 'static> Facade<S> {
	/// Reads the whole of `mem` at `fdoff`, in one call: a short count is what
	/// the call moved, not an error. [`read_exact_at`](Self::read_exact_at)
	/// loops.
	#[track_caller]
	pub fn read_at<'a>(&self, mem: impl IntoMemory<'a>, fdoff: u64) -> Transfer<'a, S> {
		let mem = mem.into_memory();
		let extents = whole(&*mem.memory);
		Transfer::new(self, Location::caller(), Kind::Read { fdoff }, mem, extents)
	}

	#[track_caller]
	pub fn read_vectored_at<'a>(
		&self,
		mem: impl IntoMemory<'a>,
		ranges: impl IntoIterator<Item = Extent>,
		fdoff: u64,
	) -> Transfer<'a, S> {
		let mem = mem.into_memory();
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Location::caller(), Kind::Read { fdoff }, mem, extents)
	}

	/// Reads the whole of `mem` at `fdoff`, over as many calls as it takes to
	/// fill it.
	///
	/// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) when the file ends
	/// first.
	#[track_caller]
	pub fn read_exact_at<'a>(&self, mem: impl IntoMemory<'a>, fdoff: u64) -> Transfer<'a, S> {
		let mem = mem.into_memory();
		let extents = whole(&*mem.memory);
		Transfer::new(
			self,
			Location::caller(),
			Kind::ReadAll { fdoff },
			mem,
			extents,
		)
	}

	#[track_caller]
	pub fn read_exact_vectored_at<'a>(
		&self,
		mem: impl IntoMemory<'a>,
		ranges: impl IntoIterator<Item = Extent>,
		fdoff: u64,
	) -> Transfer<'a, S> {
		let mem = mem.into_memory();
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(
			self,
			Location::caller(),
			Kind::ReadAll { fdoff },
			mem,
			extents,
		)
	}

	/// Writes the whole of `mem` at `fdoff`, in one call: a short count is what
	/// the call moved, not an error. [`write_all_at`](Self::write_all_at)
	/// loops.
	#[track_caller]
	pub fn write_at<'a>(&self, mem: impl IntoMemory<'a>, fdoff: u64) -> Transfer<'a, S> {
		let mem = mem.into_memory();
		let extents = whole(&*mem.memory);
		Transfer::new(
			self,
			Location::caller(),
			Kind::Write { fdoff },
			mem,
			extents,
		)
	}

	#[track_caller]
	pub fn write_vectored_at<'a>(
		&self,
		mem: impl IntoMemory<'a>,
		ranges: impl IntoIterator<Item = Extent>,
		fdoff: u64,
	) -> Transfer<'a, S> {
		let mem = mem.into_memory();
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(
			self,
			Location::caller(),
			Kind::Write { fdoff },
			mem,
			extents,
		)
	}

	/// Writes the whole of `mem` at `fdoff`, over as many calls as it takes to
	/// write it all.
	///
	/// [`WriteZero`](io::ErrorKind::WriteZero) when a call writes nothing.
	#[track_caller]
	pub fn write_all_at<'a>(&self, mem: impl IntoMemory<'a>, fdoff: u64) -> Transfer<'a, S> {
		let mem = mem.into_memory();
		let extents = whole(&*mem.memory);
		Transfer::new(
			self,
			Location::caller(),
			Kind::WriteAll { fdoff },
			mem,
			extents,
		)
	}

	#[track_caller]
	pub fn write_all_vectored_at<'a>(
		&self,
		mem: impl IntoMemory<'a>,
		ranges: impl IntoIterator<Item = Extent>,
		fdoff: u64,
	) -> Transfer<'a, S> {
		let mem = mem.into_memory();
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(
			self,
			Location::caller(),
			Kind::WriteAll { fdoff },
			mem,
			extents,
		)
	}

	#[track_caller]
	pub fn sync(&self) -> Op<S> {
		Op::new(self, Location::caller(), Submitter::fsync_at)
	}

	/// Reports the resource's pending deferred write-back errors — `ENOSPC`,
	/// `EIO` and the like — without forcing anything to disk.
	///
	/// This is not [`Write::flush`](std::io::Write::flush): it names no buffer
	/// of the caller's, and it does not make the data durable — [`sync`](Self::sync)
	/// is what does that. What it reports is the error the filesystem had
	/// deferred and would otherwise surface at the resource's next close.
	#[track_caller]
	pub fn flush(&self) -> Op<S> {
		Op::new(self, Location::caller(), Submitter::flush_at)
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Facade<S> {
	#[track_caller]
	pub fn wait_readable(&self) -> Op<S> {
		Op::new(self, Location::caller(), Submitter::wait_readable_at)
	}

	#[track_caller]
	pub fn wait_writable(&self) -> Op<S> {
		Op::new(self, Location::caller(), Submitter::wait_writable_at)
	}
}
