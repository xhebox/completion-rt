//! File operations: the read, write, and sync.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::flow::{Kind, Op, Transfer, whole};
use crate::{AsDescriptor, Extent, Extents, Handle, Memory, Submitter};

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

/// A resource an operation goes through, and the submitter it goes to.
///
/// [`File`] is this over a file, and a [`SocketHandle`](crate::SocketHandle) —
/// a stream in particular — is this over a socket. The resource is named
/// through a [`Handle`], so [`take`](Facade::take) resolves only once every
/// other clone has let go of it.
pub struct Facade<S> {
	handle: Handle<S>,
	submitter: Submitter,
}

impl<S> Facade<S> {
	/// Wraps `resource`, its operations going to `submitter`.
	pub fn new(resource: impl Into<S>, submitter: &Submitter) -> Facade<S> {
		Self {
			handle: Handle::new(resource.into()),
			submitter: submitter.clone(),
		}
	}

	pub(super) fn submitter(&self) -> &Submitter {
		&self.submitter
	}

	/// The handle the resource is named through. Operations submitted by hand
	/// take it, so they are waited for by [`take`](Facade::take) like any
	/// other.
	pub(super) fn handle(&self) -> &Handle<S> {
		&self.handle
	}

	/// Waits until every other clone of the facade has let go, then gives the
	/// resource back; `None` if another [`take`](Facade::take) is already
	/// waiting for it.
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

/// An async file facade.
pub type File = Facade<fs::File>;

impl Facade<fs::File> {
	/// Reads the whole of `mem` at `fdoff`, in one call: a short count is what
	/// the call moved, not an error. [`read_exact_at`](Self::read_exact_at)
	/// loops.
	pub fn read_at(&self, mem: Arc<dyn Memory>, fdoff: u64) -> Transfer<fs::File> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::Read { fdoff }, mem, extents)
	}

	/// Reads the named ranges of `mem` at `fdoff`, in one call.
	pub fn read_vectored_at(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
		fdoff: u64,
	) -> Transfer<fs::File> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::Read { fdoff }, mem, extents)
	}

	/// Reads the whole of `mem` at `fdoff`, over as many calls as it takes to
	/// fill it.
	///
	/// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) when the file ends
	/// first.
	pub fn read_exact_at(&self, mem: Arc<dyn Memory>, fdoff: u64) -> Transfer<fs::File> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::ReadAll { fdoff }, mem, extents)
	}

	/// Reads the named ranges of `mem` at `fdoff`, over as many calls as it
	/// takes to fill them.
	pub fn read_exact_vectored_at(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
		fdoff: u64,
	) -> Transfer<fs::File> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::ReadAll { fdoff }, mem, extents)
	}

	/// Writes the whole of `mem` at `fdoff`, in one call: a short count is what
	/// the call moved, not an error. [`write_all_at`](Self::write_all_at)
	/// loops.
	pub fn write_at(&self, mem: Arc<dyn Memory>, fdoff: u64) -> Transfer<fs::File> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::Write { fdoff }, mem, extents)
	}

	/// Writes the named ranges of `mem` at `fdoff`, in one call.
	pub fn write_vectored_at(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
		fdoff: u64,
	) -> Transfer<fs::File> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::Write { fdoff }, mem, extents)
	}

	/// Writes the whole of `mem` at `fdoff`, over as many calls as it takes to
	/// write it all.
	///
	/// [`WriteZero`](io::ErrorKind::WriteZero) when a call writes nothing.
	pub fn write_all_at(&self, mem: Arc<dyn Memory>, fdoff: u64) -> Transfer<fs::File> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::WriteAll { fdoff }, mem, extents)
	}

	/// Writes the named ranges of `mem` at `fdoff`, over as many calls as it
	/// takes to write them all.
	pub fn write_all_vectored_at(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
		fdoff: u64,
	) -> Transfer<fs::File> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::WriteAll { fdoff }, mem, extents)
	}

	/// Makes everything written to the file durable.
	pub fn sync(&self) -> Op<fs::File> {
		Op::new(self, Submitter::fsync)
	}

	/// Reports the file's pending deferred write-back errors — `ENOSPC`, `EIO`
	/// and the like — without forcing anything to disk.
	///
	/// This is not [`Write::flush`](std::io::Write::flush): it names no buffer
	/// of the caller's, and it does not make the data durable — [`sync`](Self::sync)
	/// is what does that. What it reports is the error the filesystem had
	/// deferred and would otherwise surface at the file's next close.
	pub fn flush(&self) -> Op<fs::File> {
		Op::new(self, Submitter::flush)
	}
}

impl<S: AsDescriptor + Send + Sync + 'static> Facade<S> {
	pub fn wait_readable(&self) -> Op<S> {
		Op::new(self, Submitter::wait_readable)
	}

	pub fn wait_writable(&self) -> Op<S> {
		Op::new(self, Submitter::wait_writable)
	}
}
