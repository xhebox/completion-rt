use std::fs;
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;

use completion_backend::{AsDescriptor, BorrowedDescriptor, Error, IoVec, Op, Response};

use crate::memory::AsIoVec;
use crate::{Result, submit};

/// Submits the single transfer `op` builds over `buffer`'s range.
pub(crate) async fn transfer<D: AsDescriptor + 'static, M: AsIoVec>(
	fd: D,
	buffer: M,
	op: impl FnOnce(IoVec) -> Op + Send,
) -> Result<usize> {
	let op = op(buffer.as_iovec());
	let (response, _fd, _memory) = submit(fd, op, buffer)?.await;
	match response? {
		Response::Count(bytes) => Ok(bytes),
		other => unreachable!("a transfer reports a count, not {other:?}"),
	}
}

async fn repeat<D: AsDescriptor + 'static, M: AsIoVec>(
	fd: D,
	buffer: M,
	short: fn() -> io::Error,
	mut make_op: impl FnMut(usize, IoVec) -> Op + Send,
) -> Result<()> {
	// The loop owns the one descriptor and buffer handle: each round moves them
	// into the round's entry, and the completion hands them back for the next.
	// A round dropped in flight keeps them in its entry until the kernel finishes.
	let mut fd = fd;
	let mut buffer = buffer;
	let whole = buffer.as_iovec();
	let mut done = 0;
	while done < whole.len {
		// SAFETY: `done < whole.len`, so the rest of the range is in the buffer.
		let remaining = IoVec {
			ptr: unsafe { whole.ptr.add(done) },
			len: whole.len - done,
		};
		let slot = submit(fd, make_op(done, remaining), buffer)?;
		let (response, returned_fd, returned_buf) = slot.await;
		match response? {
			Response::Count(0) => return Err(Error::from(short())),
			Response::Count(bytes) => {
				fd = returned_fd;
				buffer = returned_buf;
				done += bytes;
			}
			other => unreachable!("a transfer reports a count, not {other:?}"),
		}
	}
	Ok(())
}

pub(crate) async fn fill<D: AsDescriptor + 'static, M: AsIoVec>(
	fd: D,
	buffer: M,
	op: impl FnMut(usize, IoVec) -> Op + Send,
) -> Result<()> {
	repeat(
		fd,
		buffer,
		|| {
			io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"the source ended before the memory was full",
			)
		},
		op,
	)
	.await
}

pub(crate) async fn drain<D: AsDescriptor + 'static, M: AsIoVec>(
	fd: D,
	buffer: M,
	op: impl FnMut(usize, IoVec) -> Op + Send,
) -> Result<()> {
	repeat(
		fd,
		buffer,
		|| io::Error::new(io::ErrorKind::WriteZero, "a call moved nothing"),
		op,
	)
	.await
}

/// Builder for opening files with optional direct I/O (`O_DIRECT` / `F_NOCACHE` /
/// `FILE_FLAG_NO_BUFFERING`).
///
/// On Windows, [`OpenOptions::open`] always sets `FILE_FLAG_OVERLAPPED`, which
/// IOCP requires before a [`std::fs::File`] can be handed to [`File`]; the
/// portable backend takes either mode.
pub struct OpenOptions {
	inner: fs::OpenOptions,
	direct: bool,
}

impl Default for OpenOptions {
	fn default() -> OpenOptions {
		OpenOptions::new()
	}
}

impl OpenOptions {
	/// Creates a blank set of options.
	pub fn new() -> OpenOptions {
		OpenOptions {
			inner: fs::OpenOptions::new(),
			direct: false,
		}
	}

	/// Sets the option for read access.
	pub fn read(mut self, read: bool) -> OpenOptions {
		self.inner.read(read);
		self
	}

	/// Sets the option for write access.
	pub fn write(mut self, write: bool) -> OpenOptions {
		self.inner.write(write);
		self
	}

	/// Sets the option for append mode.
	pub fn append(mut self, append: bool) -> OpenOptions {
		self.inner.append(append);
		self
	}

	/// Sets the option for truncating a previous file.
	pub fn truncate(mut self, truncate: bool) -> OpenOptions {
		self.inner.truncate(truncate);
		self
	}

	/// Sets the option to create a new file if it does not exist.
	pub fn create(mut self, create: bool) -> OpenOptions {
		self.inner.create(create);
		self
	}

	/// Sets the option to create a new file, failing if it already exists.
	pub fn create_new(mut self, create_new: bool) -> OpenOptions {
		self.inner.create_new(create_new);
		self
	}

	/// Enables direct I/O, bypassing the OS page cache (`O_DIRECT` on Linux/Android,
	/// `F_NOCACHE` on Apple platforms, `FILE_FLAG_NO_BUFFERING` on Windows).
	pub fn direct(mut self) -> OpenOptions {
		self.direct = true;
		self
	}

	/// Opens a file at `path` with the configured options.
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
			rustix::fs::fcntl_nocache(&file, true)?;
		}
		Ok(file)
	}
}

/// Handle for positional file I/O.
///
/// Every transfer takes an explicit byte offset and leaves the OS file cursor
/// unchanged, so cloned handles can run concurrent transfers on the same file.
#[derive(Clone)]
pub struct File(Arc<fs::File>);

impl File {
	/// Wraps `file`.
	///
	/// On Windows with the IOCP backend, `file` must be opened with
	/// `FILE_FLAG_OVERLAPPED` (see [`OpenOptions`]).
	#[fastrace::trace]
	pub fn new(file: impl Into<fs::File>) -> File {
		File(Arc::new(file.into()))
	}

	/// Unwraps the underlying [`std::fs::File`] if no clones of this handle and
	/// no in-flight operations remain.
	pub fn try_take(self) -> Option<fs::File> {
		Arc::into_inner(self.0)
	}

	/// Performs a single read into `buf` at byte offset `offset`.
	pub fn read_at<M: AsIoVec>(
		&self,
		buf: M,
		offset: u64,
	) -> impl Future<Output = Result<usize>> + 'static {
		transfer(self.clone(), buf, move |iovec| Op::Read {
			offset: Some(offset),
			buf: iovec,
		})
	}

	/// Reads into `buf` starting at `offset` until the buffer is completely filled.
	pub fn read_exact_at<M: AsIoVec>(
		&self,
		buf: M,
		offset: u64,
	) -> impl Future<Output = Result<()>> + 'static {
		fill(self.clone(), buf, move |done, iovec| Op::Read {
			offset: Some(offset + done as u64),
			buf: iovec,
		})
	}

	/// Performs a single write from `buf` at byte offset `offset`.
	pub fn write_at<M: AsIoVec>(
		&self,
		buf: M,
		offset: u64,
	) -> impl Future<Output = Result<usize>> + 'static {
		transfer(self.clone(), buf, move |iovec| Op::Write {
			offset: Some(offset),
			buf: iovec,
		})
	}

	/// Writes all bytes in `buf` starting at `offset`.
	pub fn write_all_at<M: AsIoVec>(
		&self,
		buf: M,
		offset: u64,
	) -> impl Future<Output = Result<()>> + 'static {
		drain(self.clone(), buf, move |done, iovec| Op::Write {
			offset: Some(offset + done as u64),
			buf: iovec,
		})
	}

	/// Flushes all modified file data and metadata to disk (`fsync` /
	/// `FlushFileBuffers`).
	pub fn sync(&self) -> impl Future<Output = Result<()>> + use<> {
		let file = self.clone();
		async move { submit(file, Op::Fsync, ())?.await.0.map(|_response| ()) }
	}

	/// Checks for deferred write-back errors (such as `ENOSPC` or `EIO`) without
	/// forcing data to disk.
	pub fn flush(&self) -> impl Future<Output = Result<()>> + use<> {
		let file = self.clone();
		async move { submit(file, Op::Flush, ())?.await.0.map(|_response| ()) }
	}

	/// Preallocates disk space for `len` bytes starting at `offset`.
	pub fn allocate(&self, offset: u64, len: u64) -> impl Future<Output = Result<()>> + use<> {
		let file = self.clone();
		async move {
			submit(file, Op::Allocate { offset, len }, ())?
				.await
				.0
				.map(|_response| ())
		}
	}

	/// Deallocates storage for `len` bytes at `offset` and zeroes the range while
	/// keeping the file's length unchanged (hole punching).
	pub fn punch(&self, offset: u64, len: u64) -> impl Future<Output = Result<()>> + use<> {
		let file = self.clone();
		async move {
			submit(file, Op::Punch { offset, len }, ())?
				.await
				.0
				.map(|_response| ())
		}
	}
}

impl AsDescriptor for File {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		self.0.as_descriptor()
	}
}

#[cfg(test)]
mod tests {
	use assert2::assert;

	use super::OpenOptions;

	#[test]
	fn open_options_builder_records_direct_io_flag() {
		assert!(!OpenOptions::default().direct);
		assert!(
			OpenOptions::new()
				.read(true)
				.write(true)
				.append(false)
				.truncate(false)
				.create(true)
				.create_new(false)
				.direct()
				.direct
		);
	}
}
