//! File and positional descriptor operations.

use std::fs;
use std::io;
use std::path::Path;

use completion_backend::Span;

use crate::op::{Action, Facade, Kind, Op, Transfer};

/// Builder for opening files with optional direct I/O (`O_DIRECT` / `F_NOCACHE` /
/// `FILE_FLAG_NO_BUFFERING`).
///
/// On Windows, [`OpenOptions::open`] always sets `FILE_FLAG_OVERLAPPED`, which is
/// required before wrapping a [`std::fs::File`] in [`Facade`].
pub struct OpenOptions {
	inner: std::fs::OpenOptions,
	direct: bool,
}

impl OpenOptions {
	/// Creates a blank set of options.
	pub fn new() -> OpenOptions {
		OpenOptions {
			inner: std::fs::OpenOptions::new(),
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

/// Type alias for a [`Facade`] wrapping [`std::fs::File`].
pub type File = Facade<fs::File>;

macro_rules! positional {
	($($resource:ty),* $(,)?) => {
		$(
			impl Facade<$resource> {
				/// Performs a single read into `span` at byte offset `fdoff`.
				///
				/// May transfer fewer bytes than `span.len()`; use [`read_exact_at`](Self::read_exact_at)
				/// to read until `span` is full.
				pub fn read_at(&self, span: Span, fdoff: u64) -> Transfer {
					Transfer::new(self, Kind::Read { fdoff }, span)
				}

				/// Reads into `span` starting at `fdoff` until `span` is completely filled.
				///
				/// Fails with [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) if EOF is
				/// reached before `span` is full.
				pub fn read_exact_at(&self, span: Span, fdoff: u64) -> Transfer {
					Transfer::new(self, Kind::ReadAll { fdoff }, span)
				}

				/// Performs a single write from `span` at byte offset `fdoff`.
				///
				/// May transfer fewer bytes than `span.len()`; use [`write_all_at`](Self::write_all_at)
				/// to write the entire span.
				pub fn write_at(&self, span: Span, fdoff: u64) -> Transfer {
					Transfer::new(self, Kind::Write { fdoff }, span)
				}

				/// Writes all bytes in `span` starting at `fdoff`.
				///
				/// Fails with [`WriteZero`](io::ErrorKind::WriteZero) if a single write
				/// call transfers zero bytes.
				pub fn write_all_at(&self, span: Span, fdoff: u64) -> Transfer {
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
	completion_backend::OwnedDescriptor,
);

impl Facade<fs::File> {
	/// Flushes all modified file data and metadata to disk (`fsync` / `FlushFileBuffers`).
	pub fn sync(&self) -> Op {
		Op::new(self, Action::Fsync)
	}

	/// Checks for deferred write-back errors (such as `ENOSPC` or `EIO`) without
	/// forcing data to disk.
	///
	/// Unlike [`sync`](Self::sync), this does not guarantee durability; it surfaces
	/// filesystem write-back errors that would otherwise be reported at close.
	pub fn flush(&self) -> Op {
		Op::new(self, Action::Flush)
	}

	/// Preallocates disk space for `len` bytes starting at `offset`.
	///
	/// Extends the file with zeros if `offset + len` exceeds the current file length;
	/// never shrinks the file.
	pub fn allocate(&self, offset: u64, len: u64) -> Op {
		Op::new(self, Action::Allocate { offset, len })
	}

	/// Deallocates storage for `len` bytes at `offset` and zeroes the range while
	/// keeping the file's length unchanged (hole punching).
	///
	/// On Windows, storage is only reclaimed if the file is marked sparse.
	pub fn punch(&self, offset: u64, len: u64) -> Op {
		Op::new(self, Action::Punch { offset, len })
	}
}
