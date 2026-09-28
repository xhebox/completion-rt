//! Windows IOCP backend: one completion port, one OVERLAPPED per operation.
//!
//! File handles must be opened with `FILE_FLAG_OVERLAPPED`, which
//! `OpenOptions` sets on Windows; this backend associates each handle with the
//! port on first use and issues `ReadFile`/`WriteFile` with a per-operation
//! OVERLAPPED. Operations without an overlapped form (`FlushFileBuffers`) run
//! inline and post their result through the port.
//!
//! Only the file operations are implemented: a completion port has no socket
//! receive/send and no readiness wait, and each of those methods says so rather
//! than pretending.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use windows_sys::Win32::Foundation::{GetLastError, HANDLE, WAIT_TIMEOUT};
use windows_sys::Win32::Storage::FileSystem::{FlushFileBuffers, ReadFile, WriteFile};
use windows_sys::Win32::System::IO::{
	CancelIoEx, CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_ENTRY,
	PostQueuedCompletionStatus,
};

use crate::core::Response;
use crate::desc::RawHandle;
use crate::{Extents, Memory, OwnedDescriptor};

use super::Backend;
use crate::backend::{Config, PollMode};
use crate::event::Event;

/// `ERROR_IO_PENDING`: the operation was accepted and will complete later.
const ERROR_IO_PENDING: u32 = 997;
/// `ERROR_OPERATION_ABORTED`: what `CancelIoEx` makes a stopped operation
/// report.
const ERROR_OPERATION_ABORTED: u32 = 995;
/// An NTSTATUS's low word is its facility-specific code; close enough for an
/// experimental crate, and the common cases (access denied, invalid handle,
/// disk full) map to recognizable errno values.
const fn ntstatus_errno(status: usize) -> i32 {
	(status & 0xFFFF) as i32
}

pub struct Iocp {
	/// The port that carries this backend's completions. The event made it,
	/// and only this backend drains it.
	event: Event,
	active: HashMap<u64, Active>,
	/// Completions the port reported and nobody took yet.
	ready: VecDeque<(u64, io::Result<Response>)>,
}

/// One in-flight operation: the OVERLAPPED the kernel writes into, plus the
/// memory keeping the buffer it names alive until the completion.
struct Active {
	ov: Box<Overlapped>,
	/// Kept only to own the buffer the OVERLAPPED names.
	_memory: Option<Arc<dyn Memory>>,
}

/// The OVERLAPPED plus the token that names its operation. `inner` is the
/// first field so a pointer to the box is a valid `*mut OVERLAPPED`.
#[repr(C)]
struct Overlapped {
	inner: OVERLAPPED,
	token: u64,
	/// A Win32 error from an immediate submit-time failure, or 0.
	submit_error: i32,
	/// The handle the operation was issued on, for `CancelIoEx`.
	handle: HANDLE,
}

impl Iocp {
	pub fn new(config: &Config) -> io::Result<(Iocp, Event)> {
		if config.wakeup == PollMode::Fd {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"an IOCP port is consumed by GetQueuedCompletionStatusEx, not waited on",
			));
		}
		if config.iopoll || config.sqpoll {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"a completion port has no IOPOLL or SQPOLL mode",
			));
		}
		let event = Event::new()?;
		Ok((
			Iocp {
				event: event.clone(),
				active: HashMap::new(),
				ready: VecDeque::new(),
			},
			event,
		))
	}

	fn post(&mut self, token: u64) {
		let Some(active) = self.active.get(&token) else {
			return;
		};
		let ov_ptr = &*active.ov as *const Overlapped as *const OVERLAPPED as *mut OVERLAPPED;
		// SAFETY: `ov_ptr` names the box in `active`, which lives until the
		// completion is collected in `poll`.
		unsafe { PostQueuedCompletionStatus(self.event.handle().as_platform(), 0, 0, ov_ptr) };
	}

	/// Associates `fd` with the port so its completions land here.
	/// Re-associating an already-associated handle with the same port is a
	/// no-op returning the existing port.
	fn associate(&self, fd: HANDLE) -> io::Result<()> {
		let port = unsafe { CreateIoCompletionPort(fd, self.event.handle().as_platform(), 0, 0) };
		if port.is_null() {
			return Err(io::Error::last_os_error());
		}
		Ok(())
	}

	/// Issues one overlapped read or write: a per-operation OVERLAPPED, with
	/// the outcome posted when the kernel reports or the call fails outright.
	fn issue(
		&mut self,
		id: u64,
		fd: HANDLE,
		read: bool,
		buf: *mut u8,
		len: usize,
		fdoff: u64,
		memory: Arc<dyn Memory>,
	) -> io::Result<()> {
		let mut ov = Box::new(Overlapped {
			inner: OVERLAPPED {
				Internal: 0,
				InternalHigh: 0,
				Anonymous: windows_sys::Win32::System::IO::OVERLAPPED_0 {
					Anonymous: windows_sys::Win32::System::IO::OVERLAPPED_0_0 {
						Offset: fdoff as u32,
						OffsetHigh: (fdoff >> 32) as u32,
					},
				},
				hEvent: std::ptr::null_mut(),
			},
			token: id,
			submit_error: 0,
			handle: fd,
		});
		let ov_ptr = &mut *ov as *mut Overlapped as *mut OVERLAPPED;
		self.active.insert(
			id,
			Active {
				ov,
				_memory: Some(memory),
			},
		);
		// SAFETY: standard Win32 calls; `ov_ptr` names the box in `active`,
		// alive until the completion is collected.
		let ok = unsafe {
			if read {
				ReadFile(fd, buf.cast(), len as u32, std::ptr::null_mut(), ov_ptr)
			} else {
				WriteFile(fd, buf.cast(), len as u32, std::ptr::null_mut(), ov_ptr)
			}
		};
		if ok == 0 {
			let error = unsafe { GetLastError() };
			if error != ERROR_IO_PENDING {
				self.active.get_mut(&id).unwrap().ov.submit_error = error as i32;
				self.post(id);
			}
		} else {
			// Completed inline; InternalHigh holds the byte count.
			self.post(id);
		}
		Ok(())
	}

	/// The one buffer an overlapped call names, and its offset.
	fn buffer(memory: &Arc<dyn Memory>, extents: &Extents) -> io::Result<(*mut u8, usize)> {
		let buffers = crate::memory::spans(memory, extents)?;
		let [buf] = buffers.as_slice() else {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"iocp backend needs exactly one buffer per operation",
			));
		};
		Ok((buf.as_ptr(), buf.len()))
	}

	fn unsupported() -> io::Error {
		io::Error::new(
			io::ErrorKind::Unsupported,
			"this operation is not implemented on the IOCP backend",
		)
	}
}

impl Backend for Iocp {
	fn read(
		&mut self,
		id: u64,
		fd: RawHandle,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		let handle: HANDLE = fd.as_platform();
		self.associate(handle)?;
		let (buf, len) = Self::buffer(&memory, &extents)?;
		self.issue(id, handle, true, buf, len, fdoff, memory)
	}

	fn write(
		&mut self,
		id: u64,
		fd: RawHandle,
		fdoff: u64,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		let handle: HANDLE = fd.as_platform();
		self.associate(handle)?;
		let (buf, len) = Self::buffer(&memory, &extents)?;
		self.issue(id, handle, false, buf, len, fdoff, memory)
	}

	fn fsync(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		let handle: HANDLE = fd.as_platform();
		self.associate(handle)?;
		// No overlapped form: sync inline and post the outcome.
		let ok = unsafe { FlushFileBuffers(handle) };
		let submit_error = if ok == 0 {
			(unsafe { GetLastError() }) as i32
		} else {
			0
		};
		self.active.insert(
			id,
			Active {
				ov: Box::new(Overlapped {
					inner: zeroed(),
					token: id,
					submit_error,
					handle,
				}),
				_memory: None,
			},
		);
		self.post(id);
		Ok(())
	}

	fn flush(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		// Windows reports no deferred write-back error when a handle is closed:
		// there is no close-time error to surface. `FlushFileBuffers`
		// approximates a fsync, not a flush, and is too costly to run on every
		// close — so the operation succeeds without touching the disk. Should a
		// mechanism appear, only this backend changes.
		let handle: HANDLE = fd.as_platform();
		self.active.insert(
			id,
			Active {
				ov: Box::new(Overlapped {
					inner: zeroed(),
					token: id,
					submit_error: 0,
					handle,
				}),
				_memory: None,
			},
		);
		self.post(id);
		Ok(())
	}

	fn recv(
		&mut self,
		_id: u64,
		_fd: RawHandle,
		_memory: Arc<dyn Memory>,
		_extents: Extents,
	) -> io::Result<()> {
		Err(Self::unsupported())
	}

	fn recv_with_fds(
		&mut self,
		_id: u64,
		_fd: RawHandle,
		_memory: Arc<dyn Memory>,
		_extents: Extents,
	) -> io::Result<()> {
		Err(Self::unsupported())
	}

	fn send(
		&mut self,
		_id: u64,
		_fd: RawHandle,
		_memory: Arc<dyn Memory>,
		_extents: Extents,
	) -> io::Result<()> {
		Err(Self::unsupported())
	}

	fn send_with_fds(
		&mut self,
		_id: u64,
		_fd: RawHandle,
		_memory: Arc<dyn Memory>,
		_extents: Extents,
		_fds: Vec<OwnedDescriptor>,
	) -> io::Result<()> {
		Err(Self::unsupported())
	}

	fn accept(&mut self, _id: u64, _fd: RawHandle) -> io::Result<()> {
		Err(Self::unsupported())
	}

	fn readable(&mut self, _id: u64, _fd: RawHandle) -> io::Result<()> {
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a completion port has no readiness wait",
		))
	}

	fn writable(&mut self, _id: u64, _fd: RawHandle) -> io::Result<()> {
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a completion port has no readiness wait",
		))
	}

	fn cancel(&mut self, id: u64) {
		let Some(active) = self.active.get(&id) else {
			return;
		};
		let pointer = &*active.ov as *const Overlapped as *const OVERLAPPED;
		// SAFETY: the box stays in `active` until its completion is collected,
		// which is after the kernel has stopped touching the OVERLAPPED.
		unsafe { CancelIoEx(active.ov.handle, pointer) };
	}

	fn poll(&mut self, timeout: Option<Duration>) -> io::Result<u64> {
		if let Some((id, _)) = self.ready.front() {
			return Ok(*id);
		}
		let timeout_ms = match timeout {
			None => u32::MAX,
			Some(duration) => duration.as_millis().min(u32::MAX as u128 - 1) as u32,
		};
		let mut entries: [OVERLAPPED_ENTRY; 64] = unsafe { std::mem::zeroed() };
		let mut count: u32 = 0;
		// SAFETY: standard Win32 call; entries and count point at writable
		// memory of the right size.
		let ok = unsafe {
			GetQueuedCompletionStatusEx(
				self.event.handle().as_platform(),
				entries.as_mut_ptr(),
				entries.len() as u32,
				&mut count,
				timeout_ms,
				0,
			)
		};
		if ok == 0 {
			let error = io::Error::last_os_error();
			if error.raw_os_error() == Some(WAIT_TIMEOUT as i32) {
				return Err(io::Error::from(io::ErrorKind::WouldBlock));
			}
			return Err(error);
		}
		for entry in entries[..count as usize].iter() {
			// A wake carries no OVERLAPPED; nothing waits for it.
			if entry.lpOverlapped.is_null() {
				continue;
			}
			// SAFETY: the pointer names a box we inserted into `active` at
			// submit time and that is alive until this collection.
			let ov = entry.lpOverlapped as *mut Overlapped;
			let (id, result) = unsafe {
				let id = (*ov).token;
				let result = if (*ov).submit_error != 0 {
					Err(io::Error::from_raw_os_error((*ov).submit_error))
				} else if (*ov).inner.Internal == 0 {
					Ok((*ov).inner.InternalHigh as usize)
				} else {
					Err(io::Error::from_raw_os_error(ntstatus_errno(
						(*ov).inner.Internal,
					)))
				};
				// Drops the OVERLAPPED box and the memory holding the buffer
				// it named.
				self.active.remove(&id);
				(id, result)
			};
			self.ready.push_back((id, result.map(Response::Count)));
		}
		match self.ready.front() {
			Some((id, _)) => Ok(*id),
			None => Err(io::Error::from(io::ErrorKind::WouldBlock)),
		}
	}

	fn take(&mut self, id: u64) -> io::Result<Response> {
		match self.ready.pop_front() {
			Some((found, response)) if found == id => response,
			Some((found, response)) => {
				self.ready.push_front((found, response));
				Err(io::Error::other(format!(
					"take({id}) found a completion for {found}"
				)))
			}
			None => Err(io::Error::new(
				io::ErrorKind::NotFound,
				"no completion to take",
			)),
		}
	}

	fn poll_fd(&self) -> io::Result<Event> {
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"an IOCP port is consumed by GetQueuedCompletionStatusEx, not waited on",
		))
	}
}

/// Whether `error` is a cancellation. `CancelIoEx` reports the aborted operation
/// as `ERROR_OPERATION_ABORTED`.
pub(super) fn cancelled(error: &io::Error) -> bool {
	error.raw_os_error() == Some(ERROR_OPERATION_ABORTED as i32)
}

fn zeroed() -> OVERLAPPED {
	unsafe { std::mem::zeroed() }
}

// SAFETY: the pointer fields are opaque kernel handles; the box owns them
// and the completion collection (`poll`) is the only place they are touched.
unsafe impl Send for Overlapped {}
unsafe impl Sync for Overlapped {}
