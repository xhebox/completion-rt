//! Windows IOCP backend; file handles require `FILE_FLAG_OVERLAPPED`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use windows_sys::Win32::Foundation::{ERROR_INVALID_PARAMETER, GetLastError, HANDLE, WAIT_TIMEOUT};
use windows_sys::Win32::Networking::WinSock::{
	AcceptEx, INVALID_SOCKET, IPPROTO_TCP, SO_UPDATE_ACCEPT_CONTEXT, SOCK_STREAM, SOCKADDR,
	SOCKADDR_STORAGE, SOCKET, SOL_SOCKET, WSA_FLAG_OVERLAPPED, WSABUF, WSAGetLastError, WSARecv,
	WSASend, WSASocketW, closesocket, getsockname, setsockopt,
};
use windows_sys::Win32::Storage::FileSystem::{
	FILE_ALLOCATION_INFO, FILE_END_OF_FILE_INFO, FileAllocationInfo, FileEndOfFileInfo,
	FlushFileBuffers, ReadFile, SetFileInformationByHandle, WriteFile,
};
use windows_sys::Win32::System::IO::{
	CancelIoEx, CreateIoCompletionPort, DeviceIoControl, GetQueuedCompletionStatusEx, OVERLAPPED,
	OVERLAPPED_ENTRY, PostQueuedCompletionStatus,
};
use windows_sys::Win32::System::Ioctl::{FILE_ZERO_DATA_INFORMATION, FSCTL_SET_ZERO_DATA};

use crate::core::Response;
use crate::desc::{RawHandle, from_raw_socket};
use crate::memory::Extents;
use crate::{Memory, OwnedDescriptor};

use super::Backend;
use crate::backend::Config;
use crate::event::Event;
use crate::park::Park;

/// `ERROR_IO_PENDING`, the same number as `WSA_IO_PENDING`: the operation was
/// accepted and will complete later.
const IO_PENDING: u32 = 997;
/// `ERROR_OPERATION_ABORTED`: what `CancelIoEx` makes a stopped operation
/// report.
const ERROR_OPERATION_ABORTED: u32 = 995;
/// `STATUS_END_OF_FILE`: the status a read beginning at or past the end of a
/// file completes with. The status block carries the NTSTATUS itself, not the
/// `ERROR_HANDLE_EOF` the same condition reports from a synchronous call.
const STATUS_END_OF_FILE: usize = 0xC000_0011;
/// The room `AcceptEx` needs beyond the two addresses it writes; the kernel
/// takes `sizeof(SOCKADDR) + 16` per address, and `SOCKADDR_STORAGE` is the
/// widest `SOCKADDR`.
const ADDRESS_LEN: u32 = (size_of::<SOCKADDR_STORAGE>() + 16) as u32;
/// The current NTSTATUS conversion uses only the low word; it is not a general NTSTATUS-to-Win32 mapping.
const fn ntstatus_errno(status: usize) -> i32 {
	(status & 0xFFFF) as i32
}

pub(crate) struct Iocp {
	event: Event,
	active: HashMap<u64, Active>,
	/// Handles already associated with this port. A handle stays associated
	/// until it is closed, and a second `CreateIoCompletionPort` on it fails
	/// with `ERROR_INVALID_PARAMETER`, so each handle is associated once.
	associated: HashSet<usize>,
	ready: VecDeque<(u64, io::Result<Response>)>,
}

struct Active {
	ov: Box<Overlapped>,
	_memory: Option<Arc<dyn Memory>>,
	/// An `AcceptEx` in flight. The socket the kernel fills in and the address
	/// buffer it writes are both alive until the completion lands.
	accept: Option<Pending>,
}

/// An `AcceptEx` the kernel has not completed yet.
struct Pending {
	socket: Option<SOCKET>,
	listener: SOCKET,
	_addresses: Box<[u8]>,
}

impl Drop for Pending {
	fn drop(&mut self) {
		if let Some(socket) = self.socket.take() {
			// SAFETY: the socket was created for this accept and is owned here
			// until the response takes it.
			unsafe { closesocket(socket) };
		}
	}
}

/// `inner` must be first so the kernel's `OVERLAPPED` pointer also identifies this allocation.
#[repr(C)]
struct Overlapped {
	inner: OVERLAPPED,
	token: u64,
	submit_error: i32,
	handle: HANDLE,
}

impl Iocp {
	pub(super) fn new(config: &Config, embedded: bool) -> io::Result<(Iocp, Event)> {
		if embedded {
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
				associated: HashSet::new(),
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

	/// Puts `id` in `active` and returns the `OVERLAPPED` the kernel completes
	/// on, filling the offset `fdoff` for a byte transfer.
	fn enqueue(
		&mut self,
		id: u64,
		fd: HANDLE,
		fdoff: u64,
		memory: Option<Arc<dyn Memory>>,
		accept: Option<Pending>,
	) -> *mut OVERLAPPED {
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
				_memory: memory,
				accept,
			},
		);
		ov_ptr
	}

	/// Reports a submission the kernel refused up front, or completed: the
	/// error is 0 and the work is done. A call that failed posts no completion
	/// of its own, so the reactor queues one; a call that returned success does
	/// not need one, because an associated handle gets the kernel's packet even
	/// when the call returned synchronously.
	fn refuse(&mut self, id: u64, error: i32) {
		if let Some(active) = self.active.get_mut(&id) {
			active.ov.submit_error = error;
		}
		self.post(id);
	}

	/// A call with no overlapped form: the reactor thread runs it and queues
	/// the completion itself. Nonzero is a code `io::Error::from_raw_os_error`
	/// takes; neither is a negative NTSTATUS.
	fn in_thread(
		&mut self,
		id: u64,
		fd: RawHandle,
		work: impl FnOnce(HANDLE) -> i32,
	) -> io::Result<()> {
		let handle: HANDLE = fd.as_platform();
		self.associate(handle)?;
		let code = work(handle);
		self.enqueue(id, handle, 0, None, None);
		self.refuse(id, code);
		Ok(())
	}

	fn associate(&mut self, fd: HANDLE) -> io::Result<()> {
		if self.associated.contains(&(fd as usize)) {
			return Ok(());
		}
		// SAFETY: standard Win32 call; the port is open for the reactor's life.
		let port = unsafe { CreateIoCompletionPort(fd, self.event.handle().as_platform(), 0, 0) };
		if port.is_null() {
			return Err(io::Error::last_os_error());
		}
		self.associated.insert(fd as usize);
		Ok(())
	}

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
		let ov_ptr = self.enqueue(id, fd, fdoff, Some(memory), None);
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
			if error != IO_PENDING {
				self.refuse(id, error as i32);
			}
		}
		Ok(())
	}

	fn transfer(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
		send: bool,
	) -> io::Result<()> {
		let socket = fd.into_raw() as SOCKET;
		self.associate(socket as HANDLE)?;
		let (buf, len) = Self::buffer(&memory, &extents)?;
		let ov_ptr = self.enqueue(id, socket as HANDLE, 0, Some(memory), None);
		let buffer = WSABUF {
			len: len as u32,
			buf: buf,
		};
		let mut moved: u32 = 0;
		let mut flags: u32 = 0;
		// SAFETY: standard Winsock calls; `buffer`, `flags` and `ov_ptr` outlive
		// the call, and the kernel writes into them only until the completion.
		let failed = unsafe {
			if send {
				WSASend(socket, &buffer, 1, &mut moved, 0, ov_ptr, None)
			} else {
				WSARecv(socket, &buffer, 1, &mut moved, &mut flags, ov_ptr, None)
			}
		};
		if failed != 0 {
			let error = unsafe { WSAGetLastError() };
			if error != IO_PENDING as i32 {
				self.refuse(id, error);
			}
		}
		Ok(())
	}

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

/// The address family a listener is bound in, which an accept socket has to
/// share.
fn family(listener: SOCKET) -> io::Result<i32> {
	let mut storage: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
	let mut len = size_of::<SOCKADDR_STORAGE>() as i32;
	// SAFETY: `storage` is a sockaddr_storage — the widest address any family
	// here uses — and `len` says how much of it may be written.
	if unsafe { getsockname(listener, &mut storage as *mut _ as *mut SOCKADDR, &mut len) } != 0 {
		return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
	}
	Ok(storage.ss_family as i32)
}

/// Turns a completed `AcceptEx` into the socket it made. The accepted socket
/// is a plain descriptor until it is bound to the listener's context, so
/// `SO_UPDATE_ACCEPT_CONTEXT` runs before it is handed over; a failure closes
/// it with the `Pending` that owns it.
fn accepted(mut pending: Pending) -> io::Result<Response> {
	let listener = pending.listener;
	let socket = pending
		.socket
		.expect("a completed accept still owns its socket");
	// SAFETY: the option value is the listener the accept named, whose size
	// `SO_UPDATE_ACCEPT_CONTEXT` takes; the accepted socket is the one the
	// kernel filled in for this pending accept.
	let rc = unsafe {
		setsockopt(
			socket,
			SOL_SOCKET,
			SO_UPDATE_ACCEPT_CONTEXT,
			&listener as *const SOCKET as *const u8,
			size_of::<SOCKET>() as i32,
		)
	};
	if rc != 0 {
		return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
	}
	pending.socket = None;
	// SAFETY: the kernel created this socket for the accept, and the response
	// is now its only owner.
	Ok(Response::Accepted(unsafe {
		from_raw_socket(RawHandle::from_raw_handle(
			std::ptr::without_provenance_mut(socket),
		))
	}))
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
		let ok = unsafe { FlushFileBuffers(handle) };
		let submit_error = if ok == 0 {
			(unsafe { GetLastError() }) as i32
		} else {
			0
		};
		self.enqueue(id, handle, 0, None, None);
		if let Some(active) = self.active.get_mut(&id) {
			active.ov.submit_error = submit_error;
		}
		self.post(id);
		Ok(())
	}

	fn flush(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		// Windows exposes no close-time deferred write-back error; `FlushFileBuffers` would force durability instead.
		let handle: HANDLE = fd.as_platform();
		self.enqueue(id, handle, 0, None, None);
		self.post(id);
		Ok(())
	}

	fn allocate(&mut self, id: u64, fd: RawHandle, offset: u64, len: u64) -> io::Result<()> {
		let end = offset.saturating_add(len);
		self.in_thread(id, fd, move |handle| {
			// SAFETY: standard Win32 calls on handles the facade owns; both
			// info structs outlive their calls.
			let size = FILE_END_OF_FILE_INFO {
				EndOfFile: end as i64,
			};
			if unsafe {
				SetFileInformationByHandle(
					handle,
					FileEndOfFileInfo,
					&size as *const _ as *const core::ffi::c_void,
					size_of::<FILE_END_OF_FILE_INFO>() as u32,
				)
			} == 0
			{
				return unsafe { GetLastError() } as i32;
			}
			// How much is reserved, not where it is: the range's end is what
			// has to be backed.
			let reservation = FILE_ALLOCATION_INFO {
				AllocationSize: end as i64,
			};
			if unsafe {
				SetFileInformationByHandle(
					handle,
					FileAllocationInfo,
					&reservation as *const _ as *const core::ffi::c_void,
					size_of::<FILE_ALLOCATION_INFO>() as u32,
				)
			} == 0
			{
				return unsafe { GetLastError() } as i32;
			}
			0
		})
	}

	fn punch(&mut self, id: u64, fd: RawHandle, offset: u64, len: u64) -> io::Result<()> {
		let handle: HANDLE = fd.as_platform();
		self.associate(handle)?;
		let Some(last) = offset.checked_add(len) else {
			self.refuse(id, ERROR_INVALID_PARAMETER as i32);
			return Ok(());
		};
		let range = FILE_ZERO_DATA_INFORMATION {
			FileOffset: offset as i64,
			BeyondFinalZero: last as i64,
		};
		let mut done = 0u32;
		// SAFETY: a `DeviceIoControl` on an associated handle completes through
		// the port; the range outlives the call, which with an `OVERLAPPED`
		// only starts the operation.
		let ok = unsafe {
			DeviceIoControl(
				handle,
				FSCTL_SET_ZERO_DATA,
				&range as *const _ as *const core::ffi::c_void,
				size_of::<FILE_ZERO_DATA_INFORMATION>() as u32,
				std::ptr::null_mut(),
				0,
				&mut done,
				self.enqueue(id, handle, 0, None, None),
			)
		};
		if ok == 0 {
			let error = unsafe { GetLastError() } as i32;
			if error != IO_PENDING as i32 {
				self.refuse(id, error);
			}
		}
		Ok(())
	}

	fn recv(
		&mut self,
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		self.transfer(id, fd, memory, extents, false)
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
		id: u64,
		fd: RawHandle,
		memory: Arc<dyn Memory>,
		extents: Extents,
	) -> io::Result<()> {
		self.transfer(id, fd, memory, extents, true)
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

	fn accept(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		let listener = fd.into_raw() as SOCKET;
		self.associate(listener as HANDLE)?;
		let mut pending = Pending {
			socket: None,
			listener,
			_addresses: vec![0u8; 2 * ADDRESS_LEN as usize].into_boxed_slice(),
		};
		let addresses = pending._addresses.as_mut_ptr();
		// SAFETY: `AcceptEx` takes a socket of the listener's family created
		// for overlapped I/O; the buffer and the socket live in `pending`
		// until the completion lands.
		let socket = unsafe {
			WSASocketW(
				family(listener)?,
				SOCK_STREAM,
				IPPROTO_TCP,
				std::ptr::null(),
				0,
				WSA_FLAG_OVERLAPPED,
			)
		};
		if socket == INVALID_SOCKET {
			return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
		}
		pending.socket = Some(socket);
		let ov_ptr = self.enqueue(id, listener as HANDLE, 0, None, Some(pending));
		// SAFETY: the same buffer and socket `pending` owns, and `ov_ptr` the
		// box `active` owns; both outlive the call and the completion.
		let ok = unsafe {
			AcceptEx(
				listener,
				socket,
				addresses.cast(),
				0,
				ADDRESS_LEN,
				ADDRESS_LEN,
				std::ptr::null_mut(),
				ov_ptr,
			)
		};
		if ok == 0 {
			let error = unsafe { WSAGetLastError() };
			if error != IO_PENDING as i32 {
				self.refuse(id, error);
			}
		}
		Ok(())
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

	fn poll(&mut self, timeout: Option<Duration>, _park: &Park) -> io::Result<u64> {
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
				} else if (*ov).inner.Internal == STATUS_END_OF_FILE {
					// A read beginning at or past the end of a file: a `read`
					// moves nothing there, and reporting that is what a unix
					// file does too.
					Ok(0)
				} else if (*ov).inner.Internal == 0 {
					Ok((*ov).inner.InternalHigh as usize)
				} else {
					Err(io::Error::from_raw_os_error(ntstatus_errno(
						(*ov).inner.Internal,
					)))
				};
				(id, result)
			};
			let active = self.active.remove(&id);
			let response = match (result, active.and_then(|active| active.accept)) {
				// An accept reports the socket, not the bytes it moved.
				(Ok(_), Some(pending)) => accepted(pending),
				(result, pending) => {
					// A failed accept closes the socket it created.
					drop(pending);
					result.map(Response::Count)
				}
			};
			self.ready.push_back((id, response));
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

pub(super) fn cancelled(error: &io::Error) -> bool {
	error.raw_os_error() == Some(ERROR_OPERATION_ABORTED as i32)
}

// SAFETY: the pointer fields are opaque kernel handles; the box owns them
// and the completion collection (`poll`) is the only place they are touched.
unsafe impl Send for Overlapped {}
unsafe impl Sync for Overlapped {}
