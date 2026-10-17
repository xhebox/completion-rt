//! Windows IOCP backend; file handles require `FILE_FLAG_OVERLAPPED`.
//!
//! The reactor files each operation under an id, and this backend files it again
//! under a key of its own `active` slot map, which the `OVERLAPPED` the kernel
//! completes on carries: the kernel's packet names the key, the entry names the
//! reactor's id.

use std::collections::HashSet;
use std::io;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, AsRawSocket, FromRawHandle, FromRawSocket, OwnedHandle};
use std::sync::Arc;
use std::task::{Wake, Waker};
use std::time::Duration;

use slotmap::{DefaultKey, SlotMap};
use windows_sys::Win32::Foundation::{
	ERROR_INVALID_PARAMETER, GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::Networking::WinSock::{
	AcceptEx, INVALID_SOCKET, IPPROTO_TCP, SO_UPDATE_ACCEPT_CONTEXT, SOCK_STREAM, SOCKADDR,
	SOCKADDR_STORAGE, SOCKET, SOL_SOCKET, WSA_FLAG_OVERLAPPED, WSABUF, WSAGetLastError, WSARecv,
	WSASend, WSASocketW, closesocket, getsockname, setsockopt,
};
use windows_sys::Win32::Storage::FileSystem::{
	FILE_ALLOCATION_INFO, FILE_END_OF_FILE_INFO, FileAllocationInfo, FileEndOfFileInfo,
	FlushFileBuffers, GetFileSizeEx, ReadFile, SetFileInformationByHandle, WriteFile,
};
use windows_sys::Win32::System::IO::{
	CancelIoEx, CreateIoCompletionPort, DeviceIoControl, GetQueuedCompletionStatusEx, OVERLAPPED,
	OVERLAPPED_ENTRY, PostQueuedCompletionStatus,
};
use windows_sys::Win32::System::Ioctl::{FILE_ZERO_DATA_INFORMATION, FSCTL_SET_ZERO_DATA};

use super::Op;
use crate::desc::{BorrowedDescriptor, OwnedSocket};
use crate::error::Error;
use crate::memory::{IoVec, Span};

use super::{Config, Response};

/// How long a teardown waits for one round of completions before going on.
const WAIT_MILLIS: u32 = 1000;
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

/// The completion port every operation completes on; a packet with no
/// `OVERLAPPED` is a wake.
struct Port(OwnedHandle);

impl Port {
	fn new() -> io::Result<Port> {
		// SAFETY: plain Win32 call with valid arguments.
		let port =
			unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
		if port.is_null() {
			return Err(io::Error::last_os_error());
		}
		// SAFETY: the port handle is unowned; we own it from here on.
		Ok(Port(unsafe { OwnedHandle::from_raw_handle(port) }))
	}

	fn handle(&self) -> HANDLE {
		self.0.as_raw_handle()
	}
}

impl Wake for Port {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref();
	}

	fn wake_by_ref(self: &Arc<Self>) {
		// SAFETY: the port is open for as long as this `Arc` lives.
		if unsafe { PostQueuedCompletionStatus(self.handle(), 0, 0, std::ptr::null_mut()) } == 0 {
			log::warn!(error:% = io::Error::last_os_error(); "the completion port refused a wake");
		}
	}
}

pub(crate) struct Iocp {
	port: Arc<Port>,
	/// What the kernel holds for an operation, filed under a key of this map:
	/// the `OVERLAPPED` box the completion packet points at, and the pending
	/// accept whose socket and address buffer it fills. An operation whose entry
	/// the reactor has already taken keeps its place here until the packet
	/// arrives.
	active: SlotMap<DefaultKey, Active>,
	/// The `OVERLAPPED` boxes no operation is using, each with its allocation
	/// kept for the next one.
	free_overlapped: Vec<Box<Overlapped>>,
	/// Handles already associated with this port. A handle stays associated
	/// until it is closed, and a second `CreateIoCompletionPort` on it fails
	/// with `ERROR_INVALID_PARAMETER`, so each handle is associated once.
	associated: HashSet<usize>,
	/// The iovec array an operation resolves through: a socket transfer names
	/// every buffer from it, a read or write only its first, but every extent
	/// has to resolve before submission.
	scratch: Vec<IoVec>,
}

struct Active {
	/// The id the reactor gave the operation, which the completion reports.
	id: u64,
	ov: Box<Overlapped>,
	/// An `AcceptEx` in flight. The socket the kernel fills in and the address
	/// buffer it writes are both alive until the completion lands.
	accept: Option<Pending>,
	/// Whether the completion reports the bytes that moved. A sync, a flush, an
	/// allocation and a punch report the completion itself, and it is kept here
	/// because the reactor's entry may be gone by the time the packet arrives.
	bytes: bool,
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
	/// The key the operation is filed under in `active`.
	key: DefaultKey,
	/// The id the reactor gave the operation.
	id: u64,
	submit_error: i32,
	handle: HANDLE,
}

impl Overlapped {
	/// A boxed `OVERLAPPED` ready to be filled in for the next operation.
	fn zeroed() -> Overlapped {
		Overlapped {
			// SAFETY: `OVERLAPPED` is plain data; zero is the "no offset, no
			// event" state Windows documents for an unused one.
			inner: unsafe { std::mem::zeroed() },
			key: DefaultKey::default(),
			id: 0,
			submit_error: 0,
			handle: std::ptr::null_mut(),
		}
	}
}

impl Iocp {
	pub(super) fn new(config: &Config) -> io::Result<Iocp> {
		if config.iopoll || config.sqpoll {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"a completion port has no IOPOLL or SQPOLL mode",
			));
		}
		Ok(Iocp {
			port: Arc::new(Port::new()?),
			active: SlotMap::new(),
			free_overlapped: Vec::new(),
			associated: HashSet::new(),
			scratch: Vec::new(),
		})
	}

	fn post(&mut self, key: DefaultKey) {
		let Some(active) = self.active.get(key) else {
			return;
		};
		let ov_ptr = &*active.ov as *const Overlapped as *const OVERLAPPED as *mut OVERLAPPED;
		// SAFETY: `ov_ptr` names the box in `active`, which lives until the
		// completion is collected in `poll`.
		unsafe { PostQueuedCompletionStatus(self.port.handle(), 0, 0, ov_ptr) };
	}

	/// Files `id` in `active` and returns the key it is filed under, together
	/// with the `OVERLAPPED` the kernel completes on, filling the offset `fdoff`
	/// for a byte transfer.
	fn enqueue(
		&mut self,
		id: u64,
		fd: HANDLE,
		fdoff: u64,
		accept: Option<Pending>,
		bytes: bool,
	) -> (DefaultKey, *mut OVERLAPPED) {
		let mut ov = self
			.free_overlapped
			.pop()
			.unwrap_or_else(|| Box::new(Overlapped::zeroed()));
		ov.inner = OVERLAPPED {
			Internal: 0,
			InternalHigh: 0,
			Anonymous: windows_sys::Win32::System::IO::OVERLAPPED_0 {
				Anonymous: windows_sys::Win32::System::IO::OVERLAPPED_0_0 {
					Offset: fdoff as u32,
					OffsetHigh: (fdoff >> 32) as u32,
				},
			},
			hEvent: std::ptr::null_mut(),
		};
		ov.id = id;
		ov.submit_error = 0;
		ov.handle = fd;
		// The box lives on the heap, so the `OVERLAPPED` the kernel is handed
		// keeps its address when the box moves into the table.
		let ov_ptr = &mut *ov as *mut Overlapped as *mut OVERLAPPED;
		let key = self.active.insert_with_key(move |key| {
			ov.key = key;
			Active {
				id,
				ov,
				accept,
				bytes,
			}
		});
		(key, ov_ptr)
	}

	/// Reports a submission the kernel refused up front, or completed: the
	/// error is 0 and the work is done. A call that failed posts no completion
	/// of its own, so one is posted here for it; a call that returned success
	/// does not need one, because an associated handle gets the kernel's packet
	/// even when the call returned synchronously.
	fn refuse(&mut self, key: DefaultKey, error: i32) {
		if let Some(active) = self.active.get_mut(key) {
			active.ov.submit_error = error;
		}
		self.post(key);
	}

	/// A call with no overlapped form: the reactor thread runs it and queues
	/// the completion itself. Nonzero is a code `io::Error::from_raw_os_error`
	/// takes; neither is a negative NTSTATUS.
	fn in_thread(&mut self, id: u64, handle: HANDLE, work: impl FnOnce(HANDLE) -> i32) {
		let code = work(handle);
		let (key, _) = self.enqueue(id, handle, 0, None, false);
		self.refuse(key, code);
	}

	fn punch(&mut self, id: u64, handle: HANDLE, offset: u64, len: u64) -> io::Result<()> {
		self.associate(handle)?;
		let Some(last) = offset.checked_add(len) else {
			// The range cannot name one, and there is nothing to ask the kernel
			// for: the refusal is this operation's completion.
			let (key, _) = self.enqueue(id, handle, 0, None, false);
			self.refuse(key, ERROR_INVALID_PARAMETER as i32);
			return Ok(());
		};
		let range = FILE_ZERO_DATA_INFORMATION {
			FileOffset: offset as i64,
			BeyondFinalZero: last as i64,
		};
		let mut done = 0u32;
		let (key, ov_ptr) = self.enqueue(id, handle, 0, None, false);
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
				ov_ptr,
			)
		};
		if ok == 0 {
			let error = unsafe { GetLastError() } as i32;
			if error != IO_PENDING as i32 {
				self.refuse(key, error);
			}
		}
		Ok(())
	}

	fn accept(&mut self, id: u64, listener: SOCKET) -> io::Result<()> {
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
		let (key, ov_ptr) = self.enqueue(id, listener as HANDLE, 0, Some(pending), false);
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
				self.refuse(key, error);
			}
		}
		Ok(())
	}

	fn associate(&mut self, fd: HANDLE) -> io::Result<()> {
		if self.associated.contains(&(fd as usize)) {
			return Ok(());
		}
		// SAFETY: standard Win32 call; the port is open for the reactor's life.
		let port = unsafe { CreateIoCompletionPort(fd, self.port.handle(), 0, 0) };
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
		span: &Span,
		fdoff: u64,
	) -> io::Result<()> {
		self.associate(fd)?;
		let (buf, len) = Self::first(span, &mut self.scratch)?;
		let (key, ov_ptr) = self.enqueue(id, fd, fdoff, None, true);
		// SAFETY: standard Win32 calls; `ov_ptr` names the box in `active`,
		// alive until the completion is collected.
		let ok = unsafe {
			if read {
				ReadFile(fd, buf.cast(), capped(len), std::ptr::null_mut(), ov_ptr)
			} else {
				WriteFile(fd, buf.cast(), capped(len), std::ptr::null_mut(), ov_ptr)
			}
		};
		if ok == 0 {
			let error = unsafe { GetLastError() };
			if error != IO_PENDING {
				self.refuse(key, error as i32);
			}
		}
		Ok(())
	}

	fn transfer(&mut self, id: u64, socket: SOCKET, span: &Span, send: bool) -> io::Result<()> {
		self.associate(socket as HANDLE)?;
		// The array only has to outlive the call: an overlapped `WSASend` or
		// `WSARecv` copies it, and keeps only the buffers it names. Resolving
		// first refuses an extent out of bounds before anything is queued.
		span.resolve_into(&mut self.scratch)?;
		let wsabufs: Vec<WSABUF> = self
			.scratch
			.iter()
			.map(|buf| WSABUF {
				len: capped(buf.len()),
				buf: buf.as_ptr(),
			})
			.collect();
		let (key, ov_ptr) = self.enqueue(id, socket as HANDLE, 0, None, true);
		let count = wsabufs.len() as u32;
		let mut moved: u32 = 0;
		let mut flags: u32 = 0;
		// SAFETY: standard Winsock calls; `wsabufs`, `flags` and `ov_ptr` outlive
		// the call, and the kernel writes into them only until the completion.
		let failed = unsafe {
			if send {
				WSASend(socket, wsabufs.as_ptr(), count, &mut moved, 0, ov_ptr, None)
			} else {
				WSARecv(
					socket,
					wsabufs.as_ptr(),
					count,
					&mut moved,
					&mut flags,
					ov_ptr,
					None,
				)
			}
		};
		if failed != 0 {
			let error = unsafe { WSAGetLastError() };
			if error != IO_PENDING as i32 {
				self.refuse(key, error);
			}
		}
		Ok(())
	}

	/// The first of `span`: `ReadFile` and `WriteFile` take one, and their
	/// scatter forms want page-aligned unbuffered I/O. A gathered span moves
	/// short, and a caller after all of it repeats the call. Every extent is
	/// still resolved, so one out of bounds refuses the submission.
	fn first(span: &Span, scratch: &mut Vec<IoVec>) -> io::Result<(*mut u8, usize)> {
		if let Some(buf) = span.resolve_single() {
			let buf = buf?;
			return Ok((buf.as_ptr(), buf.len()));
		}
		span.resolve_into(scratch)?;
		let buf = scratch.first().expect("a span of more than one extent");
		Ok((buf.as_ptr(), buf.len()))
	}

	fn unsupported() -> io::Error {
		io::Error::new(
			io::ErrorKind::Unsupported,
			"this operation is not implemented on the IOCP backend",
		)
	}
}

/// The most bytes one Win32 call names: `ReadFile` and a `WSABUF` carry a
/// `u32`, so a longer buffer moves what fits and reports a short count, which
/// `read_exact_at` and `write_all_at` repeat.
fn capped(len: usize) -> u32 {
	u32::try_from(len).unwrap_or(u32::MAX)
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
		OwnedSocket::from_raw_socket(socket as _)
	}))
}

/// The file handle an operation on a non-socket descriptor runs on.
fn as_handle(fd: BorrowedDescriptor<'_>) -> io::Result<HANDLE> {
	match fd {
		BorrowedDescriptor::Handle(handle) => Ok(handle.as_raw_handle()),
		BorrowedDescriptor::Socket(_) => Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a file call on a socket descriptor",
		)),
	}
}

/// The socket an operation on a socket descriptor runs on.
fn as_socket(fd: BorrowedDescriptor<'_>) -> io::Result<SOCKET> {
	match fd {
		BorrowedDescriptor::Socket(socket) => Ok(socket.as_raw_socket() as SOCKET),
		BorrowedDescriptor::Handle(_) => Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a socket call on a non-socket descriptor",
		)),
	}
}

impl Iocp {
	pub(super) fn submit(
		&mut self,
		id: u64,
		fd: BorrowedDescriptor<'_>,
		op: Op,
	) -> Result<(), Error> {
		match op {
			Op::Read { offset, span } => self.issue(id, as_handle(fd)?, true, &span, offset)?,
			Op::Write { offset, span } => self.issue(id, as_handle(fd)?, false, &span, offset)?,
			Op::Fsync => {
				self.in_thread(id, as_handle(fd)?, |handle| {
					// SAFETY: a plain Win32 call on a handle the facade owns.
					if unsafe { FlushFileBuffers(handle) } == 0 {
						unsafe { GetLastError() as i32 }
					} else {
						0
					}
				});
			}
			Op::Flush => self.in_thread(id, as_handle(fd)?, |_| 0),
			Op::Allocate { offset, len } => {
				self.in_thread(id, as_handle(fd)?, move |handle| {
					allocate(handle, offset, len)
				});
			}
			Op::Punch { offset, len } => self.punch(id, as_handle(fd)?, offset, len)?,
			Op::Recv { span } => self.transfer(id, as_socket(fd)?, &span, false)?,
			Op::Send { span, fds } => {
				if !fds.is_empty() {
					return Err(Self::unsupported().into());
				}
				self.transfer(id, as_socket(fd)?, &span, true)?;
			}
			Op::Accept => self.accept(id, as_socket(fd)?)?,
			Op::RecvWithFds { .. } => return Err(Self::unsupported().into()),
			Op::Readable | Op::Writable | Op::Hangup => {
				return Err(io::Error::new(
					io::ErrorKind::Unsupported,
					"a completion port has no readiness wait",
				)
				.into());
			}
		}
		Ok(())
	}

	pub(super) fn cancel(&mut self, id: u64) {
		let Some(active) = self.active.values().find(|active| active.id == id) else {
			return;
		};
		let pointer = &*active.ov as *const Overlapped as *const OVERLAPPED;
		// SAFETY: the box stays in `active` until its completion is collected,
		// which is after the kernel has stopped touching the OVERLAPPED.
		unsafe { CancelIoEx(active.ov.handle, pointer) };
	}

	pub(super) fn poll(
		&mut self,
		timeout: Option<Duration>,
		mut complete: impl FnMut(u64, Result<Response, Error>),
	) -> Result<(), Error> {
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
				self.port.handle(),
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
				return Ok(());
			}
			return Err(Error::from(error));
		}
		for entry in entries[..count as usize].iter() {
			if entry.lpOverlapped.is_null() {
				continue;
			}
			// SAFETY: the pointer names a box we inserted into `active` at
			// submit time and that is alive until this collection.
			let ov = entry.lpOverlapped as *mut Overlapped;
			let (key, id, result) = unsafe {
				let key = (*ov).key;
				let id = (*ov).id;
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
				(key, id, result)
			};
			// The kernel is done with what it held for this operation, whether
			// or not the reactor still waits for one of its own. The box goes
			// back to the free list with its allocation kept.
			let active = self.active.remove(key);
			let bytes = active.as_ref().is_some_and(|active| active.bytes);
			let accept = active.and_then(|active| {
				self.free_overlapped.push(active.ov);
				active.accept
			});
			let response = match (result, accept) {
				// An accept reports the socket, not the bytes it moved.
				(Ok(_), Some(pending)) => accepted(pending),
				(result, pending) => {
					// A failed accept closes the socket it created.
					drop(pending);
					result.map(|moved| match bytes {
						true => Response::Count(moved),
						false => Response::Done,
					})
				}
			};
			complete(id, response.map_err(landed));
		}
		Ok(())
	}

	pub(super) fn waker(&self) -> Waker {
		Waker::from(Arc::clone(&self.port))
	}
}

impl Drop for Iocp {
	fn drop(&mut self) {
		// A packet may still be on its way for an operation the reactor let go
		// of, and the box it points at must not go before it arrives. Ask the
		// kernel to stop what it still holds, then collect what it returns.
		for active in self.active.values() {
			let pointer = &*active.ov as *const Overlapped as *const OVERLAPPED;
			// SAFETY: the box stays in `active` until its completion is
			// collected below.
			unsafe { CancelIoEx(active.ov.handle, pointer) };
		}
		let mut entries: [OVERLAPPED_ENTRY; 64] = unsafe { std::mem::zeroed() };
		while !self.active.is_empty() {
			let mut count: u32 = 0;
			// SAFETY: standard Win32 call; entries and count point at writable
			// memory of the right size.
			let ok = unsafe {
				GetQueuedCompletionStatusEx(
					self.port.handle(),
					entries.as_mut_ptr(),
					entries.len() as u32,
					&mut count,
					WAIT_MILLIS,
					0,
				)
			};
			// The port refused the wait, or nothing came back in time: what is
			// still held cannot be collected from here, and waiting forever
			// would be worse than going on.
			if ok == 0 || count == 0 {
				break;
			}
			for entry in entries[..count as usize].iter() {
				if entry.lpOverlapped.is_null() {
					continue;
				}
				// SAFETY: the pointer names a box in `active`, which is alive.
				let key = unsafe { (*(entry.lpOverlapped as *mut Overlapped)).key };
				self.active.remove(key);
			}
		}
	}
}

/// Grows the file to the range's end, so the space it needs is reserved; the
/// call has no overlapped form and runs on the reactor thread.
fn allocate(handle: HANDLE, offset: u64, len: u64) -> i32 {
	let end = offset.saturating_add(len);
	// SAFETY: standard Win32 calls on handles the facade owns; both info
	// structs outlive their calls.
	//
	// Setting the end of file shrinks as readily as it grows, and a reservation
	// below the current length is what the range is already backed by;
	// allocate only grows, so the current length floors both.
	let mut current = 0i64;
	if unsafe { GetFileSizeEx(handle, &mut current) } == 0 {
		return unsafe { GetLastError() } as i32;
	}
	let want = end.max(current.max(0) as u64);
	let size = FILE_END_OF_FILE_INFO {
		EndOfFile: want as i64,
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
	// How much is reserved, not where it is: the range's end is what has to be
	// backed.
	let reservation = FILE_ALLOCATION_INFO {
		AllocationSize: want as i64,
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
}

/// A failed completion; the abort `CancelIoEx` causes is the cancel.
fn landed(error: io::Error) -> Error {
	if error.raw_os_error() == Some(ERROR_OPERATION_ABORTED as i32) {
		Error::cancelled()
	} else {
		Error::from(error)
	}
}

// SAFETY: the pointer fields are opaque kernel handles; the box owns them
// and the completion collection (`poll`) is the only place they are touched.
unsafe impl Send for Overlapped {}
unsafe impl Sync for Overlapped {}
