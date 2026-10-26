//! Windows IOCP backend; file handles require `FILE_FLAG_OVERLAPPED`.
//!
//! Each operation is filed under a key of this backend's `active` slot map,
//! which the `OVERLAPPED` the kernel completes on carries: the kernel's packet
//! names the key, and the key's FFI form is the id the completion reports.

use std::collections::HashSet;
use std::io;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, AsRawSocket, FromRawHandle, FromRawSocket, OwnedHandle};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use slotmap::{DefaultKey, Key, KeyData, SlotMap};
use socket2::SockAddr;
use windows_sys::Win32::Foundation::{
	ERROR_INVALID_PARAMETER, GetLastError, HANDLE, INVALID_HANDLE_VALUE, NTSTATUS,
	RtlNtStatusToDosError, WAIT_TIMEOUT,
};
use windows_sys::Win32::Networking::WinSock::{
	ADDRESS_FAMILY, AF_INET, AF_INET6, AF_UNIX, AcceptEx, INVALID_SOCKET, IPPROTO_TCP,
	LPFN_CONNECTEX, SIO_BASE_HANDLE, SIO_GET_EXTENSION_FUNCTION_POINTER, SO_UPDATE_ACCEPT_CONTEXT,
	SO_UPDATE_CONNECT_CONTEXT, SOCK_STREAM, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE,
	SOCKET, SOCKET_ERROR, SOL_SOCKET, WSA_FLAG_OVERLAPPED, WSABUF, WSAEINVAL, WSAGetLastError,
	WSAID_CONNECTEX, WSAIoctl, WSARecv, WSASend, WSASocketW, bind, closesocket, getsockname,
	setsockopt,
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
use windows_sys::Win32::System::Threading::{
	INFINITE, RegisterWaitForSingleObject, UnregisterWaitEx, WT_EXECUTEONLYONCE,
};
use windows_sys::core::GUID;

use super::{Config, Driver, IoVec, Op, Response, Waker, WakerInner};
use crate::desc::{BorrowedDescriptor, OwnedSocket};
use crate::error::{Error, Result};

/// How long a teardown waits for one round of completions before going on.
const WAIT_MILLIS: u32 = 1000;
/// `ERROR_IO_PENDING`, the same number as `WSA_IO_PENDING`: the operation was
/// accepted and will complete later.
const IO_PENDING: u32 = 997;
/// `ERROR_OPERATION_ABORTED`, the error a stopped operation reports: a cancel
/// completes with it once the status block's `STATUS_CANCELLED` is converted.
const ERROR_OPERATION_ABORTED: u32 = 995;
/// `STATUS_END_OF_FILE`: the status a read beginning at or past the end of a
/// file completes with. The status block carries the NTSTATUS itself, not the
/// `ERROR_HANDLE_EOF` the same condition reports from a synchronous call.
const STATUS_END_OF_FILE: usize = 0xC000_0011;
/// The room `AcceptEx` needs beyond the two addresses it writes; the kernel
/// takes `sizeof(SOCKADDR) + 16` per address, and `SOCKADDR_STORAGE` is the
/// widest `SOCKADDR`.
const ADDRESS_LEN: u32 = (size_of::<SOCKADDR_STORAGE>() + 16) as u32;
/// `IOCTL_AFD_POLL`: what asks the AFD driver to report a socket's readiness
/// through the completion port the request names.
const IOCTL_AFD_POLL: u32 = 0x0001_2024;
/// The AFD events each readiness wait arms.
const AFD_POLL_RECEIVE: u32 = 0x0001;
const AFD_POLL_RECEIVE_EXPEDITED: u32 = 0x0002;
const AFD_POLL_SEND: u32 = 0x0004;
const AFD_POLL_DISCONNECT: u32 = 0x0008;
const AFD_POLL_ABORT: u32 = 0x0010;
const AFD_POLL_LOCAL_CLOSE: u32 = 0x0020;
const AFD_POLL_ACCEPT: u32 = 0x0080;
const AFD_POLL_CONNECT_FAIL: u32 = 0x0100;
/// The `ERROR_OPERATION_ABORTED` a cancelled completion reports. The status
/// block carries an `NTSTATUS`, and only the kernel's own conversion reads one:
/// masking the low word turns `STATUS_CANCELLED` into `ERROR_NOT_OWNER`, which
/// names an unrelated failure.
fn ntstatus_errno(status: usize) -> i32 {
	// SAFETY: plain `ntdll` call on the status the kernel wrote into the block.
	unsafe { RtlNtStatusToDosError(status as NTSTATUS) as i32 }
}

/// The completion port every operation completes on; a packet with no
/// `OVERLAPPED` is a wake.
pub(super) struct Port {
	handle: OwnedHandle,
	notified: AtomicBool,
}

impl Port {
	fn new() -> io::Result<Port> {
		// SAFETY: plain Win32 call with valid arguments.
		let port =
			unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
		if port.is_null() {
			return Err(io::Error::last_os_error());
		}
		// SAFETY: the port handle is unowned; we own it from here on.
		Ok(Port {
			handle: unsafe { OwnedHandle::from_raw_handle(port) },
			notified: AtomicBool::new(false),
		})
	}

	fn handle(&self) -> HANDLE {
		self.handle.as_raw_handle()
	}

	pub(super) fn wake(&self) {
		if self.notified.swap(true, Ordering::AcqRel) {
			return;
		}
		// SAFETY: the port is open for as long as this `Port` lives.
		if unsafe { PostQueuedCompletionStatus(self.handle(), 0, 0, std::ptr::null_mut()) } == 0 {
			self.notified.store(false, Ordering::Release);
			log::warn!(error:% = io::Error::last_os_error(); "the completion port refused a wake");
		}
	}
}

/// One handle an `IOCTL_AFD_POLL` names, with the events it waits for and the
/// status the driver reports them under.
#[repr(C)]
struct AfdPollHandleInfo {
	handle: HANDLE,
	events: u32,
	status: NTSTATUS,
}

/// The request `IOCTL_AFD_POLL` takes and fills in: the AFD driver reads it
/// when the request runs, so it is boxed and kept in the operation's `active`
/// entry until the completion lands.
#[repr(C)]
struct AfdPollInfo {
	timeout: i64,
	number_of_handles: u32,
	exclusive: u32,
	handles: [AfdPollHandleInfo; 1],
}

// SAFETY: the request is plain data holding one opaque kernel handle; the box
// that owns it stays where it is, and the reactor is the only reader of it.
unsafe impl Send for AfdPollInfo {}

/// What a waitable handle's single wait answers with: the port to post the
/// completion to, the `OVERLAPPED` that completion names, and whether one of
/// the two paths has already posted it.
struct WaitPacket {
	port: Arc<Port>,
	ov_ptr: usize,
	fired: AtomicBool,
}

/// The thread-pool callback a waitable handle's wait runs once the object
/// signals: posting the completion turns the signal into the operation's
/// result.
unsafe extern "system" fn wait_callback(context: *mut core::ffi::c_void, _timed_out: bool) {
	// SAFETY: `context` points at a `WaitPacket` its `RegisteredWait` keeps
	// alive until `UnregisterWaitEx(.., INVALID_HANDLE_VALUE)` returns, which is
	// after any running callback has returned.
	let packet = unsafe { &*(context as *const WaitPacket) };
	// A cancel that posted first owns the completion; the other one does nothing.
	if !packet.fired.swap(true, Ordering::AcqRel) {
		// SAFETY: the packet names the `OVERLAPPED` box the operation is filed
		// under, which lives until the completion is collected.
		unsafe {
			PostQueuedCompletionStatus(
				packet.port.handle(),
				0,
				0,
				packet.ov_ptr as *mut OVERLAPPED,
			);
		}
	}
}

/// One waitable handle's registration with the thread pool, kept in the
/// operation's `active` entry until the completion is collected.
struct RegisteredWait {
	wait_object: HANDLE,
	packet: Arc<WaitPacket>,
}

// SAFETY: registration and unregistration are plain Win32 calls on a
// thread-pool wait, which either thread may make.
unsafe impl Send for RegisteredWait {}

impl Drop for RegisteredWait {
	fn drop(&mut self) {
		if !self.wait_object.is_null() {
			// `INVALID_HANDLE_VALUE` waits for a running callback to return, so
			// the packet the callback reads stays alive for the whole of it.
			unsafe { UnregisterWaitEx(self.wait_object, INVALID_HANDLE_VALUE) };
		}
	}
}

pub struct Iocp<T = ()> {
	port: Arc<Port>,
	/// What the kernel holds for an operation, filed under a key of this map:
	/// the `OVERLAPPED` box the completion packet points at, and the pending
	/// accept whose socket and address buffer it fills. An operation whose entry
	/// the reactor has already taken keeps its place here until the packet
	/// arrives.
	active: SlotMap<DefaultKey, Active<T>>,
	/// The `OVERLAPPED` boxes no operation is using, each with its allocation
	/// kept for the next one.
	#[allow(clippy::vec_box)]
	free_overlapped: Vec<Box<Overlapped>>,
	/// Handles already associated with this port. A handle stays associated
	/// until it is closed, and a second `CreateIoCompletionPort` on it fails
	/// with `ERROR_INVALID_PARAMETER`, so each handle is associated once.
	associated: HashSet<usize>,
}

struct Active<T> {
	ov: Box<Overlapped>,
	/// An `AcceptEx` in flight. The socket the kernel fills in and the address
	/// buffer it writes are both alive until the completion lands.
	accept: Option<Pending>,
	/// A `ConnectEx` in flight, and the socket it connects: the socket takes up
	/// the connection's context once the completion lands.
	connect: Option<SOCKET>,
	/// Whether the completion reports the bytes that moved. A sync, a flush, an
	/// allocation and a punch report the completion itself, and it is kept here
	/// because the reactor's entry may be gone by the time the packet arrives.
	bytes: bool,
	/// The `IOCTL_AFD_POLL` request the kernel reads and fills in while it is in
	/// flight.
	_afd: Option<Box<AfdPollInfo>>,
	/// The thread-pool wait a waitable handle is waited on with.
	wait: Option<RegisteredWait>,
	/// The caller's data, handed back with the completion.
	data: T,
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
	/// The key the operation is filed under in `active`, whose FFI form is the
	/// id its completion reports.
	key: DefaultKey,
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
			submit_error: 0,
			handle: std::ptr::null_mut(),
		}
	}
}

impl<T> Iocp<T> {
	pub(super) fn new(config: &Config) -> io::Result<Iocp<T>> {
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

	/// Files the operation in `active` and returns the key it is filed under,
	/// together with the `OVERLAPPED` the kernel completes on, filling the
	/// offset `fdoff` for a byte transfer.
	fn enqueue(
		&mut self,
		fd: HANDLE,
		fdoff: u64,
		accept: Option<Pending>,
		bytes: bool,
		data: T,
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
		ov.submit_error = 0;
		ov.handle = fd;
		// The box lives on the heap, so the `OVERLAPPED` the kernel is handed
		// keeps its address when the box moves into the table.
		let ov_ptr = &mut *ov as *mut Overlapped as *mut OVERLAPPED;
		let key = self.active.insert_with_key(move |key| {
			ov.key = key;
			Active {
				ov,
				accept,
				connect: None,
				bytes,
				_afd: None,
				wait: None,
				data,
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
	fn in_thread(&mut self, handle: HANDLE, work: impl FnOnce(HANDLE) -> i32, data: T) -> u64 {
		let code = work(handle);
		let (key, _) = self.enqueue(handle, 0, None, false, data);
		self.refuse(key, code);
		key.data().as_ffi()
	}

	fn punch(&mut self, handle: HANDLE, offset: u64, len: u64, data: T) -> io::Result<u64> {
		self.associate(handle)?;
		let Some(last) = offset.checked_add(len) else {
			// The range cannot name one, and there is nothing to ask the kernel
			// for: the refusal is this operation's completion.
			let (key, _) = self.enqueue(handle, 0, None, false, data);
			self.refuse(key, ERROR_INVALID_PARAMETER as i32);
			return Ok(key.data().as_ffi());
		};
		let range = FILE_ZERO_DATA_INFORMATION {
			FileOffset: offset as i64,
			BeyondFinalZero: last as i64,
		};
		let mut done = 0u32;
		let (key, ov_ptr) = self.enqueue(handle, 0, None, false, data);
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
		Ok(key.data().as_ffi())
	}

	fn accept(&mut self, listener: SOCKET, data: T) -> io::Result<u64> {
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
		let (key, ov_ptr) = self.enqueue(listener as HANDLE, 0, Some(pending), false, data);
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
		Ok(key.data().as_ffi())
	}

	/// Connects `socket` to `addr` through `ConnectEx`, the only overlapped form
	/// a connection has.
	fn connect(&mut self, socket: SOCKET, addr: &SockAddr, data: T) -> io::Result<u64> {
		self.associate(socket as HANDLE)?;
		bind_unnamed(socket, addr)?;
		let mut connect_ex: LPFN_CONNECTEX = None;
		let mut bytes = 0u32;
		// SAFETY: the GUID names the extension, and `connect_ex` is where its
		// pointer is written: both sizes are those of the values themselves.
		let rc = unsafe {
			WSAIoctl(
				socket,
				SIO_GET_EXTENSION_FUNCTION_POINTER,
				&WSAID_CONNECTEX as *const _ as *const core::ffi::c_void,
				size_of::<GUID>() as u32,
				&mut connect_ex as *mut LPFN_CONNECTEX as *mut core::ffi::c_void,
				size_of::<LPFN_CONNECTEX>() as u32,
				&mut bytes,
				std::ptr::null_mut(),
				None,
			)
		};
		if rc != 0 {
			return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
		}
		let connect_ex = connect_ex.expect("a taken extension function pointer is set");
		let (key, ov_ptr) = self.enqueue(socket as HANDLE, 0, None, false, data);
		// The socket takes up the connection's context when the completion lands,
		// which is where `poll` runs it; until then this entry names it.
		self.active[key].connect = Some(socket);
		// SAFETY: the address outlives the call, which copies it, and `ov_ptr`
		// names the box `active` owns until the completion lands.
		let ok = unsafe {
			connect_ex(
				socket,
				addr.as_ptr().cast(),
				addr.len(),
				std::ptr::null(),
				0,
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
		Ok(key.data().as_ffi())
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

	fn issue(&mut self, fd: HANDLE, read: bool, buf: IoVec, fdoff: u64, data: T) -> Result<u64> {
		self.associate(fd)?;
		let (key, ov_ptr) = self.enqueue(fd, fdoff, None, true, data);
		// SAFETY: standard Win32 calls; `ov_ptr` names the box in `active`,
		// alive until the completion is collected.
		let ok = unsafe {
			if read {
				ReadFile(
					fd,
					buf.ptr.cast(),
					capped(buf.len),
					std::ptr::null_mut(),
					ov_ptr,
				)
			} else {
				WriteFile(
					fd,
					buf.ptr.cast(),
					capped(buf.len),
					std::ptr::null_mut(),
					ov_ptr,
				)
			}
		};
		if ok == 0 {
			let error = unsafe { GetLastError() };
			if error != IO_PENDING {
				self.refuse(key, error as i32);
			}
		}
		Ok(key.data().as_ffi())
	}

	fn transfer(&mut self, socket: SOCKET, buf: IoVec, send: bool, data: T) -> Result<u64> {
		self.associate(socket as HANDLE)?;
		// The `WSABUF` only has to outlive the call: an overlapped `WSASend` or
		// `WSARecv` copies it, and keeps only the buffer it names.
		let wsabuf = WSABUF {
			len: capped(buf.len),
			buf: buf.ptr,
		};
		let (key, ov_ptr) = self.enqueue(socket as HANDLE, 0, None, true, data);
		let mut moved: u32 = 0;
		let mut flags: u32 = 0;
		// SAFETY: standard Winsock calls; `wsabuf`, `flags` and `ov_ptr` outlive
		// the call, and the kernel writes into them only until the completion.
		let failed = unsafe {
			if send {
				WSASend(socket, &wsabuf, 1, &mut moved, 0, ov_ptr, None)
			} else {
				WSARecv(socket, &wsabuf, 1, &mut moved, &mut flags, ov_ptr, None)
			}
		};
		if failed != 0 {
			let error = unsafe { WSAGetLastError() };
			if error != IO_PENDING as i32 {
				self.refuse(key, error);
			}
		}
		Ok(key.data().as_ffi())
	}

	/// Routes a byte transfer by what the descriptor is: a stream socket takes
	/// `recv`/`send`, and a handle — or a transfer that names an offset — takes
	/// `ReadFile`/`WriteFile`.
	fn read_or_write(
		&mut self,
		fd: BorrowedDescriptor<'_>,
		read: bool,
		buf: IoVec,
		offset: Option<u64>,
		data: T,
	) -> Result<u64> {
		match (offset, fd) {
			(None, BorrowedDescriptor::Socket(socket)) => {
				self.transfer(socket.as_raw_socket() as SOCKET, buf, !read, data)
			}
			(offset, fd) => self.issue(as_handle(fd)?, read, buf, offset.unwrap_or(0), data),
		}
	}

	/// Waits until the socket's AFD endpoint reports one of `events`, through a
	/// request that completes on the port once it does.
	fn afd_poll(&mut self, socket: SOCKET, events: u32, data: T) -> Result<u64> {
		let base = base_socket(socket) as HANDLE;
		self.associate(base)?;
		let mut afd = Box::new(AfdPollInfo {
			// A wait the cancel ends is the only end it has.
			timeout: i64::MAX,
			number_of_handles: 1,
			exclusive: 0,
			handles: [AfdPollHandleInfo {
				handle: base,
				events,
				status: 0,
			}],
		});
		let afd_ptr = &mut *afd as *mut AfdPollInfo as *mut core::ffi::c_void;
		let (key, ov_ptr) = self.enqueue(base, 0, None, false, data);
		// The driver reads the request once the call returns, so the box moves
		// into the entry that keeps it where the pointer says it is.
		self.active[key]._afd = Some(afd);
		let mut done = 0u32;
		// SAFETY: `afd_ptr` names the box `active` now holds, and `ov_ptr` names
		// the box it holds too; both outlive the call and the completion.
		let ok = unsafe {
			DeviceIoControl(
				base,
				IOCTL_AFD_POLL,
				afd_ptr,
				size_of::<AfdPollInfo>() as u32,
				afd_ptr,
				size_of::<AfdPollInfo>() as u32,
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
		Ok(key.data().as_ffi())
	}

	/// Waits until the waitable kernel object `handle` signals.
	fn wait_handle(&mut self, handle: HANDLE, data: T) -> Result<u64> {
		let (key, ov_ptr) = self.enqueue(handle, 0, None, false, data);
		let packet = Arc::new(WaitPacket {
			port: Arc::clone(&self.port),
			ov_ptr: ov_ptr as usize,
			fired: AtomicBool::new(false),
		});
		let mut wait_object: HANDLE = std::ptr::null_mut();
		// SAFETY: the callback is handed `packet`, which the registration filed
		// below keeps alive until it is unregistered, and a one-shot wait runs it
		// at most once.
		let ok = unsafe {
			RegisterWaitForSingleObject(
				&mut wait_object,
				handle,
				Some(wait_callback),
				Arc::as_ptr(&packet) as *const core::ffi::c_void,
				INFINITE,
				WT_EXECUTEONLYONCE,
			)
		};
		if ok == 0 {
			// Nothing was registered, so no callback can name the packet.
			self.refuse(key, unsafe { GetLastError() } as i32);
			return Ok(key.data().as_ffi());
		}
		self.active[key].wait = Some(RegisteredWait {
			wait_object,
			packet,
		});
		Ok(key.data().as_ffi())
	}

	/// Asks the kernel to stop what it still holds for `active`, or answers a
	/// waitable handle's wait whose object has not signalled.
	fn cancel_active(port: &Port, active: &mut Active<T>) {
		if let Some(wait) = &active.wait {
			// A wait already taken by its callback is on its way to the port, and
			// the cancel has nothing left to say.
			if !wait.packet.fired.swap(true, Ordering::AcqRel) {
				active.ov.submit_error = ERROR_OPERATION_ABORTED as i32;
				// SAFETY: the box stays in `active` until its completion is
				// collected, which is after this packet arrives.
				unsafe {
					PostQueuedCompletionStatus(
						port.handle(),
						0,
						0,
						&*active.ov as *const Overlapped as *mut OVERLAPPED,
					);
				}
			}
			return;
		}
		let pointer = &*active.ov as *const Overlapped as *const OVERLAPPED;
		// SAFETY: the box stays in `active` until its completion is collected,
		// which is after the kernel has stopped touching the OVERLAPPED.
		unsafe { CancelIoEx(active.ov.handle, pointer) };
	}
}

/// Grows the file to the range's end, so the space it needs is reserved; the
/// call has no overlapped form.
///
/// Setting the end of file shrinks as readily as it grows, and a reservation
/// below the current length is what the range is already backed by; allocate
/// only grows, so the current length floors both.
fn allocate(handle: HANDLE, offset: u64, len: u64) -> i32 {
	let end = offset.saturating_add(len);
	// SAFETY: standard Win32 calls on handles the caller holds; both info
	// structs outlive their calls.
	let mut current = 0i64;
	if unsafe { GetFileSizeEx(handle, &mut current) } == 0 {
		return unsafe { GetLastError() as i32 };
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
		return unsafe { GetLastError() as i32 };
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
		return unsafe { GetLastError() as i32 };
	}
	0
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

/// Binds `socket` to an unnamed local address of `addr`'s family. `ConnectEx`
/// takes only a bound socket, and the local address a client's own holds is the
/// unnamed one: the wildcard address for an internet family, and a socket with
/// no path for a unix one.
fn bind_unnamed(socket: SOCKET, addr: &SockAddr) -> io::Result<()> {
	let family = addr.family();
	let len = match family {
		AF_INET => size_of::<SOCKADDR_IN>(),
		AF_INET6 => size_of::<SOCKADDR_IN6>(),
		// The length is what makes a unix address unnamed: a bare family says
		// the `sun_path` is not there.
		AF_UNIX => size_of::<ADDRESS_FAMILY>(),
		other => {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				format!("a connect cannot bind a socket of address family {other}"),
			));
		}
	};
	let mut storage: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
	storage.ss_family = family;
	// SAFETY: `storage` is zeroed but for the family, and `len` says how much of
	// it the call reads.
	if unsafe {
		bind(
			socket,
			&storage as *const SOCKADDR_STORAGE as *const SOCKADDR,
			len as i32,
		)
	} == 0
	{
		return Ok(());
	}
	let error = unsafe { WSAGetLastError() };
	// A socket the caller bound already is bound: `WSAEINVAL` is what binding
	// one twice reports.
	if error == WSAEINVAL {
		return Ok(());
	}
	Err(io::Error::from_raw_os_error(error))
}

/// Turns a completed `ConnectEx` into the operation's result. The socket is a
/// plain descriptor until it takes up the connection's context — the same step
/// a completed `AcceptEx` needs — so `SO_UPDATE_CONNECT_CONTEXT` runs before the
/// operation reports its completion.
fn connected(socket: SOCKET) -> io::Result<Response> {
	// SAFETY: the option takes no value, which is what the null pointer and the
	// zero length say; the socket is the one the connect ran on.
	let rc = unsafe {
		setsockopt(
			socket,
			SOL_SOCKET,
			SO_UPDATE_CONNECT_CONTEXT,
			std::ptr::null(),
			0,
		)
	};
	if rc != 0 {
		return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
	}
	Ok(Response::Done)
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

/// The AFD endpoint handle behind `socket`, which an `IOCTL_AFD_POLL` runs on.
/// A socket no layered provider wraps answers with its own handle.
fn base_socket(socket: SOCKET) -> SOCKET {
	let mut base: SOCKET = INVALID_SOCKET;
	let mut bytes = 0u32;
	// SAFETY: the control writes one `SOCKET` into `base`, which is the size
	// `bytes` reports.
	let rc = unsafe {
		WSAIoctl(
			socket,
			SIO_BASE_HANDLE,
			std::ptr::null(),
			0,
			&mut base as *mut SOCKET as *mut core::ffi::c_void,
			size_of::<SOCKET>() as u32,
			&mut bytes,
			std::ptr::null_mut(),
			None,
		)
	};
	if rc == SOCKET_ERROR || base == INVALID_SOCKET {
		return socket;
	}
	base
}

impl<T> Driver<T> for Iocp<T> {
	fn submit(&mut self, fd: BorrowedDescriptor<'_>, op: Op, data: T) -> Result<u64> {
		match op {
			Op::Read { offset, buf } => self.read_or_write(fd, true, buf, offset, data),
			Op::Write { offset, buf } => self.read_or_write(fd, false, buf, offset, data),
			Op::Fsync => Ok(self.in_thread(
				as_handle(fd)?,
				|handle| {
					// SAFETY: a plain Win32 call on a handle the caller holds.
					if unsafe { FlushFileBuffers(handle) } == 0 {
						unsafe { GetLastError() as i32 }
					} else {
						0
					}
				},
				data,
			)),
			Op::Flush => Ok(self.in_thread(as_handle(fd)?, |_| 0, data)),
			Op::Allocate { offset, len } => Ok(self.in_thread(
				as_handle(fd)?,
				move |handle| allocate(handle, offset, len),
				data,
			)),
			Op::Punch { offset, len } => self
				.punch(as_handle(fd)?, offset, len, data)
				.map_err(Error::from),
			Op::Recv { buf } => self.transfer(as_socket(fd)?, buf, false, data),
			Op::Send { buf } => self.transfer(as_socket(fd)?, buf, true, data),
			Op::Accept => self.accept(as_socket(fd)?, data).map_err(Error::from),
			Op::Connect { addr } => self
				.connect(as_socket(fd)?, &addr, data)
				.map_err(Error::from),
			Op::Readable => match fd {
				BorrowedDescriptor::Socket(socket) => self.afd_poll(
					socket.as_raw_socket() as SOCKET,
					AFD_POLL_RECEIVE
						| AFD_POLL_RECEIVE_EXPEDITED
						| AFD_POLL_ACCEPT
						| AFD_POLL_DISCONNECT
						| AFD_POLL_ABORT
						| AFD_POLL_LOCAL_CLOSE
						| AFD_POLL_CONNECT_FAIL,
					data,
				),
				BorrowedDescriptor::Handle(handle) => {
					self.wait_handle(handle.as_raw_handle(), data)
				}
			},
			Op::Writable => match fd {
				BorrowedDescriptor::Socket(socket) => self.afd_poll(
					socket.as_raw_socket() as SOCKET,
					AFD_POLL_SEND | AFD_POLL_ABORT | AFD_POLL_LOCAL_CLOSE | AFD_POLL_CONNECT_FAIL,
					data,
				),
				BorrowedDescriptor::Handle(handle) => {
					self.wait_handle(handle.as_raw_handle(), data)
				}
			},
			// A half-close is a `DISCONNECT` the driver reports as readable, so a
			// wait for the peer gone listens for the aborted connection alone.
			Op::Hangup => self.afd_poll(
				as_socket(fd)?,
				AFD_POLL_ABORT | AFD_POLL_LOCAL_CLOSE | AFD_POLL_CONNECT_FAIL,
				data,
			),
		}
	}

	fn get_mut(&mut self, id: u64) -> Option<&mut T> {
		self.active
			.get_mut(DefaultKey::from(KeyData::from_ffi(id)))
			.map(|active| &mut active.data)
	}

	fn cancel(&mut self, id: u64) {
		// The id is the key the operation is filed under: one the port never
		// handed out, or one whose slot has been taken again since, names nothing.
		let key = DefaultKey::from(KeyData::from_ffi(id));
		let Some(active) = self.active.get_mut(key) else {
			return;
		};
		Self::cancel_active(&self.port, active);
	}

	fn cancel_all(&mut self) {
		for active in self.active.values_mut() {
			Self::cancel_active(&self.port, active);
		}
	}

	fn is_idle(&self) -> bool {
		self.active.is_empty()
	}

	fn poll(
		&mut self,
		timeout: Option<Duration>,
		mut complete: impl FnMut(u64, T, Result<Response>),
	) -> Result<()> {
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
				self.port.notified.store(false, Ordering::Release);
				continue;
			}
			// SAFETY: the pointer names a box we inserted into `active` at
			// submit time and that is alive until this collection.
			let ov = entry.lpOverlapped as *mut Overlapped;
			let (key, result) = unsafe {
				let key = (*ov).key;
				let result = if (*ov).submit_error != 0 {
					Err(io::Error::from_raw_os_error((*ov).submit_error))
				} else if (*ov).inner.Internal == STATUS_END_OF_FILE {
					// A read beginning at or past the end of a file: a `read`
					// moves nothing there, and reporting that is what a unix
					// file does too.
					Ok(0)
				} else if (*ov).inner.Internal == 0 {
					Ok((*ov).inner.InternalHigh)
				} else {
					Err(io::Error::from_raw_os_error(ntstatus_errno(
						(*ov).inner.Internal,
					)))
				};
				(key, result)
			};
			// The kernel is done with what it held for this operation, whether
			// or not the reactor still waits for one of its own. The box goes
			// back to the free list with its allocation kept.
			let Some(active) = self.active.remove(key) else {
				// Nothing is left to report this packet with: the operation's
				// entry went when the port collected it.
				continue;
			};
			let bytes = active.bytes;
			let data = active.data;
			let (accept, connect) = {
				self.free_overlapped.push(active.ov);
				(active.accept, active.connect)
			};
			let response = match (result, accept, connect) {
				// An accept reports the socket, not the bytes it moved.
				(Ok(_), Some(pending), _) => accepted(pending),
				// A connect reports nothing it moved: the socket is the result, once
				// it takes up the connection's context.
				(Ok(_), None, Some(socket)) => connected(socket),
				(result, pending, _) => {
					// A failed accept closes the socket it created.
					drop(pending);
					result.map(|moved| match bytes {
						true => Response::Count(moved),
						false => Response::Done,
					})
				}
			};
			complete(key.data().as_ffi(), data, response.map_err(landed));
		}
		Ok(())
	}

	fn waker(&self) -> Waker {
		Waker(WakerInner::Iocp(Arc::clone(&self.port)))
	}
}

impl<T> Drop for Iocp<T> {
	fn drop(&mut self) {
		// A packet may still be on its way for an operation the reactor let go
		// of, and the box it points at must not go before it arrives. Ask the
		// kernel to stop what it still holds, then collect what it returns.
		for active in self.active.values_mut() {
			Self::cancel_active(&self.port, active);
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

#[cfg(test)]
mod tests {
	use std::io;

	use assert2::assert;
	use rstest::rstest;

	use super::{Config, ERROR_OPERATION_ABORTED, Iocp, ntstatus_errno};

	#[test]
	fn ntstatus_errno_maps_status_cancelled_to_operation_aborted() {
		assert!(ntstatus_errno(0xC000_0120) == ERROR_OPERATION_ABORTED as i32);
	}

	#[rstest]
	#[case::iopoll(Config { iopoll: true, ..Config::default() })]
	#[case::sqpoll(Config { sqpoll: true, ..Config::default() })]
	fn iocp_rejects_iopoll_and_sqpoll_configs(#[case] config: Config) {
		assert!(let Err(error) = Iocp::<()>::new(&config));
		assert!(error.kind() == io::ErrorKind::Unsupported);
	}
}
