//! The Windows side of the portable backend: a descriptor is a handle or a
//! socket, and the poller watches only sockets — every call on a handle runs
//! on the pool, and a socket takes `recv`/`send` where a unix descriptor takes
//! `read`/`write`.

use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::windows::io::{AsRawHandle, AsRawSocket, BorrowedSocket, RawSocket};
use std::slice;

use socket2::SockRef;
use windows_sys::Win32::Foundation::{ERROR_HANDLE_EOF, ERROR_IO_PENDING, GetLastError, HANDLE};
use windows_sys::Win32::Storage::FileSystem::{
	FILE_ALLOCATION_INFO, FILE_END_OF_FILE_INFO, FileAllocationInfo, FileEndOfFileInfo,
	FlushFileBuffers, GetFileSizeEx, ReadFile, SetFileInformationByHandle, WriteFile,
};
use windows_sys::Win32::System::IO::{DeviceIoControl, GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::Ioctl::{FILE_ZERO_DATA_INFORMATION, FSCTL_SET_ZERO_DATA};

use super::{Kind, Op, Progress, Route, counted, first_buffer};
use crate::backend::Response;
use crate::desc::BorrowedDescriptor;
use crate::memory::Resolved;

/// A descriptor as the platform names one: the poller watches only sockets, so
/// a handle and a socket are told apart before anything else.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum Raw {
	/// A kernel handle, kept as a number: handles are process-wide, and the
	/// reactor holds the descriptor open until the pool job naming it lands.
	Handle(usize),
	Socket(RawSocket),
}

impl Raw {
	/// The handle a file call runs on.
	fn handle(self) -> io::Result<HANDLE> {
		match self {
			Raw::Handle(handle) => Ok(handle as HANDLE),
			Raw::Socket(_) => Err(unsupported("a file call on a socket descriptor")),
		}
	}
}

/// The socket a poller registration and a socket call name.
pub(super) type Borrowed<'a> = BorrowedSocket<'a>;

pub(super) fn raw(fd: BorrowedDescriptor<'_>) -> Raw {
	match fd {
		BorrowedDescriptor::Handle(handle) => Raw::Handle(handle.as_raw_handle() as usize),
		BorrowedDescriptor::Socket(socket) => Raw::Socket(socket.as_raw_socket()),
	}
}

/// Borrows `raw` for as long as the reactor holds it: every caller either
/// deregisters the source or lets the call it makes return before the
/// descriptor goes.
pub(super) fn borrow(raw: Raw) -> io::Result<Borrowed<'static>> {
	match raw {
		// SAFETY: the socket stays open while the reactor holds it.
		Raw::Socket(socket) => Ok(unsafe { BorrowedSocket::borrow_raw(socket) }),
		// Only a socket is a readiness source, so only one reaches a socket call.
		Raw::Handle(_) => Err(unsupported("a socket call on a non-socket descriptor")),
	}
}

/// Where an operation on `raw` runs. Only a socket is a readiness source here,
/// so every call on a handle is a pool job.
pub(super) fn route(raw: Raw, op: &Op) -> Route {
	match (raw, op) {
		// The offset in the `OVERLAPPED` is what makes a file call positional,
		// and a handle opened for overlapped I/O and a synchronous one both
		// take it.
		(Raw::Handle(_), Op::Read { .. }) => Route::Pool(Kind::Read),
		(Raw::Handle(_), Op::Write { .. }) => Route::Pool(Kind::Write),
		(Raw::Handle(_), Op::Fsync) => Route::Pool(Kind::Fsync),
		(Raw::Handle(_), Op::Flush) => Route::Pool(Kind::Flush),
		(Raw::Handle(_), Op::Allocate { offset, len }) => Route::Pool(Kind::Allocate {
			offset: *offset,
			len: *len,
		}),
		(Raw::Handle(_), Op::Punch { offset, len }) => Route::Pool(Kind::Punch {
			offset: *offset,
			len: *len,
		}),
		// A socket call and a wait for readiness differ only in what the
		// operation says to do once the poller reports the descriptor.
		(Raw::Socket(_), Op::Read { .. })
		| (Raw::Socket(_), Op::Write { .. })
		| (Raw::Socket(_), Op::Recv { .. })
		| (Raw::Socket(_), Op::Accept)
		| (Raw::Socket(_), Op::Readable)
		| (Raw::Socket(_), Op::Writable) => Route::Watch,
		(Raw::Socket(_), Op::Send { fds, .. }) if fds.is_empty() => Route::Watch,
		(Raw::Socket(_), Op::Send { .. }) => {
			Route::Unsupported("this backend cannot send descriptors")
		}
		(_, Op::RecvWithFds { .. }) => {
			Route::Unsupported("this backend cannot receive descriptors")
		}
		(_, Op::Hangup) => Route::Unsupported("this backend cannot wait for a hangup"),
		_ => Route::Unsupported("this operation is not implemented on this backend"),
	}
}

/// Runs a file call on the pool.
pub(super) fn run_file(
	raw: Raw,
	fdoff: u64,
	kind: Kind,
	buffer: Option<Resolved>,
) -> io::Result<Response> {
	let handle = raw.handle()?;
	match kind {
		Kind::Fsync => {
			// SAFETY: a plain Win32 call on a handle the reactor holds until the job lands.
			if unsafe { FlushFileBuffers(handle) } == 0 {
				return Err(io::Error::last_os_error());
			}
			Ok(Response::Done)
		}
		// A flush waits for nothing: the writes the handle took are the file's.
		Kind::Flush => Ok(Response::Done),
		Kind::Allocate { offset, len } => allocate(handle, offset, len).map(|()| Response::Done),
		Kind::Punch { offset, len } => punch(handle, offset, len).map(|()| Response::Done),
		Kind::Read | Kind::Write => {
			let Some((ptr, len)) = first_buffer(buffer) else {
				return Ok(Response::Count(0));
			};
			let read = matches!(kind, Kind::Read);
			match transfer(handle, fdoff, ptr, len as u32, read) {
				Ok(moved) => Ok(Response::Count(moved)),
				// A read beginning at or past the end of the file moves
				// nothing, as a unix file reports too.
				Err(error) if read && error.raw_os_error() == Some(ERROR_HANDLE_EOF as i32) => {
					Ok(Response::Count(0))
				}
				Err(error) => Err(error),
			}
		}
	}
}

/// Calls the operation the poller reported ready that only a Winsock socket
/// takes here: `read`/`write` on a socket are `recv`/`send`.
pub(super) fn run_op(raw: Raw, op: &mut Op, buffer: Option<Resolved>) -> io::Result<Progress> {
	let Some((ptr, len)) = first_buffer(buffer) else {
		return Ok(Progress::Done(Response::Count(0)));
	};
	let source = borrow(raw)?;
	let socket = SockRef::from(&source);
	match op {
		// SAFETY: the reactor holds the memory behind `ptr` until the operation
		// lands, and the call is the only mover of the range while the slice
		// stands: `recv` only writes it and `send` only reads it.
		Op::Read { .. } => counted(
			socket.recv(unsafe { slice::from_raw_parts_mut(ptr.cast::<MaybeUninit<u8>>(), len) }),
		),
		Op::Write { .. } => counted(socket.send(unsafe { slice::from_raw_parts(ptr, len) })),
		_ => unreachable!("only a read and a write reach the poller through this call"),
	}
}

/// One positional read or write, through an `OVERLAPPED` that names `fdoff`.
fn transfer(handle: HANDLE, fdoff: u64, ptr: *mut u8, len: u32, read: bool) -> io::Result<usize> {
	run_overlapped(handle, fdoff, |overlapped| {
		// SAFETY: the reactor holds the memory behind `ptr` until the job lands, and the call copies through it until the completion.
		unsafe {
			if read {
				ReadFile(handle, ptr.cast(), len, std::ptr::null_mut(), overlapped)
			} else {
				WriteFile(handle, ptr.cast(), len, std::ptr::null_mut(), overlapped)
			}
		}
	})
}

/// Grows the file to the range's end, so the space it needs is reserved; the
/// call has no overlapped form.
///
/// Setting the end of file shrinks as readily as it grows, and a reservation
/// below the current length is what the range is already backed by; allocate
/// only grows, so the current length floors both.
fn allocate(handle: HANDLE, offset: u64, len: u64) -> io::Result<()> {
	let end = offset.saturating_add(len);
	// SAFETY: standard Win32 calls on handles the caller holds; both info
	// structs outlive their calls.
	let mut current = 0i64;
	if unsafe { GetFileSizeEx(handle, &mut current) } == 0 {
		return Err(io::Error::last_os_error());
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
		return Err(io::Error::last_os_error());
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
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

/// Cuts a hole: the range reads back as zeros and the file keeps its length,
/// from the same `FSCTL_SET_ZERO_DATA` the IOCP backend sends.
fn punch(handle: HANDLE, offset: u64, len: u64) -> io::Result<()> {
	let end = offset
		.checked_add(len)
		.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
	let range = FILE_ZERO_DATA_INFORMATION {
		FileOffset: offset as i64,
		BeyondFinalZero: end as i64,
	};
	run_overlapped(handle, 0, |overlapped| {
		// SAFETY: the range outlives the wait below, which is when the kernel reads it; the call itself only starts the zeroing.
		unsafe {
			DeviceIoControl(
				handle,
				FSCTL_SET_ZERO_DATA,
				&range as *const _ as *const core::ffi::c_void,
				size_of::<FILE_ZERO_DATA_INFORMATION>() as u32,
				std::ptr::null_mut(),
				0,
				std::ptr::null_mut(),
				overlapped,
			)
		}
	})
	.map(|_| ())
}

/// Runs `call` through an `OVERLAPPED` naming `fdoff`, waiting for it when it
/// goes asynchronous, and reports the bytes that moved. The same wait serves a
/// handle opened for overlapped I/O and a synchronous one: the first completes
/// the call later, and the second before it returns.
fn run_overlapped(
	handle: HANDLE,
	fdoff: u64,
	call: impl FnOnce(*mut OVERLAPPED) -> i32,
) -> io::Result<usize> {
	let mut overlapped = OVERLAPPED {
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
	if call(&mut overlapped) == 0 {
		let error = unsafe { GetLastError() };
		// A call that completes later reports it here and is waited for below.
		if error != ERROR_IO_PENDING {
			return Err(io::Error::from_raw_os_error(error as i32));
		}
	}
	let mut moved = 0u32;
	// SAFETY: `overlapped` is this call's status block, which the kernel fills
	// whether the call completed at once or later; a null `hEvent` makes the
	// wait one on the handle.
	if unsafe { GetOverlappedResult(handle, &overlapped, &mut moved, 1) } == 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(moved as usize)
}

fn unsupported(reason: &'static str) -> io::Error {
	io::Error::new(io::ErrorKind::Unsupported, reason)
}
