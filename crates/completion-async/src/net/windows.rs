use std::io;
use std::mem::size_of;
use std::net::Shutdown;
use std::os::windows::io::{AsRawSocket, FromRawHandle, OwnedHandle};
use std::sync::atomic::{AtomicU64, Ordering};

use windows_sys::Win32::Foundation::{GENERIC_WRITE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Networking::WinSock::{
	SD_BOTH, SD_RECEIVE, SD_SEND, SO_ERROR, SOCKET, SOL_SOCKET, WSAGetLastError, getsockopt,
	shutdown as winsock_shutdown,
};
use windows_sys::Win32::Storage::FileSystem::{
	CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, OPEN_EXISTING,
	PIPE_ACCESS_INBOUND,
};
use windows_sys::Win32::System::Pipes::{
	CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
};
use windows_sys::Win32::System::Threading::GetCurrentProcessId;

use completion_backend::{BorrowedDescriptor, OwnedDescriptor};

/// How many bytes a pipe holds before a write waits.
const PIPE_BUFFER: u32 = 64 << 10;

/// The serial number of the next pipe this process names: the pipe namespace is
/// machine-wide, so a name carries both the process and this number.
static NEXT_PIPE: AtomicU64 = AtomicU64::new(0);

/// Creates a connected pair of pipes whose ends a completion port can own.
///
/// Windows has no anonymous pipe a completion port can own: `CreatePipe` hands
/// back synchronous handles. This pair is a named pipe instead, opened for
/// overlapped I/O on both ends.
pub(super) fn pipe() -> io::Result<(OwnedDescriptor, OwnedDescriptor)> {
	let name = pipe_name();
	// The first end is the server, which takes the inbound direction: the other
	// end is the one that writes what this one reads.
	// SAFETY: the call is plain Win32, and `name` is the NUL-terminated name a
	// pipe takes.
	let reader = unsafe {
		CreateNamedPipeW(
			name.as_ptr(),
			PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
			PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
			1,
			PIPE_BUFFER,
			PIPE_BUFFER,
			0,
			std::ptr::null(),
		)
	};
	if reader == INVALID_HANDLE_VALUE {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: the call handed this process the handle, and nothing else owns it.
	let reader = unsafe { OwnedHandle::from_raw_handle(reader) };
	// The instance is already waiting for a client, so this returns the write
	// end rather than waiting for one.
	// SAFETY: as above, and `name` is the same NUL-terminated name.
	let writer = unsafe {
		CreateFileW(
			name.as_ptr(),
			GENERIC_WRITE,
			0,
			std::ptr::null(),
			OPEN_EXISTING,
			FILE_FLAG_OVERLAPPED,
			std::ptr::null_mut(),
		)
	};
	if writer == INVALID_HANDLE_VALUE {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: as above.
	let writer = unsafe { OwnedHandle::from_raw_handle(writer) };
	Ok((OwnedDescriptor::from(reader), OwnedDescriptor::from(writer)))
}

/// A pipe name no other pipe has taken.
fn pipe_name() -> Vec<u16> {
	let serial = NEXT_PIPE.fetch_add(1, Ordering::Relaxed);
	// SAFETY: a plain Win32 call that reads no memory.
	let process = unsafe { GetCurrentProcessId() };
	format!(r"\\.\pipe\LOCAL\completion-rt-{process}-{serial}")
		.encode_utf16()
		.chain(std::iter::once(0))
		.collect()
}

pub(super) fn shutdown(descriptor: BorrowedDescriptor<'_>, how: Shutdown) -> io::Result<()> {
	let how = match how {
		Shutdown::Read => SD_RECEIVE,
		Shutdown::Write => SD_SEND,
		Shutdown::Both => SD_BOTH,
	};
	if unsafe { winsock_shutdown(socket(descriptor)?, how) } != 0 {
		return Err(last_error());
	}
	Ok(())
}

pub(super) fn take_error(descriptor: BorrowedDescriptor<'_>) -> io::Result<Option<io::Error>> {
	let mut value: i32 = 0;
	let mut len = size_of::<i32>() as i32;
	let socket = socket(descriptor)?;
	if unsafe {
		getsockopt(
			socket,
			SOL_SOCKET,
			SO_ERROR,
			&mut value as *mut i32 as *mut u8,
			&mut len,
		)
	} != 0
	{
		return Err(last_error());
	}
	Ok((value != 0).then(|| io::Error::from_raw_os_error(value)))
}

fn socket(descriptor: BorrowedDescriptor<'_>) -> io::Result<SOCKET> {
	match descriptor {
		BorrowedDescriptor::Socket(socket) => Ok(socket.as_raw_socket() as SOCKET),
		BorrowedDescriptor::Handle(_) => Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"a socket call on a non-socket descriptor",
		)),
	}
}

/// Winsock does not set `errno`: `WSAGetLastError` is where a failed call
/// reports.
fn last_error() -> io::Error {
	io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}
