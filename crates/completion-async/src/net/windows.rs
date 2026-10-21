use std::io;
use std::mem::size_of;
use std::net::Shutdown;
use std::os::windows::io::AsRawSocket;

use windows_sys::Win32::Networking::WinSock::{
	SD_BOTH, SD_RECEIVE, SD_SEND, SO_ERROR, SOCKET, SOL_SOCKET, WSAGetLastError, getsockopt,
	shutdown as winsock_shutdown,
};

use completion_backend::BorrowedDescriptor;

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
