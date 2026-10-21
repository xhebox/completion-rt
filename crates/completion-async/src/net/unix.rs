use std::io;
use std::net::Shutdown;

use completion_backend::BorrowedDescriptor;

pub(super) fn shutdown(descriptor: BorrowedDescriptor<'_>, how: Shutdown) -> io::Result<()> {
	let how = match how {
		Shutdown::Read => rustix::net::Shutdown::Read,
		Shutdown::Write => rustix::net::Shutdown::Write,
		Shutdown::Both => rustix::net::Shutdown::Both,
	};
	rustix::net::shutdown(descriptor, how).map_err(io::Error::from)
}

pub(super) fn take_error(descriptor: BorrowedDescriptor<'_>) -> io::Result<Option<io::Error>> {
	match rustix::net::sockopt::socket_error(descriptor)? {
		Ok(()) => Ok(None),
		Err(errno) => Ok(Some(errno.into())),
	}
}
