//! A pipe: one end is waited on, the other is raised.

use std::io;

use rustix::io::FdFlags;

use super::Inner;

pub(super) fn new() -> io::Result<Inner> {
	let (read, write) = rustix::pipe::pipe()?;
	// Apple has no `pipe2`, so the flags go on afterwards.
	rustix::io::ioctl_fionbio(&read, true)?;
	rustix::io::ioctl_fionbio(&write, true)?;
	rustix::io::fcntl_setfd(&read, FdFlags::CLOEXEC)?;
	rustix::io::fcntl_setfd(&write, FdFlags::CLOEXEC)?;
	Ok(Inner { read, write })
}
