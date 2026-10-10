//! An eventfd: one counter, waited on and raised alike.

use std::io;

use rustix::event::EventfdFlags;

use super::Inner;

pub(super) fn new() -> io::Result<Inner> {
	let read = rustix::event::eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK)?;
	let write = read.try_clone()?;
	Ok(Inner { read, write })
}
