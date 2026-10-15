//! The operations a reactor runs, and the requests they resolve to.

use std::io;

use crate::desc::{OwnedDescriptor, RawHandle};
use crate::memory::{Extents, IoVecs};

use super::space::Space;

pub(super) enum Request {
	Read {
		fd: RawHandle,
		fdoff: u64,
		buffers: IoVecs,
	},
	Write {
		fd: RawHandle,
		fdoff: u64,
		buffers: IoVecs,
	},
	Fsync {
		fd: RawHandle,
	},
	Flush {
		fd: RawHandle,
	},
	Allocate {
		fd: RawHandle,
		offset: u64,
		len: u64,
	},
	Punch {
		fd: RawHandle,
		offset: u64,
		len: u64,
	},
	Recv {
		fd: RawHandle,
		buffers: IoVecs,
	},
	RecvWithFds {
		fd: RawHandle,
		buffers: IoVecs,
	},
	Send {
		fd: RawHandle,
		buffers: IoVecs,
	},
	SendWithFds {
		fd: RawHandle,
		buffers: IoVecs,
		fds: Vec<OwnedDescriptor>,
	},
	Accept {
		fd: RawHandle,
	},
	/// Waits for `fd` to become readable.
	///
	/// A completion resolves once, so a caller that wants readiness again
	/// submits another one — this is not a standing registration.
	Readable {
		fd: RawHandle,
	},
	Writable {
		fd: RawHandle,
	},
	/// Waits for the peer to be gone, or the socket to fail.
	Hangup {
		fd: RawHandle,
	},
}

impl Request {
	/// Control buffers are backend-owned; only payload buffers count as caller memory.
	pub(super) fn names_memory(&self) -> bool {
		match self {
			Request::Read { .. }
			| Request::Write { .. }
			| Request::Recv { .. }
			| Request::RecvWithFds { .. }
			| Request::Send { .. } => true,
			// Only a send carrying descriptors and no bytes touches nothing of
			// the caller's: the payload is what names memory.
			Request::SendWithFds { buffers, .. } => buffers.iter().any(|buffer| buffer.len() != 0),
			Request::Fsync { .. }
			| Request::Flush { .. }
			| Request::Allocate { .. }
			| Request::Punch { .. }
			| Request::Accept { .. }
			| Request::Readable { .. }
			| Request::Writable { .. }
			| Request::Hangup { .. } => false,
		}
	}

	pub(super) fn kind(&self) -> &'static str {
		match self {
			Request::Read { .. } => "read",
			Request::Write { .. } => "write",
			Request::Fsync { .. } => "fsync",
			Request::Flush { .. } => "flush",
			Request::Allocate { .. } => "allocate",
			Request::Punch { .. } => "punch",
			Request::Recv { .. } => "recv",
			Request::RecvWithFds { .. } => "recv_with_fds",
			Request::Send { .. } => "send",
			Request::SendWithFds { .. } => "send_with_fds",
			Request::Accept { .. } => "accept",
			Request::Readable { .. } => "readable",
			Request::Writable { .. } => "writable",
			Request::Hangup { .. } => "hangup",
		}
	}
}

/// An operation named by its arguments; [`Reactor::submit`] supplies the descriptor from its handle.
///
/// A transfer names its memory as `extents` of a [`Space`].
pub enum Op {
	Read {
		offset: u64,
		space: Space,
		extents: Extents,
	},
	Write {
		offset: u64,
		space: Space,
		extents: Extents,
	},
	Fsync,
	Flush,
	Allocate {
		offset: u64,
		len: u64,
	},
	Punch {
		offset: u64,
		len: u64,
	},
	Recv {
		space: Space,
		extents: Extents,
	},
	RecvWithFds {
		space: Space,
		extents: Extents,
	},
	Send {
		space: Space,
		extents: Extents,
	},
	SendWithFds {
		space: Space,
		extents: Extents,
		fds: Vec<OwnedDescriptor>,
	},
	Accept,
	Readable,
	Writable,
	/// Resolves once the peer is gone or the socket has failed; a peer that
	/// only shut its write side has not hung up.
	Hangup,
}

impl Op {
	pub(super) fn space(&self) -> Option<&Space> {
		match self {
			Op::Read { space, .. }
			| Op::Write { space, .. }
			| Op::Recv { space, .. }
			| Op::RecvWithFds { space, .. }
			| Op::Send { space, .. }
			| Op::SendWithFds { space, .. } => Some(space),
			Op::Fsync
			| Op::Flush
			| Op::Allocate { .. }
			| Op::Punch { .. }
			| Op::Accept
			| Op::Readable
			| Op::Writable
			| Op::Hangup => None,
		}
	}

	/// The request the backend gets, its extents resolved against the space.
	pub(super) fn into_request(self, fd: RawHandle) -> io::Result<Request> {
		Ok(match self {
			Op::Read {
				offset,
				space,
				extents,
			} => Request::Read {
				fd,
				fdoff: offset,
				buffers: space.resolve(&extents)?,
			},
			Op::Write {
				offset,
				space,
				extents,
			} => Request::Write {
				fd,
				fdoff: offset,
				buffers: space.resolve(&extents)?,
			},
			Op::Fsync => Request::Fsync { fd },
			Op::Flush => Request::Flush { fd },
			Op::Allocate { offset, len } => Request::Allocate { fd, offset, len },
			Op::Punch { offset, len } => Request::Punch { fd, offset, len },
			Op::Recv { space, extents } => Request::Recv {
				fd,
				buffers: space.resolve(&extents)?,
			},
			Op::RecvWithFds { space, extents } => Request::RecvWithFds {
				fd,
				buffers: space.resolve(&extents)?,
			},
			Op::Send { space, extents } => Request::Send {
				fd,
				buffers: space.resolve(&extents)?,
			},
			Op::SendWithFds {
				space,
				extents,
				fds,
			} => Request::SendWithFds {
				fd,
				buffers: space.resolve(&extents)?,
				fds,
			},
			Op::Accept => Request::Accept { fd },
			Op::Readable => Request::Readable { fd },
			Op::Writable => Request::Writable { fd },
			Op::Hangup => Request::Hangup { fd },
		})
	}
}
