//! Socket operations: the receive and the send.

use std::sync::Arc;

use crate::flow::{Accept, Kind, Receive, Transfer, whole};
use crate::io::Facade;
use crate::{AsDescriptor, Extent, Extents, Memory, OwnedDescriptor, OwnedSocket, Submitter};

/// A resource a socket facade names: a socket, not a file.
///
/// Receiving and sending are only meaningful on a socket, which is what this
/// marks. It is not sealed: a type of the caller's own is a socket once the
/// caller implements this and [`AsDescriptor`] for it, and that type owns the
/// socket — an operation holds the handle, not the number, so an impl over a
/// bare descriptor would leave it open to be closed and handed out again
/// underneath the operation. `fs::File`, which is also a resource, is
/// deliberately not one.
///
/// A socket that is a byte stream is also a [`StreamHandle`]; a datagram socket
/// is this alone.
pub trait SocketHandle: AsDescriptor + Send + Sync + 'static {}

/// A socket pair that carries a byte stream, with the loops a stream needs and
/// a datagram has no use for.
///
/// A stream keeps no messages: a receive may take half of what a send made, so
/// the loops over the calls are what fill a memory and empty one. A datagram
/// socket takes a message per call and sends one — there is nothing to
/// continue, so none of `recv_exact`, `send_all` and their like is on it.
pub trait StreamHandle: SocketHandle {}

/// A socket a connection can be taken out of.
///
/// The stream the accept reports is named by [`Stream`](Self::Stream): the
/// listener says what its connections are, and the caller gets a typed facade
/// for them. Not a [`SocketHandle`]: a listener is only listened on, and
/// receiving from it is a mistake the type should not allow.
pub trait ListenerHandle: AsDescriptor + Send + Sync + 'static {
	type Stream: StreamHandle + From<OwnedSocket>;
}

impl<S: SocketHandle> Facade<S> {
	/// Receives into the whole of `mem`, in one call: a short count is what
	/// arrived, not an error. [`recv_exact`](Facade::recv_exact) loops.
	pub fn recv(&self, mem: Arc<dyn Memory>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::Recv, mem, extents)
	}

	/// Receives into the named ranges of `mem`, in one call.
	pub fn recv_vectored(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
	) -> Transfer<S> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::Recv, mem, extents)
	}

	/// Receives into the whole of `mem`, taking the descriptors the control
	/// messages carry with the bytes.
	pub fn recv_with_fds(&self, mem: Arc<dyn Memory>) -> Receive<S> {
		let extents = whole(&*mem);
		Receive::new(self, Kind::Recv, mem, extents)
	}

	/// Sends the whole of `mem`, in one call: a short count is what the call
	/// took, not an error. [`send_all`](Facade::send_all) loops.
	pub fn send(&self, mem: Arc<dyn Memory>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::Send, mem, extents)
	}

	/// Sends the named ranges of `mem`, in one call.
	pub fn send_vectored(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
	) -> Transfer<S> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::Send, mem, extents)
	}

	/// Sends the whole of `mem`, carrying `fds` with the bytes.
	///
	/// One call carries the descriptors; see
	/// [`send_all_with_fds`](Facade::send_all_with_fds) for a send that loops.
	pub fn send_with_fds(&self, mem: Arc<dyn Memory>, fds: Vec<OwnedDescriptor>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::with_fds(self, Kind::Send, mem, extents, fds)
	}
}

impl<S: StreamHandle> Facade<S> {
	/// Receives into the whole of `mem`, over as many calls as it takes to fill
	/// it.
	///
	/// [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) when the peer closes
	/// first.
	pub fn recv_exact(&self, mem: Arc<dyn Memory>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::RecvAll, mem, extents)
	}

	/// Receives into the named ranges of `mem`, over as many calls as it takes
	/// to fill them.
	pub fn recv_exact_vectored(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
	) -> Transfer<S> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::RecvAll, mem, extents)
	}

	/// [`recv_with_fds`](Facade::recv_with_fds) over as many calls as it takes
	/// to fill `mem`, gathering the descriptors every call brings.
	pub fn recv_exact_with_fds(&self, mem: Arc<dyn Memory>) -> Receive<S> {
		let extents = whole(&*mem);
		Receive::new(self, Kind::RecvAll, mem, extents)
	}

	/// Sends the whole of `mem`, over as many calls as it takes.
	///
	/// [`WriteZero`](std::io::ErrorKind::WriteZero) when a call takes nothing.
	pub fn send_all(&self, mem: Arc<dyn Memory>) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::new(self, Kind::SendAll, mem, extents)
	}

	/// Sends the named ranges of `mem`, over as many calls as it takes.
	pub fn send_all_vectored(
		&self,
		mem: Arc<dyn Memory>,
		ranges: impl IntoIterator<Item = Extent>,
	) -> Transfer<S> {
		let extents: Extents = ranges.into_iter().collect();
		Transfer::new(self, Kind::SendAll, mem, extents)
	}

	/// Sends the whole of `mem` over as many calls as it takes, carrying `fds`
	/// with the first call's bytes: descriptors travel with bytes, so the one
	/// call the kernel took them in is the one they are announced in.
	pub fn send_all_with_fds(
		&self,
		mem: Arc<dyn Memory>,
		fds: Vec<OwnedDescriptor>,
	) -> Transfer<S> {
		let extents = whole(&*mem);
		Transfer::with_fds(self, Kind::SendAll, mem, extents, fds)
	}
}

impl<S: ListenerHandle> Facade<S> {
	/// Takes the next connection in: the stream it made, in a facade of its own.
	pub fn accept(&self) -> Accept<S> {
		Accept::new(self, Submitter::accept)
	}
}

impl SocketHandle for std::net::TcpStream {}
impl StreamHandle for std::net::TcpStream {}

impl SocketHandle for std::net::UdpSocket {}

impl ListenerHandle for std::net::TcpListener {
	type Stream = std::net::TcpStream;
}

#[cfg(unix)]
mod platform {
	use std::os::unix::net::{UnixDatagram, UnixListener, UnixStream};

	use super::{ListenerHandle, SocketHandle, StreamHandle};

	impl SocketHandle for UnixStream {}
	impl StreamHandle for UnixStream {}

	impl SocketHandle for UnixDatagram {}

	impl ListenerHandle for UnixListener {
		type Stream = UnixStream;
	}
}
