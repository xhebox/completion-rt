//! What a resource is, as far as its facade goes.

use completion_rt::{AsDescriptor, OwnedSocket};

/// A resource a [`Facade`](crate::Facade) wraps, and the one [`Kind`] that
/// decides its operations. Implementable by caller-owned types.
///
/// The kind is what the type knows. A [`File`](std::fs::File) may still be a
/// FIFO, and an [`OwnedFd`](std::os::fd::OwnedFd) a socket; for those the
/// kernel answers, as it does today.
pub trait Resource: AsDescriptor + Send + Sync + 'static {
	type Kind: Kind;
}

/// A listening socket with a typed accepted stream.
pub trait ListenerHandle: Resource<Kind = Listener> {
	type Stream: Resource + From<OwnedSocket>;
}

mod sealed {
	pub trait Sealed {}
}

/// One of [`File`], [`Descriptor`], [`Datagram`], [`Stream`], [`Listener`],
/// [`Signal`].
pub trait Kind: sealed::Sealed {}

/// A kind that is read and written at an offset: [`File`] and [`Descriptor`].
pub trait Positional: Kind {}

/// A kind that sends and receives: [`Datagram`] and [`Stream`].
pub trait SocketKind: Kind {}

/// A file: read and written at an offset, synced, flushed, allocated and
/// punched.
pub enum File {}

/// Read and written at an offset, and nothing only a file does: a pipe, a
/// terminal, a descriptor whose type says no more. One that cannot seek
/// ignores the offset, or refuses it.
pub enum Descriptor {}

/// A socket with message boundaries: sends and receives, no loops.
pub enum Datagram {}

/// A byte-stream socket: sends and receives, the loops over them, `shutdown`.
pub enum Stream {}

/// A listening socket: `accept`.
pub enum Listener {}

/// A descriptor waited on for readiness, and nothing else: an eventfd, a
/// pidfd, a signal endpoint.
///
/// Nothing moves through one. That is why it is a kind of its own rather than
/// a [`Descriptor`]: a resource with a transfer has no readiness wait to
/// offer, because the backend arms the readiness itself when a call would
/// block and the completion says how much moved.
pub enum Signal {}

impl sealed::Sealed for File {}
impl sealed::Sealed for Descriptor {}
impl sealed::Sealed for Datagram {}
impl sealed::Sealed for Stream {}
impl sealed::Sealed for Listener {}
impl sealed::Sealed for Signal {}

impl Kind for File {}
impl Kind for Descriptor {}
impl Kind for Datagram {}
impl Kind for Stream {}
impl Kind for Listener {}
impl Kind for Signal {}

impl Positional for File {}
impl Positional for Descriptor {}

impl SocketKind for Datagram {}
impl SocketKind for Stream {}

macro_rules! resources {
	($kind:ident: $($resource:ty),* $(,)?) => {
		$(
			impl Resource for $resource {
				type Kind = $kind;
			}
		)*
	};
}

resources!(File: std::fs::File);
resources!(
	Descriptor: std::io::Stdin,
	std::io::Stdout,
	std::io::Stderr,
	std::io::PipeReader,
	std::io::PipeWriter,
);
resources!(Stream: std::net::TcpStream);
resources!(Datagram: std::net::UdpSocket);
resources!(Listener: std::net::TcpListener);

impl ListenerHandle for std::net::TcpListener {
	type Stream = std::net::TcpStream;
}

#[cfg(unix)]
mod platform {
	use std::os::fd::OwnedFd;
	use std::os::unix::net::{UnixDatagram, UnixListener, UnixStream};
	use std::process::{ChildStderr, ChildStdin, ChildStdout};

	use super::{Datagram, Descriptor, Listener, ListenerHandle, Resource, Stream};

	resources!(Descriptor: OwnedFd, ChildStdin, ChildStdout, ChildStderr);
	resources!(Stream: UnixStream);
	resources!(Datagram: UnixDatagram);
	resources!(Listener: UnixListener);

	impl ListenerHandle for UnixListener {
		type Stream = UnixStream;
	}
}

#[cfg(windows)]
mod platform {
	use std::os::windows::io::{OwnedHandle, OwnedSocket};

	use completion_rt::OwnedDescriptor;

	use super::{Datagram, Descriptor, Resource};

	resources!(Descriptor: OwnedHandle, OwnedDescriptor);
	resources!(Datagram: OwnedSocket);
}
