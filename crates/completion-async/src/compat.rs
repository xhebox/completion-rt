use std::future::Future;
use std::io;
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use completion_backend::Op;

use crate::net::StreamSocket;
use crate::{Slot, submit};

/// Readiness-backed wrapper around a [`StreamSocket`] implementing
/// [`futures_io::AsyncRead`] and [`futures_io::AsyncWrite`] (and
/// `tokio::io::{AsyncRead, AsyncWrite}` when the `tokio-io` feature is
/// enabled).
///
/// Transfers perform non-blocking socket reads and writes directly into the
/// caller's slice and arm [`Op::Readable`] / [`Op::Writable`] waits on
/// `WouldBlock`, avoiding intermediate [`Buffer`](crate::Buffer) allocations or
/// copies and remaining cancellation-safe across `select!`.
pub struct PollSocket {
	socket: StreamSocket,
	read_wait: Option<Pin<Box<Slot<StreamSocket, ()>>>>,
	write_wait: Option<Pin<Box<Slot<StreamSocket, ()>>>>,
}

impl PollSocket {
	/// Wraps `socket` and ensures the underlying OS socket is in non-blocking
	/// mode.
	pub fn new(socket: StreamSocket) -> io::Result<PollSocket> {
		socket2::SockRef::from(socket.inner_fd()).set_nonblocking(true)?;
		Ok(PollSocket {
			socket,
			read_wait: None,
			write_wait: None,
		})
	}

	/// Borrows the underlying [`StreamSocket`].
	pub fn get_ref(&self) -> &StreamSocket {
		&self.socket
	}

	/// Unwraps the [`StreamSocket`], cancelling any in-flight readiness waits.
	pub fn into_inner(mut self) -> StreamSocket {
		self.read_wait = None;
		self.write_wait = None;
		self.socket
	}

	fn poll_read_slice(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
		loop {
			if let Some(wait) = &mut self.read_wait {
				let (result, _fd, ()) = ready!(wait.as_mut().poll(cx));
				self.read_wait = None;
				result.map_err(|e| io::Error::try_from(e).unwrap_or_else(io::Error::other))?;
			}
			let sock = socket2::SockRef::from(self.socket.inner_fd());
			match io::Read::read(&mut &*sock, buf) {
				Ok(n) => return Poll::Ready(Ok(n)),
				Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
					let slot = submit(self.socket.clone(), Op::Readable, ())
						.map_err(|e| io::Error::try_from(e).unwrap_or_else(io::Error::other))?;
					self.read_wait = Some(Box::pin(slot));
				}
				Err(e) => return Poll::Ready(Err(e)),
			}
		}
	}

	fn poll_write_slice(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
		loop {
			if let Some(wait) = &mut self.write_wait {
				let (result, _fd, ()) = ready!(wait.as_mut().poll(cx));
				self.write_wait = None;
				result.map_err(|e| io::Error::try_from(e).unwrap_or_else(io::Error::other))?;
			}
			let sock = socket2::SockRef::from(self.socket.inner_fd());
			match io::Write::write(&mut &*sock, buf) {
				Ok(n) => return Poll::Ready(Ok(n)),
				Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
					let slot = submit(self.socket.clone(), Op::Writable, ())
						.map_err(|e| io::Error::try_from(e).unwrap_or_else(io::Error::other))?;
					self.write_wait = Some(Box::pin(slot));
				}
				Err(e) => return Poll::Ready(Err(e)),
			}
		}
	}
}

impl futures_io::AsyncRead for PollSocket {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut [u8],
	) -> Poll<io::Result<usize>> {
		self.get_mut().poll_read_slice(cx, buf)
	}
}

impl futures_io::AsyncWrite for PollSocket {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		self.get_mut().poll_write_slice(cx, buf)
	}

	fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Poll::Ready(Ok(()))
	}

	fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Poll::Ready(self.socket.shutdown(Shutdown::Write))
	}
}

#[cfg(any(test, feature = "tokio-io"))]
impl tokio::io::AsyncRead for PollSocket {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<io::Result<()>> {
		let this = self.get_mut();
		let n = ready!(this.poll_read_slice(cx, buf.initialize_unfilled()))?;
		buf.advance(n);
		Poll::Ready(Ok(()))
	}
}

#[cfg(any(test, feature = "tokio-io"))]
impl tokio::io::AsyncWrite for PollSocket {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		self.get_mut().poll_write_slice(cx, buf)
	}

	fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Poll::Ready(Ok(()))
	}

	fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Poll::Ready(self.socket.shutdown(Shutdown::Write))
	}
}
