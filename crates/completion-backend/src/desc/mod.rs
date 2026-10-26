#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
#[cfg(windows)]
pub use windows::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};

impl<T: AsDescriptor + ?Sized> AsDescriptor for std::sync::Arc<T> {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		(**self).as_descriptor()
	}
}

#[cfg(test)]
mod tests {
	use std::io::{Read, Write};
	#[cfg(unix)]
	use std::os::fd::AsRawFd;

	use assert2::assert;

	#[cfg(windows)]
	use super::BorrowedDescriptor;
	use super::{AsDescriptor, OwnedDescriptor, OwnedSocket};

	/// A descriptor cloned from a pipe's write end keeps the pipe writable after
	/// the descriptor it came from is gone.
	#[test]
	fn a_cloned_descriptor_outlives_the_original() {
		let (mut reader, writer) = std::io::pipe().unwrap();
		let owned = writer.as_descriptor().try_clone_to_owned().unwrap();

		// The original goes; the clone has to carry the pipe on its own.
		drop(writer);

		#[cfg(unix)]
		let mut clone = std::fs::File::from(owned);
		#[cfg(windows)]
		let mut clone = match owned {
			OwnedDescriptor::Handle(handle) => std::fs::File::from(handle),
			OwnedDescriptor::Socket(_) => panic!("a pipe carries a handle, not a socket"),
		};

		clone.write_all(b"hello").unwrap();
		drop(clone);

		let mut bytes = Vec::new();
		reader.read_to_end(&mut bytes).unwrap();
		assert!(bytes == b"hello");
	}

	/// Owned descriptors and sockets borrow as the same kind of OS handle they own.
	#[test]
	fn owned_descriptor_and_socket_round_trip_through_as_descriptor() {
		let (reader, _writer) = std::io::pipe().unwrap();
		let owned = OwnedDescriptor::from(reader);

		let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
		let owned_sock = OwnedSocket::from(udp);

		#[cfg(unix)]
		{
			assert!(owned.as_raw_fd() == owned.as_descriptor().as_raw_fd());
			assert!(owned_sock.as_raw_fd() >= 0);
		}
		#[cfg(windows)]
		{
			assert!(let BorrowedDescriptor::Handle(_) = owned.as_descriptor());
			let owned_socket = OwnedDescriptor::from(owned_sock);
			assert!(let BorrowedDescriptor::Socket(_) = owned_socket.as_descriptor());
		}
	}
}
