use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

/// An owned file descriptor closed on drop.
pub type OwnedDescriptor = OwnedFd;
/// A borrowed file descriptor.
pub type BorrowedDescriptor<'a> = BorrowedFd<'a>;
/// An owned socket descriptor closed on drop.
pub type OwnedSocket = OwnedFd;

/// Borrows the underlying OS descriptor or handle.
pub trait AsDescriptor: Send + Sync {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_>;
}

impl<T: AsFd + Send + Sync> AsDescriptor for T {
	fn as_descriptor(&self) -> BorrowedDescriptor<'_> {
		self.as_fd()
	}
}
