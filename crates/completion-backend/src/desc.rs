#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
#[cfg(windows)]
pub use windows::{AsDescriptor, BorrowedDescriptor, OwnedDescriptor, OwnedSocket};
