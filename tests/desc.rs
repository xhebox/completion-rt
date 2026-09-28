//! Duplicating a descriptor: the clone is a descriptor in its own right.

use std::io::{Read, Write};

use completion_rt::AsDescriptor as _;

/// A descriptor cloned from a pipe's write end keeps the pipe writable after
/// the descriptor it came from is gone: `try_clone_to_owned` makes a second
/// descriptor, not a second name for the one it borrowed.
#[test]
fn a_cloned_descriptor_outlives_the_original() {
	let (mut reader, writer) = std::io::pipe().unwrap();
	let owned = writer.as_descriptor().try_clone_to_owned().unwrap();

	// The original goes; the clone has to carry the pipe on its own. A clone
	// that were the same descriptor would leave this side closed.
	drop(writer);

	#[cfg(unix)]
	let mut clone = std::fs::File::from(owned);
	#[cfg(windows)]
	let mut clone = {
		use completion_rt::OwnedDescriptor;
		match owned {
			OwnedDescriptor::Handle(handle) => std::fs::File::from(handle),
			OwnedDescriptor::Socket(_) => panic!("a pipe carries a handle, not a socket"),
		}
	};

	clone.write_all(b"hello").unwrap();
	drop(clone);

	let mut bytes = Vec::new();
	reader.read_to_end(&mut bytes).unwrap();
	assert_eq!(bytes, b"hello");
}
