//! Direct I/O: whether the open asks the kernel for it, and the error shape
//! when the filesystem refuses.
mod common;

use std::io;

use assert2::assert;
use completion_async::{Buffer, ErrorKind, File, OpenOptions, Stream, block_on};

#[test]
fn a_direct_open_asks_the_kernel_for_direct_io() {
	let directory = common::tempdir("completion-direct-open").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, [0u8; 4096]).unwrap();

	// The filesystem answers: either the open takes the flag — and the
	// descriptor then carries it — or it refuses, which only the two
	// "this filesystem does not do direct I/O" errors excuse.
	match OpenOptions::new().read(true).direct().open(&path) {
		#[cfg(any(target_os = "linux", target_os = "android"))]
		Ok(file) => {
			let flags = rustix::fs::fcntl_getfl(&file).expect("F_GETFL on a fresh descriptor");
			assert!(
				flags.contains(rustix::fs::OFlags::DIRECT),
				"direct() did not reach the descriptor: {flags:?}"
			);
		}
		#[cfg(not(any(target_os = "linux", target_os = "android")))]
		Ok(file) => drop(file),
		Err(error) => assert!(
			error.kind() == io::ErrorKind::Unsupported
				|| error.kind() == io::ErrorKind::InvalidInput,
			"unexpected direct-I/O open error: {error}"
		),
	}
}

/// A pipe is a stream, not a file: it has no offset to name, and its transfers
/// move the descriptor's own position instead. The console's rotated output and
/// its stdin are pipes.
#[test]
fn a_pipe_transfers_at_its_own_position() {
	let (reader, writer) = Stream::pipe().unwrap();
	common::init_logging();

	let payload: Buffer = Buffer::copy_from(b"hello");
	let moved = block_on(writer.write(payload.clone())).unwrap().unwrap();
	assert!(moved == 5);

	let mut read: Buffer = Buffer::zeroed(5);
	let moved = block_on(reader.read_exact(read.clone())).unwrap().unwrap();
	assert!(moved == 5);
	assert!(read.get_mut().unwrap().to_vec() == b"hello");
}

/// A loop method owns its buffer one round at a time: the first round's
/// completion hands it back for the second, and dropping the future leaves it
/// in the entry of the round still in flight. The reactor frees it only when
/// that round finishes.
#[test]
fn a_dropped_read_exact_keeps_its_memory_until_the_kernel_finishes() {
	use std::task::Poll;
	use std::time::Duration;

	use completion_async::timeout;

	let (stream, writer) = Stream::pipe().unwrap();
	common::init_logging();
	let (buffer, live) = common::tracked(8);
	let payload = buffer.clone();
	drop(buffer);
	block_on(writer.write_all(Buffer::copy_from(b"first")))
		.unwrap()
		.unwrap();

	let mut read = Box::pin(stream.read_exact(payload));
	block_on(async {
		// The five bytes that are there fill the first round; the second one is
		// filed for the rest of the buffer.
		assert!(
			std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx).is_pending())).await,
			"the first round did not go in"
		);
		timeout(Duration::from_millis(50)).await.unwrap();
		assert!(
			std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx).is_pending())).await,
			"the second round did not go in"
		);
	})
	.unwrap();

	drop(read);
	assert!(
		live.alive(),
		"dropping the loop released memory a round still names"
	);
	// One pass over the reactor reaps the cancelled round.
	block_on(timeout(Duration::from_millis(50)))
		.unwrap()
		.unwrap();
	assert!(!live.alive(), "the finished round kept its memory");
}

/// A read that stops short of the buffer is the count it reached, not an
/// error; `read_exact_at` is the caller that insists on the whole buffer, and
/// the end of the file is what it reports.
#[test]
fn read_exact_at_reports_the_file_ending_first() {
	let directory = common::tempdir("completion-read-exact").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	common::init_logging();
	let file = File::new(common::open(&path).unwrap());

	let mut buffer: Buffer = Buffer::zeroed(8);
	// One call: five bytes are left, and that is what it reports.
	assert!(block_on(file.read_at(buffer.clone(), 0)).unwrap().unwrap() == 5);
	assert!(&buffer.get_mut().unwrap().to_vec()[..5] == b"hello");

	let mut buffer: Buffer = Buffer::zeroed(8);
	// The loop stops where the file does, leaving the bytes it did bring back
	// in the buffer and reporting `UnexpectedEof`.
	let result = block_on(file.read_exact_at(buffer.clone(), 0)).unwrap();
	assert!(
		&buffer.get_mut().unwrap().to_vec()[..5] == b"hello",
		"what did arrive stays"
	);
	assert!(let Err(error) = result, "the file has fewer bytes than the buffer holds");
	assert!(error.done() == 5);
	assert!(error.kind() == ErrorKind::Failed);
	assert!(io::Error::try_from(error).unwrap().kind() == io::ErrorKind::UnexpectedEof);
}

/// `write_all_at` writes past one call's worth: a payload larger than a page
/// goes down whole, and `read_exact_at` brings all of it back.
#[test]
fn write_all_at_and_read_exact_at_move_a_large_payload() {
	let directory = common::tempdir("completion-write-all").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	common::init_logging();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);

	let len = 4 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Buffer = Buffer::copy_from(&bytes);
	block_on(file.write_all_at(payload.clone(), 0))
		.unwrap()
		.unwrap();
	block_on(file.sync()).unwrap().unwrap();

	let mut read: Buffer = Buffer::zeroed(len);
	block_on(file.read_exact_at(read.clone(), 0))
		.unwrap()
		.unwrap();
	assert!(
		read.get_mut().unwrap().to_vec() == bytes,
		"the payload did not come back whole"
	);
}

/// A handle reads and writes a buffer, and gives the plain file back once
/// nothing else names it.
#[test]
fn the_handle_reads_and_writes_a_whole_buffer() {
	let directory = common::tempdir("completion-handle").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	common::init_logging();
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.open(&path)
		.unwrap();
	let file = File::new(file);

	let written: Buffer = Buffer::copy_from(b"hello");
	assert!(
		block_on(file.write_at(written.clone(), 0))
			.unwrap()
			.unwrap()
			== 5
	);
	block_on(file.sync()).unwrap().unwrap();

	let mut read: Buffer = Buffer::zeroed(5);
	assert!(block_on(file.read_at(read.clone(), 0)).unwrap().unwrap() == 5);
	assert!(read.get_mut().unwrap().to_vec() == b"hello");

	// Back to the plain file: nothing names it any more, so the handle gives it
	// up; no binding holds it any longer.
	assert!(let Some(plain) = file.try_take(), "the handle still holds the file");
	drop(plain);
}

/// A transfer owns a clone of the handle's resource and the reactor, so a
/// poll-driven state machine can keep it beside the handle, let the handle go
/// before the first poll, and still drive the transfer to its end.
#[test]
fn a_transfer_outlives_the_handle_it_was_made_from() {
	/// What one hand-written poll loop holds across its polls.
	struct Reading<F> {
		file: Option<File>,
		read: F,
	}

	/// A state machine boxes what it is waiting for, which needs the future to
	/// be `'static`.
	fn assert_static<T: 'static>(transfer: T) -> T {
		transfer
	}

	let directory = common::tempdir("completion-future-outlives").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	common::init_logging();
	let file = File::new(common::open(&path).unwrap());

	let mut buffer: Buffer = Buffer::zeroed(5);
	let read = assert_static(file.read_at(buffer.clone(), 0));
	let mut reading = Reading {
		file: Some(file),
		read,
	};
	// The handle goes: the transfer names the file through its own clone of
	// the resource.
	drop(reading.file.take());

	assert!(block_on(reading.read).unwrap().unwrap() == 5);
	assert!(buffer.get_mut().unwrap().to_vec() == b"hello");
}

/// `flush` reports the file's deferred write-back errors by closing a
/// duplicate, so the handle keeps its own descriptor and goes on working.
#[test]
fn flush_leaves_the_handle_usable() {
	let directory = common::tempdir("completion-flush").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	common::init_logging();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);

	let first: Buffer = Buffer::copy_from(b"hello");
	block_on(file.write_all_at(first.clone(), 0))
		.unwrap()
		.unwrap();
	block_on(file.flush()).unwrap().unwrap();

	// The duplicate the flush closed was not the handle's descriptor: the same
	// handle still writes and reads.
	let second: Buffer = Buffer::copy_from(b"world");
	block_on(file.write_all_at(second.clone(), 5))
		.unwrap()
		.unwrap();

	let mut read: Buffer = Buffer::zeroed(10);
	block_on(file.read_exact_at(read.clone(), 0))
		.unwrap()
		.unwrap();
	assert!(read.get_mut().unwrap().to_vec() == b"helloworld");
}

fn filled(tag: &str, len: usize) -> (common::Scratch, std::path::PathBuf, File) {
	let directory = common::tempdir(tag).unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, vec![0xab; len]).unwrap();
	common::init_logging();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);
	(directory, path, file)
}

/// An allocate past the end grows the file to the range's end; one inside it
/// leaves both the length and the bytes alone.
#[test]
fn an_allocate_grows_the_file_and_never_shrinks_it() {
	let (_directory, path, file) = filled("completion-allocate", 8192);

	block_on(file.allocate(0, 4096)).unwrap().unwrap();
	assert!(std::fs::metadata(&path).unwrap().len() == 8192);
	assert!(std::fs::read(&path).unwrap() == vec![0xab; 8192]);

	block_on(file.allocate(8192, 4096)).unwrap().unwrap();
	let metadata = std::fs::metadata(&path).unwrap();
	assert!(metadata.len() == 12288);
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		assert!(
			metadata.blocks() * 512 >= 12288,
			"the range is not backed: {} blocks",
			metadata.blocks()
		);
	}
	let contents = std::fs::read(&path).unwrap();
	assert!(&contents[..8192] == &[0xab; 8192][..]);
	assert!(&contents[8192..] == &[0; 4096][..]);
}

/// A punch reads back as zeros across the range, block-aligned or not, and
/// leaves the length alone.
#[test]
fn a_punch_zeroes_the_range_and_keeps_the_length() {
	let (_directory, path, file) = filled("completion-punch", 3 * 4096);

	block_on(file.punch(100, 5000)).unwrap().unwrap();

	let contents = std::fs::read(&path).unwrap();
	assert!(contents.len() == 3 * 4096);
	assert!(&contents[..100] == &[0xab; 100][..]);
	assert!(&contents[100..5100] == &[0; 5000][..]);
	assert!(&contents[5100..] == &[0xab; 3 * 4096 - 5100][..]);
}
