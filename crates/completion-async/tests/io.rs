//! Direct I/O: whether the open asks the kernel for it, and the error shape
//! when the filesystem refuses.
mod common;

use std::io;

#[cfg(unix)]
use completion_async::Stream;
use completion_async::{Buffer, Config, ErrorKind, Executor, File, OpenOptions};

/// The async driver the file operations run on: it owns the reactor, so
/// `block_on` both polls the operation and drives the backend under it.
fn executor() -> Executor {
	common::init_logging();
	Executor::new(Config::default()).unwrap()
}

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
			matches!(
				error.kind(),
				io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
			),
			"unexpected direct-I/O open error: {error}"
		),
	}
}

/// A pipe is a stream, not a file: it has no offset to name, and its transfers
/// move the descriptor's own position instead. The console's rotated output and
/// its stdin are pipes.
///
/// Windows: an anonymous pipe is a synchronous handle, which a completion port
/// cannot own.
#[cfg(unix)]
#[test]
fn a_pipe_transfers_at_its_own_position() {
	let (reader, writer) = std::io::pipe().unwrap();
	let mut executor = executor();
	let reader = Stream::new(reader);
	let writer = Stream::new(writer);

	let payload: Buffer = Buffer::from(b"hello");
	let memory = payload.whole();
	let moved = executor
		.block_on(writer.write(memory.take(5)))
		.unwrap()
		.unwrap();
	assert_eq!(moved, 5);

	let read: Buffer = Buffer::zeroed(5);
	let memory = read.whole();
	let moved = executor
		.block_on(reader.read_exact(memory.take(5)))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(moved, 5);
	assert_eq!(read.whole().to_vec(), b"hello");
}

/// A read that stops short of the buffer is the count it reached, not an
/// error; `read_exact_at` is the caller that insists on the whole buffer, and
/// the end of the file is what it reports.
#[test]
fn read_exact_at_reports_the_file_ending_first() {
	let directory = common::tempdir("completion-read-exact").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let mut executor = executor();
	let file = File::new(common::open(&path).unwrap());

	let buffer: Buffer = Buffer::zeroed(8);
	// One call: five bytes are left, and that is what it reports.
	let memory = buffer.whole();
	assert_eq!(
		executor.block_on(file.read_at(memory, 0)).unwrap().unwrap(),
		5
	);
	assert_eq!(&buffer.whole().to_vec()[..5], b"hello");

	let buffer: Buffer = Buffer::zeroed(8);
	// The loop stops where the file does, and the progress it made is the
	// bytes it did bring back, not the whole buffer.
	let memory = buffer.whole();
	let outcome = executor.block_on(file.read_exact_at(memory, 0)).unwrap();
	assert_eq!(outcome.value, 5);
	assert_eq!(
		&buffer.whole().to_vec()[..5],
		b"hello",
		"what did arrive stays"
	);
	match outcome
		.result
		.expect_err("the file has fewer bytes than the buffer holds")
	{
		error if error.kind() == ErrorKind::Failed => {
			assert_eq!(io::Error::from(error).kind(), io::ErrorKind::UnexpectedEof)
		}
		other => panic!("a loop that ran out of file reports an io error, not {other:?}"),
	}
}

/// `write_all_at` writes past one call's worth: a payload larger than a page
/// goes down whole, and `read_exact_at` brings all of it back.
#[test]
fn write_all_at_and_read_exact_at_move_a_large_payload() {
	let directory = common::tempdir("completion-write-all").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let mut executor = executor();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);

	let len = 4 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Buffer = Buffer::from(bytes.as_slice());
	let memory = payload.whole();
	executor
		.block_on(file.write_all_at(memory, 0))
		.unwrap()
		.into_result()
		.unwrap();
	executor.block_on(file.sync()).unwrap().unwrap();

	let read: Buffer = Buffer::zeroed(len);
	let memory = read.whole();
	executor
		.block_on(file.read_exact_at(memory, 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(
		read.whole().to_vec(),
		bytes,
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
	let mut executor = executor();
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.open(&path)
		.unwrap();
	let file = File::new(file);

	let written: Buffer = Buffer::from(b"hello");
	let memory = written.whole();
	assert_eq!(
		executor
			.block_on(file.write_at(memory, 0))
			.unwrap()
			.unwrap(),
		5
	);
	executor.block_on(file.sync()).unwrap().unwrap();

	let read: Buffer = Buffer::zeroed(5);
	let memory = read.whole();
	assert_eq!(
		executor.block_on(file.read_at(memory, 0)).unwrap().unwrap(),
		5
	);
	assert_eq!(read.whole().to_vec(), b"hello");

	// Back to the plain file: nothing names it any more, so the handle gives it
	// up; no binding holds it any longer.
	let plain = match file.try_take() {
		Some(plain) => plain,
		None => panic!("the handle still holds the file"),
	};
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
	let mut executor = executor();
	let file = File::new(common::open(&path).unwrap());

	let buffer: Buffer = Buffer::zeroed(5);
	let memory = buffer.whole();
	let read = assert_static(file.read_at(memory, 0));
	let mut reading = Reading {
		file: Some(file),
		read,
	};
	// The handle goes: the transfer names the file through its own clone of
	// the resource.
	drop(reading.file.take());

	assert_eq!(executor.block_on(reading.read).unwrap().unwrap(), 5);
	assert_eq!(buffer.whole().to_vec(), b"hello");
}

/// `flush` reports the file's deferred write-back errors by closing a
/// duplicate, so the handle keeps its own descriptor and goes on working.
#[test]
fn flush_leaves_the_handle_usable() {
	let directory = common::tempdir("completion-flush").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let mut executor = executor();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);

	let first: Buffer = Buffer::from(b"hello");
	let memory = first.whole();
	executor
		.block_on(file.write_all_at(memory, 0))
		.unwrap()
		.into_result()
		.unwrap();
	executor.block_on(file.flush()).unwrap().unwrap();

	// The duplicate the flush closed was not the handle's descriptor: the same
	// handle still writes and reads.
	let second: Buffer = Buffer::from(b"world");
	let memory = second.whole();
	executor
		.block_on(file.write_all_at(memory, 5))
		.unwrap()
		.into_result()
		.unwrap();

	let read: Buffer = Buffer::zeroed(10);
	let memory = read.whole();
	executor
		.block_on(file.read_exact_at(memory, 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read.whole().to_vec(), b"helloworld");
}

fn filled(tag: &str, len: usize) -> (common::Scratch, std::path::PathBuf, Executor, File) {
	let directory = common::tempdir(tag).unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, vec![0xab; len]).unwrap();
	let executor = executor();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);
	(directory, path, executor, file)
}

/// An allocate past the end grows the file to the range's end; one inside it
/// leaves both the length and the bytes alone.
#[test]
fn an_allocate_grows_the_file_and_never_shrinks_it() {
	let (_directory, path, mut executor, file) = filled("completion-allocate", 8192);

	executor.block_on(file.allocate(0, 4096)).unwrap().unwrap();
	assert_eq!(std::fs::metadata(&path).unwrap().len(), 8192);
	assert_eq!(std::fs::read(&path).unwrap(), vec![0xab; 8192]);

	executor
		.block_on(file.allocate(8192, 4096))
		.unwrap()
		.unwrap();
	let metadata = std::fs::metadata(&path).unwrap();
	assert_eq!(metadata.len(), 12288);
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
	assert_eq!(&contents[..8192], &[0xab; 8192][..]);
	assert_eq!(&contents[8192..], &[0; 4096][..]);
}

/// A punch reads back as zeros across the range, block-aligned or not, and
/// leaves the length alone.
#[test]
fn a_punch_zeroes_the_range_and_keeps_the_length() {
	let (_directory, path, mut executor, file) = filled("completion-punch", 3 * 4096);

	executor.block_on(file.punch(100, 5000)).unwrap().unwrap();

	let contents = std::fs::read(&path).unwrap();
	assert_eq!(contents.len(), 3 * 4096);
	assert_eq!(&contents[..100], &[0xab; 100][..]);
	assert_eq!(&contents[100..5100], &[0; 5000][..]);
	assert_eq!(&contents[5100..], &[0xab; 3 * 4096 - 5100][..]);
}
