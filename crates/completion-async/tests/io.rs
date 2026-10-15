//! Direct I/O: whether the open asks the kernel for it, and the error shape
//! when the filesystem refuses.
mod common;
use common::Buffer;

use common::block_on;

use std::io;
use std::sync::Arc;

use completion_async::{Config, ErrorKind, Executor, Facade, File, OpenOptions};

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

/// A pipe is not seekable, so the vector path must not insist on an offset:
/// the console's rotated output and its stdin are pipes.
///
/// Windows: an anonymous pipe is a synchronous handle, which a completion port
/// cannot own.
#[cfg(unix)]
#[test]
fn a_pipe_takes_offsets_that_it_cannot_seek_to() {
	let (reader, writer) = std::io::pipe().unwrap();
	let mut executor = executor();
	let reader: Facade<std::io::PipeReader> = Facade::with_executor(reader, &executor);
	let writer: Facade<std::io::PipeWriter> = Facade::with_executor(writer, &executor);

	let payload: Arc<Buffer> = Arc::new(Buffer::from_slice(b"hello"));
	let memory = executor.bind(payload.clone()).whole();
	let moved = executor
		.block_on(writer.write_at(memory.take(5), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(moved, 5);

	let read: Arc<Buffer> = Arc::new(Buffer::new(5));
	let memory = executor.bind(read.clone()).whole();
	let moved = executor
		.block_on(reader.read_at(memory.take(5), 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(moved, 5);
	assert_eq!(read.to_vec(), b"hello");
}

/// The vector path names host memory through a backing instead of owning a
/// buffer; the bytes are checked through an owned read, which is the only copy
/// this test may look at while a transfer is in flight.
#[test]
fn the_vector_path_goes_through_the_backing() {
	let directory = common::tempdir("completion-vector").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let mut executor = executor();
	let file: File = Facade::with_executor(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
		&executor,
	);

	let source: Arc<Buffer> = Arc::new(Buffer::from_slice(b"payload"));
	let memory = executor.bind(source.clone()).whole();
	assert_eq!(
		executor
			.block_on(file.write_at(memory, 0))
			.unwrap()
			.into_result()
			.unwrap(),
		7
	);

	let sink: Arc<Buffer> = Arc::new(Buffer::new(7));
	let memory = executor.bind(sink.clone()).whole();
	assert_eq!(
		executor
			.block_on(file.read_at(memory, 0))
			.unwrap()
			.into_result()
			.unwrap(),
		7
	);

	let memory = executor.bind(sink.clone()).whole();
	assert_eq!(
		executor
			.block_on(file.write_at(memory, 7))
			.unwrap()
			.into_result()
			.unwrap(),
		7
	);
	let whole: Arc<Buffer> = Arc::new(Buffer::new(14));
	let memory = executor.bind(whole.clone()).whole();
	assert_eq!(
		executor
			.block_on(file.read_at(memory, 0))
			.unwrap()
			.into_result()
			.unwrap(),
		14
	);
	assert_eq!(whole.to_vec(), b"payloadpayload");
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
	let file = File::with_executor(common::open(&path).unwrap(), &executor);

	let buffer: Arc<Buffer> = Arc::new(Buffer::new(8));
	// One call: five bytes are left, and that is what it reports.
	let memory = executor.bind(buffer.clone()).whole();
	assert_eq!(
		executor
			.block_on(file.read_at(memory, 0))
			.unwrap()
			.into_result()
			.unwrap(),
		5
	);
	assert_eq!(&buffer.to_vec()[..5], b"hello");

	let buffer: Arc<Buffer> = Arc::new(Buffer::new(8));
	// The loop stops where the file does, and the progress it made is the
	// bytes it did bring back, not the whole buffer.
	let memory = executor.bind(buffer.clone()).whole();
	let outcome = executor.block_on(file.read_exact_at(memory, 0)).unwrap();
	assert_eq!(outcome.value, 5);
	assert_eq!(&buffer.to_vec()[..5], b"hello", "what did arrive stays");
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
	let file = File::with_executor(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
		&executor,
	);

	let len = 4 << 20;
	let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
	let payload: Arc<Buffer> = Arc::new(Buffer::from_slice(&bytes));
	let memory = executor.bind(payload.clone()).whole();
	executor
		.block_on(file.write_all_at(memory, 0))
		.unwrap()
		.into_result()
		.unwrap();
	executor.block_on(file.sync()).unwrap().unwrap();

	let read: Arc<Buffer> = Arc::new(Buffer::new(len));
	let memory = executor.bind(read.clone()).whole();
	executor
		.block_on(file.read_exact_at(memory, 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read.to_vec(), bytes, "the payload did not come back whole");
}

/// The facade binds the file to the executor, so a call site names neither an
/// offset of a whole-buffer transfer nor the extents behind one.
#[test]
fn the_facade_reads_and_writes_a_whole_buffer() {
	let directory = common::tempdir("completion-facade").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let mut executor = executor();
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.open(&path)
		.unwrap();
	let file: File = Facade::with_executor(file, &executor);

	let written: Arc<Buffer> = Arc::new(Buffer::from_slice(b"hello"));
	let memory = executor.bind(written.clone()).whole();
	assert_eq!(
		executor
			.block_on(file.write_at(memory, 0))
			.unwrap()
			.into_result()
			.unwrap(),
		5
	);
	executor.block_on(file.sync()).unwrap().unwrap();

	let read: Arc<Buffer> = Arc::new(Buffer::new(5));
	let memory = executor.bind(read.clone()).whole();
	assert_eq!(
		executor
			.block_on(file.read_at(memory, 0))
			.unwrap()
			.into_result()
			.unwrap(),
		5
	);
	assert_eq!(read.to_vec(), b"hello");

	// Back to the plain file: nothing names it any more, so the facade gives it
	// up; the executor binding is dropped.
	let Some(plain) = block_on(file.take()) else {
		panic!("the facade still holds the file");
	};
	drop(plain);
}

/// A facade nothing else names gives its file back at once: a take resolves on
/// the first poll rather than waiting for a wake that will never come.
#[test]
fn a_facade_with_no_operations_gives_its_file_back_at_once() {
	let directory = common::tempdir("completion-take").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"data").unwrap();
	let executor = executor();
	let file = File::with_executor(common::open(&path).unwrap(), &executor);

	let plain = block_on(file.take()).expect("nothing else names the file");
	assert_eq!(plain.metadata().unwrap().len(), 4);
}

/// A transfer owns the facade's handle and executor, so a poll-driven state
/// machine can keep it beside the facade, let the facade binding go before the
/// first poll, and still drive the transfer to its end.
#[test]
fn a_transfer_outlives_the_facade_binding_it_was_made_from() {
	use completion_async::Transfer;

	/// What one hand-written poll loop holds across its polls.
	struct Reading {
		file: Option<File>,
		read: Transfer<'static, std::fs::File>,
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
	let file = File::with_executor(common::open(&path).unwrap(), &executor);

	let buffer: Arc<Buffer> = Arc::new(Buffer::new(5));
	let memory = executor.bind(buffer.clone()).whole();
	let read = assert_static(file.read_at(memory, 0));
	let mut reading = Reading {
		file: Some(file),
		read,
	};
	// The facade binding goes: the transfer names the file through its own
	// clone of the handle.
	drop(reading.file.take());

	assert_eq!(
		executor
			.block_on(reading.read)
			.unwrap()
			.into_result()
			.unwrap(),
		5
	);
	assert_eq!(buffer.to_vec(), b"hello");
}

/// `flush` reports the file's deferred write-back errors by closing a
/// duplicate, so the facade keeps its own descriptor and goes on working.
#[test]
fn flush_leaves_the_facade_usable() {
	let directory = common::tempdir("completion-flush").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let mut executor = executor();
	let file = File::with_executor(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
		&executor,
	);

	let first: Arc<Buffer> = Arc::new(Buffer::from_slice(b"hello"));
	let memory = executor.bind(first.clone()).whole();
	executor
		.block_on(file.write_all_at(memory, 0))
		.unwrap()
		.into_result()
		.unwrap();
	executor.block_on(file.flush()).unwrap().unwrap();

	// The duplicate the flush closed was not the facade's descriptor: the same
	// facade still writes and reads.
	let second: Arc<Buffer> = Arc::new(Buffer::from_slice(b"world"));
	let memory = executor.bind(second.clone()).whole();
	executor
		.block_on(file.write_all_at(memory, 5))
		.unwrap()
		.into_result()
		.unwrap();

	let read: Arc<Buffer> = Arc::new(Buffer::new(10));
	let memory = executor.bind(read.clone()).whole();
	executor
		.block_on(file.read_exact_at(memory, 0))
		.unwrap()
		.into_result()
		.unwrap();
	assert_eq!(read.to_vec(), b"helloworld");
}

/// A descriptor nothing owns anymore: `flush` must report the error rather than
/// pretend success. Unix only — the close-time error is a unix idea.
#[cfg(unix)]
#[test]
fn flush_reports_a_descriptor_that_is_not_open() {
	use std::os::fd::{AsRawFd, FromRawFd};

	let mut executor = executor();

	// Opened then closed, so the number is free and nothing owns it. Made after
	// the reactor so no descriptor it built can have taken the number back.
	let dead = {
		let (reader, writer) = std::io::pipe().unwrap();
		drop(writer);
		let raw = reader.as_raw_fd();
		drop(reader);
		raw
	};

	let file = File::with_executor(unsafe { std::fs::File::from_raw_fd(dead) }, &executor);
	match executor.block_on(file.flush()).unwrap() {
		Err(error) if error.kind() == ErrorKind::Failed => {
			let error = io::Error::from(error);
			assert_eq!(
				error.raw_os_error(),
				Some(rustix::io::Errno::BADF.raw_os_error()),
				"a closed descriptor reports EBADF, not {error}"
			)
		}
		other => panic!("flush on a closed descriptor must fail, got {other:?}"),
	}

	// The file owns a descriptor that is not open: dropping it would close the
	// number again, which aborts under a debug assertion. Take it back and leak
	// it instead.
	let plain = match file.try_take() {
		Ok(plain) => plain,
		Err(_) => panic!("an operation still names the file"),
	};
	std::mem::forget(plain);
}

fn filled(tag: &str, len: usize) -> (common::Scratch, std::path::PathBuf, Executor, File) {
	let directory = common::tempdir(tag).unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, vec![0xab; len]).unwrap();
	let executor = executor();
	let file = File::with_executor(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
		&executor,
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
