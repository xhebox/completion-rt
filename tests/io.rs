//! Direct I/O: whether the open asks the kernel for it, and the error shape
//! when the filesystem refuses.
mod common;

use common::block_on;

use std::io;
use std::sync::Arc;
use std::time::Duration;

use completion_rt::{
	Completion, Config, Error, Extent, Facade, File, Handle, OpenOptions, Reactor, Response,
	Transfer,
};

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

/// Polls until the operation is done, then takes the result: the reactor has no
/// thread of its own, so the caller drives it.
fn run(reactor: &mut Reactor, completion: Completion) -> io::Result<Response> {
	while !reactor.is_idle() {
		reactor.poll(Some(Duration::from_millis(1))).unwrap();
	}
	block_on(completion).1
}

/// A pipe is not seekable, so the vector path must not insist on an offset:
/// the console's rotated output and its stdin are pipes.
#[test]
fn a_pipe_takes_offsets_that_it_cannot_seek_to() {
	let (reader, writer) = std::io::pipe().unwrap();
	let reader = Handle::new(reader);
	let writer = Handle::new(writer);
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();

	let payload: Arc<Vec<u8>> = Arc::new(b"hello".to_vec());
	let completion = submitter
		.write(&writer, 0, payload.clone(), [Extent { offset: 0, len: 5 }])
		.unwrap();
	assert_eq!(run(&mut reactor, completion).unwrap().count().unwrap(), 5);

	let read: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let completion = submitter
		.read(&reader, 0, read.clone(), [Extent { offset: 0, len: 5 }])
		.unwrap();
	assert_eq!(run(&mut reactor, completion).unwrap().count().unwrap(), 5);
	assert_eq!(&read[..], b"hello");
}

/// The vector path names host memory through a backing instead of owning a
/// buffer; the bytes are checked through an owned read, which is the only copy
/// this test may look at while a transfer is in flight.
#[test]
fn the_vector_path_goes_through_the_backing() {
	let directory = common::tempdir("completion-vector").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let (mut reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let file = Handle::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
	);
	let range = [Extent { offset: 0, len: 7 }];

	let source: Arc<Vec<u8>> = Arc::new(b"payload".to_vec());
	let completion = submitter.write(&file, 0, source, range).unwrap();
	assert_eq!(run(&mut reactor, completion).unwrap().count().unwrap(), 7);

	let sink: Arc<Vec<u8>> = Arc::new(vec![0; 7]);
	let completion = submitter.read(&file, 0, sink.clone(), range).unwrap();
	assert_eq!(run(&mut reactor, completion).unwrap().count().unwrap(), 7);

	let completion = submitter.write(&file, 7, sink, range).unwrap();
	assert_eq!(run(&mut reactor, completion).unwrap().count().unwrap(), 7);
	let whole: Arc<Vec<u8>> = Arc::new(vec![0; 14]);
	let completion = submitter
		.read(&file, 0, whole.clone(), [Extent { offset: 0, len: 14 }])
		.unwrap();
	assert_eq!(run(&mut reactor, completion).unwrap().count().unwrap(), 14);
	assert_eq!(&whole[..], b"payloadpayload");
}

/// A read that stops short of the buffer is the count it reached, not an
/// error; `read_exact_at` is the caller that insists on the whole buffer, and
/// the end of the file is what it reports.
#[test]
fn read_exact_at_reports_the_file_ending_first() {
	let directory = common::tempdir("completion-read-exact").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let file = File::new(std::fs::File::open(&path).unwrap(), &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; 8]);
	// One call: five bytes are left, and that is what it reports.
	assert_eq!(
		block_on(file.read_at(buffer.clone(), 0))
			.into_result()
			.unwrap(),
		5
	);
	assert_eq!(&buffer[..5], b"hello");

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; 8]);
	// The loop stops where the file does, and the progress it made is the
	// bytes it did bring back, not the whole buffer.
	let outcome = block_on(file.read_exact_at(buffer.clone(), 0));
	assert_eq!(outcome.value, 5);
	assert_eq!(&buffer[..5], b"hello", "what did arrive stays");
	match outcome
		.result
		.expect_err("the file has fewer bytes than the buffer holds")
	{
		Error::Io(error) => assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof),
		other => panic!("a loop that ran out of file reports an io error, not {other:?}"),
	}

	submitter.stop();
	driven.join().unwrap();
}

/// `write_all_at` writes past one call's worth: a payload larger than a page
/// goes down whole, and `read_exact_at` brings all of it back.
#[test]
fn write_all_at_and_read_exact_at_move_a_large_payload() {
	let directory = common::tempdir("completion-write-all").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let file = File::new(
		OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap(),
		&submitter,
	);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	let len = 4 << 20;
	let payload: Arc<Vec<u8>> = Arc::new((0..len).map(|i| i as u8).collect());
	block_on(file.write_all_at(payload.clone(), 0))
		.into_result()
		.unwrap();
	block_on(file.sync()).unwrap();

	let read: Arc<Vec<u8>> = Arc::new(vec![0; len]);
	block_on(file.read_exact_at(read.clone(), 0))
		.into_result()
		.unwrap();
	assert_eq!(*read, *payload, "the payload did not come back whole");

	submitter.stop();
	driven.join().unwrap();
}

/// The facade binds the file to the submitter, so a call site names neither an
/// offset of a whole-buffer transfer nor the extents behind one.
#[test]
fn the_facade_reads_and_writes_a_whole_buffer() {
	let directory = common::tempdir("completion-facade").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, []).unwrap();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.open(&path)
		.unwrap();
	let file = Facade::new(file, &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	let written: Arc<Vec<u8>> = Arc::new(b"hello".to_vec());
	assert_eq!(
		block_on(file.write_at(written, 0)).into_result().unwrap(),
		5
	);
	block_on(file.sync()).unwrap();

	let read: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	assert_eq!(
		block_on(file.read_at(read.clone(), 0))
			.into_result()
			.unwrap(),
		5
	);
	assert_eq!(&read[..], b"hello");

	// Back to the plain file: nothing names it any more, so the facade gives it
	// up; the submitter binding is dropped.
	let Some(plain) = block_on(file.take()) else {
		panic!("the facade still holds the file");
	};
	drop(plain);

	submitter.stop();
	driven.join().unwrap();
}

/// A facade nothing else names gives its file back at once: a take resolves on
/// the first poll rather than waiting for a wake that will never come.
#[test]
fn a_facade_with_no_operations_gives_its_file_back_at_once() {
	let directory = common::tempdir("completion-take").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"data").unwrap();
	let (_reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let file = File::new(std::fs::File::open(&path).unwrap(), &submitter);

	let plain = block_on(file.take()).expect("nothing else names the file");
	assert_eq!(plain.metadata().unwrap().len(), 4);
}

/// A transfer owns the facade's handle and submitter, so a poll-driven state
/// machine can keep it beside the facade, let the facade binding go before the
/// first poll, and still drive the transfer to its end.
#[test]
fn a_transfer_outlives_the_facade_binding_it_was_made_from() {
	/// What one hand-written poll loop holds across its polls.
	struct Reading {
		file: Option<File>,
		read: Transfer<std::fs::File>,
	}

	/// A state machine boxes what it is waiting for, which needs the future to
	/// be `'static`.
	fn assert_static<T: 'static>(transfer: T) -> T {
		transfer
	}

	let directory = common::tempdir("completion-future-outlives").unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, b"hello").unwrap();
	let (reactor, submitter) = Reactor::new(Config::default()).unwrap();
	let file = File::new(std::fs::File::open(&path).unwrap(), &submitter);

	let mut reactor = reactor;
	let driven = std::thread::spawn(move || {
		while !reactor.is_stopped() {
			reactor.poll(None).unwrap();
		}
	});

	let buffer: Arc<Vec<u8>> = Arc::new(vec![0; 5]);
	let read = assert_static(file.read_at(buffer.clone(), 0));
	let mut reading = Reading {
		file: Some(file),
		read,
	};
	// The facade binding goes: the transfer names the file through its own
	// clone of the handle.
	drop(reading.file.take());

	assert_eq!(block_on(reading.read).into_result().unwrap(), 5);
	assert_eq!(&buffer[..], b"hello");

	submitter.stop();
	driven.join().unwrap();
}
