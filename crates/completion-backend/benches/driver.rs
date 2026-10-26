//! Driver costs on their own: an empty poll, a wake, a submit that is cancelled
//! and reaped, and reads of a page-cached file at several queue depths.
//!
//! Every file read runs on a warm page cache, so the numbers are the driver's,
//! not the disk's.

use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::time::Duration;

use assert2::assert;
use completion_backend::{AsDescriptor, Backend, BackendKind, Config, IoVec, Op, Response};
use divan::Bencher;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

/// Bytes in the file the read benchmarks draw from.
const FILE_LEN: usize = 64 << 10;

/// Bytes per read, and per buffer a read names.
const BLOCK: usize = 4096;

fn backend(kind: BackendKind) -> Backend<()> {
	Backend::new(&Config {
		backend: kind,
		..Config::default()
	})
	.unwrap()
}

/// A connected loopback pair: the reader is the descriptor a wait names.
fn pair() -> (TcpStream, TcpStream) {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let writer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
	let (reader, _) = listener.accept().unwrap();
	(reader, writer)
}

/// A file of `FILE_LEN` bytes under `target/test-tmp`, opened for the backend.
/// The directory goes with the returned value, so bind it for the bench.
fn scratch_file() -> (tempfile::TempDir, std::fs::File) {
	std::fs::create_dir_all("target/test-tmp").unwrap();
	let directory = tempfile::Builder::new()
		.prefix("bench-driver")
		.tempdir_in("target/test-tmp")
		.unwrap();
	let path = directory.path().join("data");
	std::fs::write(&path, vec![0x5a; FILE_LEN]).unwrap();
	(directory, open(&path))
}

/// Opens `path` for the backend: on Windows a completion port only takes a
/// handle opened for overlapped I/O.
fn open(path: &Path) -> std::fs::File {
	#[cfg(windows)]
	{
		use std::os::windows::fs::OpenOptionsExt;
		use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;
		std::fs::OpenOptions::new()
			.read(true)
			.custom_flags(FILE_FLAG_OVERLAPPED)
			.open(path)
			.unwrap()
	}
	#[cfg(not(windows))]
	{
		std::fs::File::open(path).unwrap()
	}
}

#[divan::bench(args = [BackendKind::default(), BackendKind::Portable])]
fn poll_empty(bencher: Bencher, kind: BackendKind) {
	let mut backend = backend(kind);
	bencher.bench_local(|| {
		backend.poll(Some(Duration::ZERO), |_, _, _| {}).unwrap();
	});
}

#[divan::bench(args = [BackendKind::default(), BackendKind::Portable])]
fn waker_roundtrip(bencher: Bencher, kind: BackendKind) {
	let mut backend = backend(kind);
	let waker = backend.waker();
	bencher.bench_local(|| {
		waker.wake();
		backend.poll(Some(Duration::ZERO), |_, _, _| {}).unwrap();
	});
}

#[divan::bench(args = [BackendKind::default(), BackendKind::Portable])]
fn wait_cancel_reap(bencher: Bencher, kind: BackendKind) {
	let mut backend: Backend<()> = backend(kind);
	let (reader, _writer) = pair();
	bencher.bench_local(|| {
		let id = backend
			.submit(reader.as_descriptor(), Op::Readable, ())
			.unwrap();
		backend.cancel(id);
		while !backend.is_idle() {
			backend.poll(None, |_, _, _| {}).unwrap();
		}
	});
}

#[divan::bench(args = [1, 16, 64])]
fn read_at_default(bencher: Bencher, depth: usize) {
	bench_read_at(bencher, BackendKind::default(), depth);
}

#[divan::bench(args = [1, 16, 64])]
fn read_at_portable(bencher: Bencher, depth: usize) {
	bench_read_at(bencher, BackendKind::Portable, depth);
}

/// Submits `depth` reads of `BLOCK` bytes, then reaps them all, per iteration.
fn bench_read_at(bencher: Bencher, kind: BackendKind, depth: usize) {
	let mut backend = backend(kind);
	let (_directory, file) = scratch_file();
	let mut buffers: Vec<Vec<u8>> = (0..depth).map(|_| vec![0u8; BLOCK]).collect();
	bencher
		.counter(divan::counter::ItemsCount::new(depth))
		.bench_local(|| {
			for (index, buffer) in buffers.iter_mut().enumerate() {
				let offset = ((index * BLOCK) % FILE_LEN) as u64;
				let buf = IoVec {
					ptr: buffer.as_mut_ptr(),
					len: buffer.len(),
				};
				backend
					.submit(
						file.as_descriptor(),
						Op::Read {
							offset: Some(offset),
							buf,
						},
						(),
					)
					.unwrap();
			}
			while !backend.is_idle() {
				backend
					.poll(None, |_, _, result| {
						assert!(let Ok(Response::Count(BLOCK)) = result);
					})
					.unwrap();
			}
		});
}

fn main() {
	divan::main();
}
