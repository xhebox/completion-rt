//! Waits on a real backend: a finished or cancelled wait leaves nothing behind,
//! and the backend goes idle once it has reported.

mod common;

mod waits {
	use std::io::Write;
	use std::net::{TcpListener, TcpStream};
	use std::time::Duration;

	use assert2::assert;
	use completion_backend::{AsDescriptor, Backend, BackendKind, Config, Op};
	use rstest::rstest;

	use super::common;

	/// How long one `poll` may block before `drain` polls again.
	const TIMEOUT: Duration = Duration::from_secs(5);

	fn pair() -> (TcpStream, TcpStream) {
		let listener = TcpListener::bind("127.0.0.1:0").unwrap();
		let writer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
		let (reader, _) = listener.accept().unwrap();
		(reader, writer)
	}

	/// Polls until `backend` is idle, returning each completion as `(id, data, succeeded)`.
	fn drain(backend: &mut Backend<u32>) -> Vec<(u64, u32, bool)> {
		let mut done = Vec::new();
		for _ in 0..8 {
			if backend.is_idle() {
				break;
			}
			backend
				.poll(Some(TIMEOUT), |id, data, result| {
					done.push((id, data, result.is_ok()))
				})
				.unwrap();
		}
		assert!(backend.is_idle(), "the backend never went idle");
		done
	}

	#[rstest]
	fn a_finished_wait_leaves_nothing_behind(
		#[values(BackendKind::default(), BackendKind::Portable)] kind: BackendKind,
	) {
		common::init_logging();
		let config = Config {
			backend: kind,
			..Config::default()
		};
		let mut backend: Backend<u32> = Backend::new(&config).unwrap();
		let (reader, mut writer) = pair();
		let id = backend
			.submit(reader.as_descriptor(), Op::Readable, 1)
			.unwrap();
		writer.write_all(b"x").unwrap();
		assert!(drain(&mut backend) == [(id, 1, true)]);

		drop((reader, writer));
		backend
			.poll(Some(Duration::ZERO), |_, _, _| {
				panic!("a finished wait completed again")
			})
			.unwrap();
		assert!(backend.is_idle());

		let (reader, mut writer) = pair();
		let next = backend
			.submit(reader.as_descriptor(), Op::Readable, 2)
			.unwrap();
		writer.write_all(b"y").unwrap();
		assert!(drain(&mut backend) == [(next, 2, true)]);
		assert!(id != next);
	}

	/// A wait cancelled before its descriptor turns ready is reported once, and the
	/// readiness that follows is not dispatched to it again.
	#[rstest]
	fn a_cancelled_wait_is_not_dispatched_when_its_descriptor_turns_ready(
		#[values(BackendKind::default(), BackendKind::Portable)] kind: BackendKind,
	) {
		common::init_logging();
		let config = Config {
			backend: kind,
			..Config::default()
		};
		let mut backend: Backend<u32> = Backend::new(&config).unwrap();
		let (reader, mut writer) = pair();
		let id = backend
			.submit(reader.as_descriptor(), Op::Readable, 7)
			.unwrap();
		backend.cancel(id);
		writer.write_all(b"x").unwrap();

		let done = drain(&mut backend);
		assert!(done.len() == 1, "the cancelled wait is reported once");
		assert!((done[0].0, done[0].1) == (id, 7));

		backend
			.poll(Some(Duration::ZERO), |_, _, _| {
				panic!("a cancelled wait was dispatched again")
			})
			.unwrap();
		assert!(backend.is_idle());
	}
}
