//! A [`Config`] names the backend it runs on with a [`BackendKind`], which also
//! parses from the string a configuration file would carry.

mod common;

use completion_backend::{Backend, BackendKind, Config};

/// The default is the target's own completion mechanism: the ring where there
/// is one, IOCP on Windows, the portable backend on the other unix targets.
#[test]
fn a_kind_defaults_to_the_targets_own_backend() {
	#[cfg(target_os = "linux")]
	assert_eq!(BackendKind::default(), BackendKind::Uring);
	#[cfg(all(unix, not(target_os = "linux")))]
	assert_eq!(BackendKind::default(), BackendKind::Portable);
	#[cfg(windows)]
	assert_eq!(BackendKind::default(), BackendKind::Iocp);

	let backend: Backend = Backend::new(&Config::default()).unwrap();
	assert_eq!(backend.kind(), BackendKind::default());
}

/// Every variant a target offers round-trips through its name, whatever case
/// the name arrives in.
#[test]
fn a_kind_round_trips_through_its_name() {
	use strum::VariantArray;

	for &kind in BackendKind::VARIANTS {
		let name = kind.to_string();
		assert_eq!(name.parse::<BackendKind>().unwrap(), kind);
		assert_eq!(name.to_uppercase().parse::<BackendKind>().unwrap(), kind);
		let static_name: &'static str = kind.into();
		assert_eq!(static_name, name);
	}

	assert!("portableish".parse::<BackendKind>().is_err());
}

/// Every target offers the portable backend, whether or not it is the default:
/// on Linux it swaps the ring for `polling`'s readiness and `blocking`'s pool,
/// and on Windows it swaps IOCP for them.
#[test]
fn every_target_offers_the_portable_backend() {
	let config = Config {
		backend: "portable".parse().unwrap(),
		..Config::default()
	};
	assert_eq!(
		Backend::<()>::new(&config).unwrap().kind(),
		BackendKind::Portable
	);
	#[cfg(any(target_os = "linux", windows))]
	assert_ne!(BackendKind::default(), BackendKind::Portable);
}

/// A backend hands its mechanism to whichever thread drives it: the reactor
/// moves one between threads, so every mechanism it can pick is `Send`.
#[test]
fn a_backend_is_send() {
	fn assert_send<T: Send>() {}
	assert_send::<Backend>();
}

#[cfg(unix)]
mod waits {
	use std::io::Write;
	use std::time::Duration;

	use completion_backend::{AsDescriptor, Backend, Config, Op};

	/// How long one `poll` may block before `drain` polls again.
	const TIMEOUT: Duration = Duration::from_secs(5);

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

	/// A finished wait leaves nothing behind: its descriptor can close, the backend
	/// goes idle, and a later wait on a new descriptor completes as if the first
	/// had never been submitted.
	#[test]
	fn a_finished_wait_leaves_nothing_behind() {
		let mut backend: Backend<u32> = Backend::new(&Config::default()).unwrap();
		let (reader, mut writer) = std::io::pipe().unwrap();
		let id = backend
			.submit(reader.as_descriptor(), Op::Readable, 1)
			.unwrap();
		writer.write_all(b"x").unwrap();
		assert_eq!(drain(&mut backend), [(id, 1, true)]);

		drop((reader, writer));
		backend
			.poll(Some(Duration::ZERO), |_, _, _| {
				panic!("a finished wait completed again")
			})
			.unwrap();
		assert!(backend.is_idle());

		let (reader, mut writer) = std::io::pipe().unwrap();
		let next = backend
			.submit(reader.as_descriptor(), Op::Readable, 2)
			.unwrap();
		writer.write_all(b"y").unwrap();
		assert_eq!(drain(&mut backend), [(next, 2, true)]);
		assert_ne!(id, next);
	}

	/// A wait cancelled before its descriptor turns ready is reported once, and the
	/// readiness that follows is not dispatched to it again.
	#[test]
	fn a_cancelled_wait_is_not_dispatched_when_its_descriptor_turns_ready() {
		let mut backend: Backend<u32> = Backend::new(&Config::default()).unwrap();
		let (reader, mut writer) = std::io::pipe().unwrap();
		let id = backend
			.submit(reader.as_descriptor(), Op::Readable, 7)
			.unwrap();
		backend.cancel(id);
		writer.write_all(b"x").unwrap();

		let done = drain(&mut backend);
		assert_eq!(done.len(), 1, "the cancelled wait is reported once");
		assert_eq!((done[0].0, done[0].1), (id, 7));

		backend
			.poll(Some(Duration::ZERO), |_, _, _| {
				panic!("a cancelled wait was dispatched again")
			})
			.unwrap();
		assert!(backend.is_idle());
	}
}
