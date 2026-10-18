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

	let backend = Backend::new(&Config::default()).unwrap();
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
	assert_eq!(Backend::new(&config).unwrap().kind(), BackendKind::Portable);
	#[cfg(any(target_os = "linux", windows))]
	assert_ne!(BackendKind::default(), BackendKind::Portable);
}
