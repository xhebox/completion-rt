# Repository Guidelines

## Workflows

Use `task build`, `task check` and `task test`, never raw `cargo` or `rustup`. `task check` formats, runs ast-grep and cross-checks every backend's target; `task test` runs nextest on the host and passes `CLI_ARGS` through, e.g. `task test -- -E 'test(cancel)' --no-capture`. Run `task lock` after adding or removing a dependency; every other recipe is `--locked`.

## Logging

The library uses only the `log` facade; embedders install the logger. Tests under `tests/` get one from `tests/common`. Diagnosis is `log::debug!` with structured fields, tracing is `log::trace!`.

## Tests

Test through the public API in `tests/`; a test there drives real kernel I/O and states the behaviour a user relies on. Don't add unit tests after writing code: they restate it. Test a part in isolation only when the public API cannot reach the failure, and write down how it fails before writing it. A bug fix starts with a test that fails without the fix; a test that still passes with the fix reverted is removed.

## Test Scratch

Use `common::tempdir(tag)` in `tests/`, never `tempfile` or `std::env::temp_dir()` directly; ast-grep enforces this. Scratch paths are relative to the crate root so unix socket paths fit `sun_path`.

## FFI

`libc` is allowed only in `src/backend/uring.rs` and `src/backend/portable.rs`; everything else uses `rustix` or std.

## Comments

Comments state what the code cannot show: ABI constants with their source, spec clauses, workarounds with their reason. No narration or restating names.
