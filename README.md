# completion-rt

Completion-model async I/O for Rust: io_uring on Linux, IOCP on Windows, and a readiness-based emulation (`polling`, with file I/O on `blocking`'s pool) on other unix.

```rust
use std::sync::Arc;

use completion_async::{Cancel, Config, Executor, Facade};

let mut executor = Executor::new(Config::default())?;
let file = Facade::with_executor(std::fs::File::open("data")?, &executor);
let space = executor.bind(Arc::new(memory)); // 5 bytes: a `Memory` you implement
let cancel = Cancel::new();

let read = executor.block_on(file.read_exact_at(space.whole(), 0).until(&cancel))?;
assert_eq!(read.into_result()?, 5);
```

## Semantics

- Dropping an operation's future returns only once the kernel has let go of its memory. Cancel first, with a `Cancel` and `until`; a drop that has to wait is a bug, which panics in a debug build and is logged in a release one.
- Memory is shared, not moved: it is bound to an executor once (`Executor::bind`) or lent to a scope (`Scope::lend`), and each operation names one `Span` of that space — `space.span(offset, len)`, or `space.gather(ranges)` for several. A range outside the memory fails the operation; it does not panic.
- `Facade::read_at`, `write_at`, `recv` and `send` make exactly one system call, so they can move fewer bytes than asked. `Facade`'s `read_exact_at`, `write_all_at`, `recv_exact` and `send_all` repeat the call until everything is moved; if one stops early, on an error or a cancel, `Outcome::value` still says how many bytes it moved.
- A `Reactor` and everything bound to it stay on the thread that built it. Only its waker and a `Cancel` cross threads.

## Usage notes

- On the portable backend, pass non-blocking descriptors for anything that is not a regular file: a blocking one stalls the reactor.
- io_uring requires Linux 5.11 or later: every blocking wait passes `IORING_ENTER_EXT_ARG` for its timeout. A stream's `hangup` needs 5.13, for a multishot poll; on an older kernel it fails.
- On io_uring, waits carry `IORING_ENTER_NO_IOWAIT` (Linux 6.15+), so an idle reactor is not counted as iowait; enable the `iowait` feature to leave the flag off.

## Development

- `task build`, `task check`, `task test`. `check` never rewrites files; `task fmt` does. `test` passes `CLI_ARGS` to nextest: `task test -- -E 'test(cancel)'`. Run `task lock` after changing dependencies.
- Logging goes through the `log` facade only; tests get a logger from `tests/common`.
- Test through the public API in `crates/*/tests/`, scratch files via `common::tempdir(tag)`. A bug fix starts with a test that fails without it.
- Comments say what the code cannot: ABI constants and their source, spec clauses, workarounds and why.
