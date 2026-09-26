# completion-rt

Completion-model async I/O for Rust: io_uring on Linux, IOCP on Windows, and a readiness-based emulation (`polling`, with file I/O on `blocking`'s pool) on other unix.

```rust
use std::sync::Arc;

use completion_rt::{Cancel, Config, Executor, Facade, Memory};

let mut executor = Executor::new(Config::default())?;
let file = Facade::new(std::fs::File::open("data")?, &executor.submitter());
let memory: Arc<dyn Memory> = Arc::new(vec![0u8; 5]);
let cancel = Cancel::new();

let read = executor.block_on(file.read_exact_at(memory, 0).until(&cancel))?;
assert_eq!(read.into_result()?, 5);
```

## Semantics

- Dropping a `Completion` returns only once the kernel has let go of its memory. Cancel first, with a `Cancel` and `until`; a drop that has to wait is a bug, which panics in a debug build and is logged in a release one.
- Memory is shared, not moved: an operation names an `Arc<dyn Memory>` and the extents to transfer.
- `Submitter` reads and writes make exactly one system call, so they can move fewer bytes than asked. `Facade`'s `read_exact_at`, `write_all_at`, `recv_exact` and `send_all` repeat the call until everything is moved; if one stops early, on an error or a cancel, `Outcome::value` still says how many bytes it moved.
- `Submitter` is `Send`; one thread drives the reactor, through `Executor::block_on` or `Reactor::poll`.

## Usage notes

- On the portable backend, pass non-blocking descriptors for anything that is not a regular file: a blocking one stalls the reactor.
- On io_uring, waits carry `IORING_ENTER_NO_IOWAIT` (Linux 6.15+), so an idle reactor is not counted as iowait; enable the `iowait` feature to leave the flag off.

## Development

`task build`, `task check`, `task test`; see [AGENTS.md](AGENTS.md). Rust 2024, MSRV 1.87.
