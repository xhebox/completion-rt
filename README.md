# completion-rt

[![Test](https://github.com/xhebox/completion-rt/actions/workflows/test.yml/badge.svg)](https://github.com/xhebox/completion-rt/actions/workflows/test.yml)
[![crates.io](https://img.shields.io/crates/v/completion-async.svg)](https://crates.io/crates/completion-async)
[![docs.rs](https://img.shields.io/docsrs/completion-async)](https://docs.rs/completion-async)
[![License: MIT](https://img.shields.io/crates/l/completion-async.svg)](https://github.com/xhebox/completion-rt/blob/main/LICENSE)

Completion-based async I/O for Rust: `io_uring` on Linux, IOCP on Windows, and a portable fallback based on [`polling`](https://github.com/smol-rs/polling) and [`blocking`](https://github.com/smol-rs/blocking).

| Crate | crates.io | Documentation | Description |
| --- | --- | --- | --- |
| [`completion-async`](crates/completion-async) | [crates.io](https://crates.io/crates/completion-async) | [docs.rs](https://docs.rs/completion-async) | Single-threaded executor, background reactor (`init_reactor`), `Buffer`, and async handles (`File`, `Stream`, `StreamSocket`, `DatagramSocket`, `Listener`). |
| [`completion-backend`](crates/completion-backend) | [crates.io](https://crates.io/crates/completion-backend) | [docs.rs](https://docs.rs/completion-backend) | Low-level completion driver (`Backend`, `Op`, `IoVec`) if you bring your own runtime. |

```rust
use completion_async::{Buffer, File, block_on};

let file = File::new(std::fs::File::open("data")?);
let mut buffer = Buffer::zeroed(5);
block_on(async {
	file.read_exact_at(buffer.clone(), 0).await
})??;
assert_eq!(buffer.get_mut().unwrap(), b"hello");
```

See the [`completion-async` documentation](https://docs.rs/completion-async) and [`completion-backend` documentation](https://docs.rs/completion-backend) for API details, cancellation semantics, and platform notes.

## Development

- `task check`, `task test`, `task doc`, `task bench`. `test` passes `CLI_ARGS` to nextest: `task test -- -E 'test(cancel)'`; `bench` passes `CLI_ARGS` to Divan: `task bench -- pipe`.
- Logging goes through the `log` facade only; tests get a logger from `tests/common`.
- Unit tests in `src/` cover self-contained state machines, invariants, and conversions without OS waits; integration tests in `crates/*/tests/` exercise the public API and OS backends end to end, with scratch files via `common::tempdir(tag)`. A bug fix starts with a test that fails without it.
