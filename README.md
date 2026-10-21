# completion-rt

Completion-model async I/O for Rust: io_uring on Linux, IOCP on Windows, and a readiness-based emulation (`polling`, with file I/O on `blocking`'s pool) on every target — the default on unix other than Linux, and what `Config::backend` selects on Linux and Windows too, as `BackendKind::Portable` or the string `"portable"`.

```rust
use completion_async::{Buffer, Config, Executor, File};

let mut executor = Executor::new(Config::default())?;
let file = File::new(std::fs::File::open("data")?);
let buffer = Buffer::zeroed(5); // or `Buffer::new(Arc::new(memory))` for your own `Memory`
let scope = executor.scope();

let read = executor.block_on(async {
	scope.spawn(file.read_exact_at(buffer.whole(), 0)).await
})?;
assert_eq!(read.into_result()?, 5);
```

A resource is wrapped in the handle whose operations it takes, and each handle is built from a standard resource — `impl Into<std::fs::File>`, `impl Into<OwnedDescriptor>`, `impl Into<OwnedSocket>`: `File` for a file, `Stream` for a pipe or a stream socket, `Socket` for a datagram socket, `Listener` for a listening socket, and `Readiness` for a descriptor that only signals readiness. A handle binds no reactor, so it is `Send + Sync` and can be built on any thread; each operation resolves the reactor it submits to when it is polled.

## Semantics

- Dropping an operation's future cancels and detaches it without blocking: the reactor keeps its Buffer and descriptor alive until the kernel finishes, but the Span's bytes stay in flight until then. Cancel first, by running the operation as a task in a `Scope` — `executor.scope()`, a child of it, or `completion_async::scope()` — when the range is to be read or reused right away: `Scope::spawn` runs the task with every operation it submits filed under that scope, and `Scope::cancel` ends them and waits for the kernel.
- Memory is shared, not moved: a `Buffer` is a cheap reference-counted handle (`Buffer::zeroed`, `Buffer::from`, or `Buffer::new(Arc::new(memory))` for a `Memory` you implement), and each operation names one `Span` of that buffer — `buffer.span(offset, len)`, or `buffer.whole()` for all of it. A range outside the memory fails the operation; it does not panic. `BufferPool` hands out pre-faulted, page-aligned buffers from a fixed set of blocks, for repeated I/O.
- `File::read_at`, `File::write_at`, `Stream::recv` and `Stream::send` make exactly one system call, so they can move fewer bytes than asked, and report the count they moved. `read_exact_at`, `write_all_at`, `recv_exact` and `send_all` repeat the call until everything is moved; they report an `Outcome`, whose `value` still says how many bytes moved if one call stops early, on an error or a cancel.
- A file's transfers name their offset. A stream — a pipe, a pty, a TAP, a socket — has none to name, so its transfers move the descriptor's own position, and the same `Stream` serves a pipe and a socket.
- The reactor and the executor driving it stay on the thread that built it; only its waker crosses threads, along with the `Scope`s that cancel its work. A `Scope` is `Clone + Send + Sync`; a child scope is cancelled with its parent, and cancelling a child leaves the parent alone. Every executor has a root scope nothing can cancel, and the scopes handed out are children of it, so cancelling one never leaves the executor unable to run what follows. A handle is not bound to a reactor: an operation submits to the reactor of the executor whose `block_on` is polling it, or, outside one, the thread's default executor.

## Usage notes

- On the portable backend, pass non-blocking descriptors for anything that is not a regular file: a blocking one stalls the reactor. Windows polls sockets only; every other handle's calls run on the pool.
- io_uring requires Linux 5.11 or later: every blocking wait passes `IORING_ENTER_EXT_ARG` for its timeout. A stream's `hangup` needs 5.13, for a multishot poll; on an older kernel it fails.
- On io_uring, waits carry `IORING_ENTER_NO_IOWAIT` (Linux 6.15+), so an idle reactor is not counted as iowait; enable the `iowait` feature to leave the flag off.

## Development

- `task build`, `task check`, `task test`. `check` never rewrites files; `task fmt` does. `test` passes `CLI_ARGS` to nextest: `task test -- -E 'test(cancel)'`. Run `task lock` after changing dependencies.
- Logging goes through the `log` facade only; tests get a logger from `tests/common`.
- Test through the public API in `crates/*/tests/`, scratch files via `common::tempdir(tag)`. A bug fix starts with a test that fails without it.
- Comments say what the code cannot: ABI constants and their source, spec clauses, workarounds and why.
