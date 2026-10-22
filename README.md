# completion-rt

[![Test](https://github.com/xhebox/completion-rt/actions/workflows/test.yml/badge.svg)](https://github.com/xhebox/completion-rt/actions/workflows/test.yml)
[![crates.io](https://img.shields.io/crates/v/completion-async.svg)](https://crates.io/crates/completion-async)
[![docs.rs](https://img.shields.io/docsrs/completion-async)](https://docs.rs/completion-async)
[![License: MIT](https://img.shields.io/crates/l/completion-async.svg)](https://github.com/xhebox/completion-rt/blob/main/LICENSE)

Completion-based async I/O for Rust: `io_uring` on Linux, IOCP on Windows, and a portable fallback based on [`polling`](https://github.com/smol-rs/polling) and [`blocking`](https://github.com/smol-rs/blocking).

- `completion-async` provides a single-threaded executor and async handles (`File`, `Stream`, `StreamSocket`, `DatagramSocket`, `Listener`, `Readiness`).
- `completion-backend` provides the low-level completion driver (`Backend`, `Op`, `IoVec`) if you bring your own runtime.

```rust
use completion_async::{Buffer, File, block_on};

let file = File::new(std::fs::File::open("data")?);
let mut buffer = Buffer::zeroed(5);
let read = block_on(async {
	// A clone is a cheap handle on the same bytes.
	file.read_exact_at(buffer.clone(), 0).await
})?;
assert_eq!(read.into_result()?, 5);
assert_eq!(buffer.get_mut().unwrap(), b"hello");
```

## How it works

### Handles and executors

Wrap a standard OS resource in the matching handle:

- `File` for positioned file I/O (`read_at`, `write_at`, `sync`, `flush`, `allocate`, `punch`).
- `Stream` for non-seekable byte streams such as pipes (`pipe`, `read`, `write`, `read_exact`, `write_all`).
- `StreamSocket` for stream sockets (`recv`, `send`, `recv_exact`, `send_all`, `shutdown`, `hangup`).
- `DatagramSocket` for datagram sockets (`recv`, `send`).
- `Listener` for listening sockets (`accept`).
- `Readiness` for descriptors and waitable handles that only signal readiness (such as a socket, an `eventfd`, `pidfd`, or `signalfd` on unix, or an event or process handle on Windows).

Handles are `Send + Sync`, cheap to clone, and not bound to a specific executor: each operation submits to the executor driving the current thread when its future is first polled.

Single-operation methods (`read_at`, `write_at`, `read`, `write`, `recv`, `send`) submit one I/O operation and return the number of bytes transferred, which may be less than requested. Loop methods (`read_exact_at`, `write_all_at`, `read_exact`, `write_all`, `recv_exact`, `send_all`) keep submitting until the `Buffer` is full and return an `Outcome`, preserving the byte count already transferred if an error or unexpected EOF occurs mid-way.

### Buffers

Every I/O method takes its buffer by value as `impl AsIoVec`.

A `Buffer` implements `AsIoVec`, and cloning one is cheap and shares the same bytes. Create a `Buffer` with `Buffer::zeroed`, `Buffer::copy_from` (copies a slice), or `Buffer::from` (takes a `Vec<u8>` or `Box<[u8]>` without copying). Pass `buffer.clone()` to an operation and keep the original: once the operation has landed, `buffer.get_mut()` returns the bytes, and it returns `None` while another clone or an in-flight operation still names them.

Implement `unsafe trait AsIoVec` yourself if `Buffer` does not suit, such as a slice of a larger allocation, or a shared memory region.

### Cancellation

Dropping an in-flight I/O future requests asynchronous cancellation and returns immediately without blocking. The runtime keeps the buffer and descriptor alive in the background until the kernel finishes with them.

- For `File`, dropping a read or write is safe to retry at the same offset.
- For `Stream`, `StreamSocket`, and `DatagramSocket`, the operation may complete in the kernel before cancellation takes effect, so any bytes already consumed are lost.

## Platform notes

- Linux `io_uring` requires kernel 5.11 or newer (`StreamSocket::hangup` requires 5.13+). On 6.15+, the `completion-backend/iowait` feature re-enables counting idle ring waits as system iowait.
- On Windows with the IOCP backend, handles passed to `File::new` and `Stream::new` must be opened with `FILE_FLAG_OVERLAPPED` (use `completion_async::OpenOptions` and `Stream::pipe`, which set it automatically).

## Development

- `task check`, `task test`. `test` passes `CLI_ARGS` to nextest: `task test -- -E 'test(cancel)'`.
- Logging goes through the `log` facade only; tests get a logger from `tests/common`.
- Test through the public API in `crates/*/tests/`, scratch files via `common::tempdir(tag)`. A bug fix starts with a test that fails without it.
