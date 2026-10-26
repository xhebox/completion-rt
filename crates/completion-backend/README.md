# completion-backend

[![crates.io](https://img.shields.io/crates/v/completion-backend.svg)](https://crates.io/crates/completion-backend)
[![docs.rs](https://img.shields.io/docsrs/completion-backend)](https://docs.rs/completion-backend)
[![License: MIT](https://img.shields.io/crates/l/completion-backend.svg)](https://github.com/xhebox/completion-rt/blob/main/LICENSE)

Low-level OS completion driver for Linux (`io_uring`), Windows (IOCP), and a portable [`polling`](https://github.com/smol-rs/polling) + [`blocking`](https://github.com/smol-rs/blocking) fallback.

See the [API documentation on docs.rs](https://docs.rs/completion-backend) for usage and safety contracts, or [`completion-async`](https://crates.io/crates/completion-async) for the high-level async runtime and handles.
