//! The unix side of the portable backend: a descriptor is a file descriptor,
//! which the poller watches as one, a stream on it is `read`/`write` — a
//! watched pipe takes no socket call — and its files take the positional
//! calls and the space reservations only a unix makes.

use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, RawFd};

use super::{
	IoVec, Kind, MAX_TRANSFER, Progress, SyncFlush, Transfer, blockable, counted, one_buffer,
};
use crate::backend::Response;
use crate::desc::{BorrowedDescriptor, OwnedDescriptor};

/// The most descriptors one control message carries.
const MAX_FDS: usize = 8;

/// A descriptor as the platform names one: a file descriptor.
pub(super) type Raw = RawFd;

/// The descriptor a poller registration and a socket call name.
pub(super) type Borrowed<'a> = BorrowedFd<'a>;

pub(super) fn raw(fd: BorrowedDescriptor<'_>) -> Raw {
	fd.as_raw_fd()
}

/// Borrows `raw` for as long as the reactor holds it: every caller either
/// deregisters the source or lets the call it makes return before the
/// descriptor goes.
pub(super) fn borrow(raw: Raw) -> io::Result<Borrowed<'static>> {
	// SAFETY: the descriptor stays open while the reactor holds it.
	Ok(unsafe { BorrowedFd::borrow_raw(raw) })
}

/// Whether `error` is a connect the kernel has begun and not finished:
/// `EINPROGRESS`, which Rust may report as `WouldBlock` here, is the handshake
/// going on, and the poller reports its outcome as the socket turning writable.
pub(super) fn in_progress(error: &io::Error) -> bool {
	error.kind() == io::ErrorKind::WouldBlock || error.raw_os_error() == Some(libc::EINPROGRESS)
}

/// Where a byte transfer on `raw` runs: a regular file has no readiness to wait
/// on, so every transfer on one is a pool job; every other descriptor is the
/// poller's.
pub(super) fn transfer(raw: Raw) -> Transfer {
	if regular(raw) {
		Transfer::Pool
	} else {
		Transfer::Watch
	}
}

/// Where a sync or a flush on `raw` runs: the pool for a regular file, and here
/// for a descriptor that is no file, which the pool has no job for.
pub(super) fn sync_flush(raw: Raw) -> io::Result<SyncFlush> {
	Ok(if regular(raw) {
		SyncFlush::Pool
	} else {
		SyncFlush::Inline
	})
}

/// A space reservation runs on the pool whichever descriptor names the range:
/// the call itself reports one it cannot take.
pub(super) fn reservable(_raw: Raw) -> io::Result<()> {
	Ok(())
}

/// A socket call and a wait for readiness run on the poller, whichever
/// descriptor names them.
pub(super) fn pollable(_raw: Raw) -> io::Result<()> {
	Ok(())
}

/// A send carrying descriptors is a control message, and `sendmsg` takes one on
/// any descriptor.
pub(super) fn control_message(_raw: Raw) -> io::Result<()> {
	Ok(())
}

fn regular(raw: Raw) -> bool {
	let Ok(stat) = rustix::fs::fstat(unsafe { BorrowedFd::borrow_raw(raw) }) else {
		return false;
	};
	rustix::fs::FileType::from_raw_mode(stat.st_mode as _) == rustix::fs::FileType::RegularFile
}

/// Runs a file call on the pool.
pub(super) fn run_file(
	raw: Raw,
	fdoff: u64,
	kind: Kind,
	buffer: Option<IoVec>,
) -> io::Result<Response> {
	let fd = unsafe { BorrowedFd::borrow_raw(raw) };
	match kind {
		Kind::Fsync => rustix::fs::fsync(fd)
			.map(|()| Response::Done)
			.map_err(io::Error::from),
		Kind::Flush => flush(fd).map(|()| Response::Done),
		Kind::Allocate { offset, len } => allocate(fd, offset, len).map(|()| Response::Done),
		Kind::Punch { offset, len } => punch(fd, offset, len).map(|()| Response::Done),
		Kind::Read | Kind::Write => {
			let Some(buffer) = buffer else {
				return Ok(Response::Count(0));
			};
			let (ptr, len) = (buffer.ptr, buffer.len.min(MAX_TRANSFER));
			// SAFETY: the reactor holds the memory behind this pointer until the job lands; the syscall copies through its pointer without forming a Rust reference.
			let offset = fdoff as libc::off_t;
			count(unsafe {
				match kind {
					Kind::Read => libc::pread(fd.as_raw_fd(), ptr.cast(), len, offset),
					Kind::Write => libc::pwrite(fd.as_raw_fd(), ptr.cast(), len, offset),
					_ => unreachable!("handled above"),
				}
			})
			.map(Response::Count)
		}
	}
}

/// Grows the file to the range's end, so the reserved space is held against
/// it; a file already ending past the range keeps its length.
fn allocate(fd: BorrowedFd<'_>, offset: u64, len: u64) -> io::Result<()> {
	let end = offset
		.checked_add(len)
		.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
	let before = rustix::fs::fstat(fd)?.st_size.max(0) as u64;
	if end > before {
		preallocate(fd, offset, len, end)?;
		rustix::fs::ftruncate(fd, end)?;
	}
	Ok(())
}

#[cfg(target_vendor = "apple")]
fn preallocate(fd: BorrowedFd<'_>, _offset: u64, _len: u64, end: u64) -> io::Result<()> {
	// F_PREALLOCATE, asked for a contiguous run first and a scattered one after,
	// as rustix's own fallocate does here.
	let mut store = libc::fstore_t {
		fst_flags: libc::F_ALLOCATECONTIG,
		fst_posmode: libc::F_PEOFPOSMODE,
		fst_offset: 0,
		fst_length: end as libc::off_t,
		fst_bytesalloc: 0,
	};
	if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_PREALLOCATE, &store) } != -1 {
		return Ok(());
	}
	let error = io::Error::last_os_error();
	if error.raw_os_error() != Some(libc::ENOTSUP) {
		return Err(error);
	}
	store.fst_flags = libc::F_ALLOCATEALL;
	if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_PREALLOCATE, &store) } == -1 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

#[cfg(not(target_vendor = "apple"))]
fn preallocate(fd: BorrowedFd<'_>, offset: u64, len: u64, _end: u64) -> io::Result<()> {
	let status =
		unsafe { libc::posix_fallocate(fd.as_raw_fd(), offset as libc::off_t, len as libc::off_t) };
	if status != 0 {
		return Err(io::Error::from_raw_os_error(status));
	}
	Ok(())
}

fn punch(fd: BorrowedFd<'_>, offset: u64, len: u64) -> io::Result<()> {
	#[cfg(target_vendor = "apple")]
	{
		// `rustix::fs::fallocate` has no hole-punching mode here; F_PUNCHHOLE is
		// the fcntl that does it, and it keeps the length. It takes only a range
		// that is a multiple of the file system block size and answers EINVAL
		// otherwise, so the aligned middle goes to the kernel and the two
		// partial blocks at the ends are written over: a hole the kernel will
		// not take is still the caller's bytes.
		let end = offset
			.checked_add(len)
			.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
		let block = block_size(fd)?;
		let first = offset.next_multiple_of(block).min(end);
		let last = (end - end % block).max(first);
		if first > offset {
			zero(fd, offset, first - offset)?;
		}
		if end > last {
			zero(fd, last, end - last)?;
		}
		if last > first {
			let request = libc::fpunchhole_t {
				fp_flags: 0,
				reserved: 0,
				fp_offset: first as libc::off_t,
				fp_length: (last - first) as libc::off_t,
			};
			if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_PUNCHHOLE, &request) } == -1 {
				return Err(io::Error::last_os_error());
			}
		}
		Ok(())
	}
	#[cfg(target_os = "linux")]
	{
		// The flags the ring backend punches with: the range reads back as zeros
		// and the file keeps its length.
		rustix::fs::fallocate(
			fd,
			rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
			offset,
			len,
		)
		.map_err(io::Error::from)
	}
	#[cfg(all(not(target_vendor = "apple"), not(target_os = "linux")))]
	{
		let _ = (fd, offset, len);
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"punch is not implemented on this platform",
		))
	}
}

/// Check `close(dup(fd))` for deferred write-back errors; Rust descriptor drops discard them. Do not retry EINTR: POSIX leaves descriptor state unspecified, and Linux may already have reused the number.
fn flush(fd: BorrowedFd<'_>) -> io::Result<()> {
	let dup = unsafe { libc::dup(fd.as_raw_fd()) };
	if dup < 0 {
		return Err(io::Error::last_os_error());
	}
	if unsafe { libc::close(dup) } < 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

/// The granularity `F_PUNCHHOLE` accepts: the file system's own block size, at
/// least the page size a hole is checked against.
#[cfg(target_vendor = "apple")]
fn block_size(fd: BorrowedFd<'_>) -> io::Result<u64> {
	let stat = rustix::fs::fstat(fd)?;
	let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
	let page = if page > 0 { page as u64 } else { 4096 };
	Ok((stat.st_blksize.max(0) as u64).max(page))
}

/// Writes zeros over the range, for the ends a hole cannot take.
#[cfg(target_vendor = "apple")]
fn zero(fd: BorrowedFd<'_>, offset: u64, len: u64) -> io::Result<()> {
	const CHUNK: [u8; 4096] = [0; 4096];
	let mut done = 0u64;
	while done < len {
		let take = CHUNK.len().min((len - done) as usize);
		// SAFETY: the buffer outlives the call, which only reads from it.
		let moved = unsafe {
			libc::pwrite(
				fd.as_raw_fd(),
				CHUNK.as_ptr().cast(),
				take,
				(offset + done) as libc::off_t,
			)
		};
		let moved = count(moved)?;
		if moved == 0 {
			return Err(io::Error::from(io::ErrorKind::WriteZero));
		}
		done += moved as u64;
	}
	Ok(())
}

fn count(result: libc::ssize_t) -> io::Result<usize> {
	if result < 0 {
		Err(io::Error::last_os_error())
	} else {
		Ok(result as usize)
	}
}

/// Receives into the one buffer `buffer` names on a socket, also collecting the
/// descriptors the call carried in a control message.
pub(super) fn recv_with_fds(fd: Raw, buffer: Option<IoVec>) -> io::Result<Progress> {
	// SAFETY: the descriptor stays open for the operation's flight.
	let fd = unsafe { BorrowedFd::borrow_raw(fd) };
	let (ptr, len) = one_buffer(buffer)?;
	// The received descriptors are this call's own: nothing waits on them
	// after the response carries them out.
	let mut fds: Vec<OwnedDescriptor> = Vec::new();
	let mut iov = libc::iovec {
		iov_base: ptr.cast(),
		iov_len: len,
	};
	let mut space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
	let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
	msg.msg_iov = &mut iov;
	msg.msg_iovlen = 1;
	msg.msg_control = space.as_mut_ptr().cast();
	msg.msg_controllen = space.len() as _;
	// MSG_DONTWAIT: the caller's socket may be in blocking mode, and a call
	// that blocked would stall the reactor thread behind it.
	let result = count(unsafe { libc::recvmsg(fd.as_raw_fd(), &mut msg, libc::MSG_DONTWAIT) });
	match blockable(result)? {
		Some(moved) => {
			take_fds(&msg, &mut fds);
			Ok(Progress::Done(Response::CountWithFds(moved, fds)))
		}
		None => Ok(Progress::Blocked),
	}
}

/// Reads into the one buffer `buffer` names, on a watched descriptor.
///
/// A watched descriptor here may be a pipe, which only `read`/`write` take; a
/// socket takes them too, where Windows has only `recv`/`send`.
pub(super) fn read(fd: Raw, buffer: Option<IoVec>) -> io::Result<Progress> {
	let Some(buffer) = buffer else {
		return Ok(Progress::Done(Response::Count(0)));
	};
	let (ptr, len) = (buffer.ptr, buffer.len.min(MAX_TRANSFER));
	// SAFETY: the reactor holds the memory this buffer names until the
	// operation lands, and the call is the only mover of the range while the
	// slice stands.
	let bytes = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
	counted(rustix::io::read(borrow(fd)?, bytes).map_err(io::Error::from))
}

/// Writes from the one buffer `buffer` names, on a watched descriptor.
pub(super) fn write(fd: Raw, buffer: Option<IoVec>) -> io::Result<Progress> {
	let Some(buffer) = buffer else {
		return Ok(Progress::Done(Response::Count(0)));
	};
	let (ptr, len) = (buffer.ptr, buffer.len.min(MAX_TRANSFER));
	// SAFETY: the reactor holds the memory this buffer names until the
	// operation lands, and the call only reads it.
	let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
	counted(rustix::io::write(borrow(fd)?, bytes).map_err(io::Error::from))
}

/// Sends the one buffer `buffer` names on `fd`, carrying `fds` in a control
/// message: the descriptors are this operation's own, and the peer holds its own
/// copies once the call takes the bytes, so they go with the call.
pub(super) fn send_with_fds(
	fd: Raw,
	buffer: Option<IoVec>,
	fds: &mut Vec<OwnedDescriptor>,
) -> io::Result<Progress> {
	// SAFETY: the descriptor stays open for the operation's flight.
	let fd = unsafe { BorrowedFd::borrow_raw(fd) };
	if fds.len() > MAX_FDS {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"too many descriptors",
		));
	}
	let (ptr, len) = one_buffer(buffer)?;
	let carried: Vec<libc::c_int> = fds.iter().map(|fd| fd.as_raw_fd()).collect();
	let mut iov = libc::iovec {
		iov_base: ptr.cast(),
		iov_len: len,
	};
	let mut space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
	let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
	msg.msg_iov = &mut iov;
	msg.msg_iovlen = 1;
	msg.msg_control = space.as_mut_ptr().cast();
	msg.msg_controllen =
		unsafe { libc::CMSG_LEN((carried.len() * size_of::<libc::c_int>()) as u32) } as _;
	unsafe {
		let header = space.as_mut_ptr().cast::<libc::cmsghdr>();
		(*header).cmsg_level = libc::SOL_SOCKET;
		(*header).cmsg_type = libc::SCM_RIGHTS;
		(*header).cmsg_len = libc::CMSG_LEN((carried.len() * size_of::<libc::c_int>()) as u32) as _;
		let data = libc::CMSG_DATA(header).cast::<libc::c_int>();
		for (index, raw) in carried.iter().enumerate() {
			*data.add(index) = *raw;
		}
	}
	// MSG_DONTWAIT: the caller's socket may be in blocking mode, and a call that
	// blocked would stall the reactor thread behind it.
	let result = count(unsafe { libc::sendmsg(fd.as_raw_fd(), &msg, libc::MSG_DONTWAIT) });
	match blockable(result)? {
		Some(moved) => {
			fds.clear();
			Ok(Progress::Done(Response::Count(moved)))
		}
		None => Ok(Progress::Blocked),
	}
}

fn take_fds(msg: &libc::msghdr, out: &mut Vec<OwnedDescriptor>) {
	let mut header = unsafe { libc::CMSG_FIRSTHDR(msg) };
	while !header.is_null() {
		let (level, kind) = unsafe { ((*header).cmsg_level, (*header).cmsg_type) };
		if level == libc::SOL_SOCKET && kind == libc::SCM_RIGHTS {
			let count = ((unsafe { (*header).cmsg_len } as usize)
				- (unsafe { libc::CMSG_LEN(0) }) as usize)
				/ size_of::<libc::c_int>();
			let data = unsafe { libc::CMSG_DATA(header) }.cast::<libc::c_int>();
			for index in 0..count {
				let raw = unsafe { *data.add(index) };
				// SAFETY: the kernel put this descriptor in the process, and
				// nothing else in it owns the number.
				let descriptor = unsafe { OwnedDescriptor::from_raw_fd(raw) };
				let _ = rustix::io::fcntl_setfd(&descriptor, rustix::io::FdFlags::CLOEXEC);
				out.push(descriptor);
			}
		}
		header = unsafe { libc::CMSG_NXTHDR(msg, header) };
	}
}
