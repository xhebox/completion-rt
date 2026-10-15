//! Linux `io_uring` backend.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::OwnedDescriptor;
use crate::desc::{RawHandle, from_raw_socket};
use crate::error::Error;
use crate::memory::{IoVec, IoVecs};
use io_uring::{EnterFlags, IoUring, cqueue, opcode, squeue, types};
use rustix::io::Errno;

use super::park::{Doorbell, PARKED, Park};
use super::{Backend, Response};
use crate::backend::Config;

const MIN_RING_ENTRIES: u32 = 2;

/// Maximum descriptors per control message.
const MAX_FDS: usize = 8;

fn readable_mask() -> u32 {
	rustix::event::PollFlags::IN.bits() as u32
}

fn writable_mask() -> u32 {
	rustix::event::PollFlags::OUT.bits() as u32
}

/// The token a cancel is reported under: no completion waits for it. Ids
/// handed to operations start at one.
const CANCEL_TOKEN: u64 = 0;

/// The token the futex wait on the park's state word is reported under. Ids
/// handed to operations start at one and would have to wrap the counter to
/// reach this.
const FUTEX_TOKEN: u64 = u64::MAX;

/// `FUTEX2_SIZE_U32`: the state word is a 32-bit futex (`include/uapi/linux/futex.h`).
const FUTEX2_SIZE_U32: u32 = 0x02;

/// Wakes a park sleeping on its state word through the ring's `FUTEX_WAIT`:
/// `FUTEX_WAKE` on the word completes that wait, so `enter` returns.
struct FutexWake;

impl Doorbell for FutexWake {
	fn ring(&self, word: &std::sync::atomic::AtomicU32) {
		// Shared, not private: the ring queues its `FUTEX_WAIT` on this same
		// address, and a private wake would not match it.
		let _ = rustix::thread::futex::wake(word, rustix::thread::futex::Flags::empty(), 1);
	}
}

/// An IOPOLL ring spins instead of sleeping, and its spin reads the state
/// word itself (`Park::woke`), so there is nothing to ring.
struct Spin;

impl Doorbell for Spin {
	fn ring(&self, _word: &std::sync::atomic::AtomicU32) {}
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
	Count,
	CountWithFds,
	Accepted,
	Done,
}

enum Step {
	Read {
		fd: RawHandle,
		fdoff: u64,
		buffers: IoVecs,
	},
	Write {
		fd: RawHandle,
		fdoff: u64,
		buffers: IoVecs,
	},
	Recv {
		fd: RawHandle,
		buffers: IoVecs,
	},
	RecvWithFds {
		fd: RawHandle,
		buffers: IoVecs,
	},
	Send {
		fd: RawHandle,
		buffers: IoVecs,
		fds: Vec<OwnedDescriptor>,
	},
	Fsync {
		fd: RawHandle,
	},
	Flush {
		fd: RawHandle,
	},
	Fallocate {
		fd: RawHandle,
		offset: u64,
		len: u64,
		mode: i32,
	},
	Accept {
		fd: RawHandle,
	},
	Readable {
		fd: RawHandle,
	},
	Writable {
		fd: RawHandle,
	},
	Hangup {
		fd: RawHandle,
	},
}

impl Step {
	fn kind(&self) -> Kind {
		match self {
			Step::RecvWithFds { .. } => Kind::CountWithFds,
			Step::Accept { .. } => Kind::Accepted,
			Step::Fsync { .. }
			| Step::Flush { .. }
			| Step::Fallocate { .. }
			| Step::Readable { .. }
			| Step::Writable { .. }
			| Step::Hangup { .. } => Kind::Done,
			_ => Kind::Count,
		}
	}
}

/// The kernel dereferences the header and its buffers at execution, so all must remain stable until completion.
struct Control {
	msg: Box<libc::msghdr>,
	_iov: Box<libc::iovec>,
	_cmsg: Box<[u8]>,
	/// Descriptors must stay open until the kernel executes the send.
	_fds: Vec<OwnedDescriptor>,
}

// SAFETY: every pointer in `msg` names one of this value's own heap buffers
// (`iov`, `cmsg`), whose addresses do not change when the value moves — which
// is what the ring needs, and what makes sending it to another thread sound.
unsafe impl Send for Control {}

impl Control {
	fn receive(buf: *mut u8, len: usize) -> Control {
		Control::new(buf, len, Vec::new(), true)
	}

	fn send(buf: *const u8, len: usize, fds: Vec<OwnedDescriptor>) -> Control {
		Control::new(buf.cast_mut(), len, fds, false)
	}

	fn new(buf: *mut u8, len: usize, fds: Vec<OwnedDescriptor>, receiving: bool) -> Control {
		let mut iov = Box::new(libc::iovec {
			iov_base: buf.cast(),
			iov_len: len,
		});
		let space =
			(unsafe { libc::CMSG_SPACE((MAX_FDS * size_of::<libc::c_int>()) as u32) }) as usize;
		let mut cmsg = vec![0u8; space].into_boxed_slice();
		let mut msg: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
		msg.msg_iov = iov.as_mut() as *mut libc::iovec;
		msg.msg_iovlen = 1;
		let control_len = if fds.is_empty() {
			cmsg.len()
		} else {
			(unsafe { libc::CMSG_LEN((fds.len() * size_of::<libc::c_int>()) as u32) }) as usize
		};
		if receiving || !fds.is_empty() {
			msg.msg_control = cmsg.as_mut_ptr().cast();
			msg.msg_controllen = control_len as _;
		}
		if !fds.is_empty() {
			unsafe {
				let header = cmsg.as_mut_ptr().cast::<libc::cmsghdr>();
				(*header).cmsg_level = libc::SOL_SOCKET;
				(*header).cmsg_type = libc::SCM_RIGHTS;
				(*header).cmsg_len = msg.msg_controllen as _;
				let data = libc::CMSG_DATA(header).cast::<libc::c_int>();
				for (index, fd) in fds.iter().enumerate() {
					*data.add(index) = fd.as_raw_fd();
				}
			}
		}
		Control {
			msg,
			_iov: iov,
			_cmsg: cmsg,
			_fds: fds,
		}
	}

	fn message(&mut self) -> *mut libc::msghdr {
		&mut *self.msg
	}

	fn take_fds(&mut self) -> Vec<OwnedDescriptor> {
		let mut out = Vec::new();
		let msg: *mut libc::msghdr = &mut *self.msg;
		let mut header = unsafe { libc::CMSG_FIRSTHDR(msg) };
		while !header.is_null() {
			let (level, kind) = unsafe { ((*header).cmsg_level, (*header).cmsg_type) };
			if level == libc::SOL_SOCKET && kind == libc::SCM_RIGHTS {
				let length = (unsafe { (*header).cmsg_len } as usize)
					- (unsafe { libc::CMSG_LEN(0) }) as usize;
				let count = length / size_of::<libc::c_int>();
				let data = unsafe { libc::CMSG_DATA(header) }.cast::<libc::c_int>();
				for index in 0..count {
					let fd = unsafe { *data.add(index) };
					// SAFETY: the kernel put this descriptor in the process,
					// and nothing else in it owns the number.
					out.push(unsafe { OwnedDescriptor::from_raw_fd(fd) });
				}
			}
			header = unsafe { libc::CMSG_NXTHDR(msg, header) };
		}
		out
	}
}

struct Active {
	step: Step,
	/// The SQE points to this allocation until completion.
	_buffers: IoVecs,
	control: Option<Control>,
	fds: Vec<OwnedDescriptor>,
}

pub(crate) struct Uring {
	ring: IoUring,
	iopoll: bool,
	active: HashMap<u64, Active>,
	ready: VecDeque<(u64, Result<Response, Error>)>,
	/// Whether the kernel takes `IORING_ENTER_NO_IOWAIT` (Linux 6.15+). A kernel that does not still blocks in `io_uring_enter`, and that sleep counts as iowait.
	no_iowait: bool,
	/// Whether the futex wait is queued and has not completed.
	futex_armed: bool,
	sqpoll: bool,
	/// Hangup polls already reported, still armed until their last completion.
	retiring: HashSet<u64>,
}

impl Uring {
	pub(super) fn new(config: &Config) -> io::Result<Uring> {
		let mut builder = IoUring::builder();
		if config.iopoll {
			builder.setup_iopoll();
		}
		if config.sqpoll {
			builder.setup_sqpoll(0);
		}
		let ring = builder.build(config.entries.max(MIN_RING_ENTRIES))?;
		let iopoll = ring.params().is_setup_iopoll();
		let sqpoll = ring.params().is_setup_sqpoll();
		let no_iowait = ring.params().is_feature_no_iowait();
		Ok(Uring {
			ring,
			iopoll,
			active: HashMap::new(),
			ready: VecDeque::new(),
			no_iowait,
			futex_armed: false,
			sqpoll,
			retiring: HashSet::new(),
		})
	}

	/// Queues a wait on the park's state word. A wake that flips the word and
	/// calls `FUTEX_WAKE` completes it, so `enter` returns. The word is `PARKED`
	/// here: `Park::park` set it before polling.
	fn arm_futex(&mut self, park: &Park) -> io::Result<()> {
		if self.futex_armed {
			return Ok(());
		}
		let word = park.state().as_ptr().cast_const();
		let entry = opcode::FutexWait::new(
			word,
			u64::from(PARKED),
			u64::from(u32::MAX),
			FUTEX2_SIZE_U32,
		)
		.build()
		.user_data(FUTEX_TOKEN);
		push(&mut self.ring, entry)?;
		self.futex_armed = true;
		Ok(())
	}

	fn wait_flags(&self) -> EnterFlags {
		let mut flags = EnterFlags::GETEVENTS | EnterFlags::EXT_ARG;
		if self.no_iowait && !cfg!(feature = "iowait") {
			flags |= EnterFlags::NO_IOWAIT;
		}
		if self.sqpoll {
			flags |= EnterFlags::SQ_WAKEUP;
		}
		flags
	}

	/// The io-uring submit helpers cannot carry `NO_IOWAIT`, so call `io_uring_enter` directly.
	fn enter_wait(&mut self, timeout: Option<Duration>) -> io::Result<()> {
		let flags = self.wait_flags().bits();
		loop {
			// Dropping the submission-queue borrow publishes its tail.
			let queued = self.ring.submission().len() as u32;
			let result = match timeout {
				// SAFETY: `EXT_ARG` is set and the argument outlives the call.
				None => unsafe {
					let args = types::SubmitArgs::new();
					self.ring.submitter().enter(queued, 1, flags, Some(&args))
				},
				Some(timeout) => {
					let timespec = types::Timespec::from(timeout);
					let args = types::SubmitArgs::new().timespec(&timespec);
					// SAFETY: `EXT_ARG` is set, and `args` and the timespec it
					// points to outlive the call.
					unsafe { self.ring.submitter().enter(queued, 1, flags, Some(&args)) }
				}
			};
			match result {
				Ok(_) => return Ok(()),
				Err(error) if error.raw_os_error() == Some(Errno::TIME.raw_os_error()) => {
					return Ok(());
				}
				Err(error) if error.raw_os_error() == Some(Errno::INTR.raw_os_error()) => continue,
				Err(error) => return Err(error),
			}
		}
	}

	/// IOPOLL must actively poll the device; without a timeout, spin until completion.
	fn poll_devices(&mut self, timeout: Option<Duration>, park: &Park) -> io::Result<()> {
		let start = Instant::now();
		loop {
			self.drive()?;
			self.reap()?;
			if !self.ready.is_empty() {
				return Ok(());
			}
			// An IOPOLL ring has no doorbell, so a cross-thread wake is only in
			// the state word: see it within one spin instead of to the timeout.
			if park.woke() {
				return Ok(());
			}
			if self.active.is_empty() {
				return Ok(());
			}
			match timeout {
				Some(Duration::ZERO) => return Ok(()),
				Some(timeout) if start.elapsed() >= timeout => return Ok(()),
				_ => {}
			}
		}
	}

	fn drive(&mut self) -> io::Result<()> {
		self.ring.submitter().submit_and_wait(0)?;
		Ok(())
	}

	fn reap(&mut self) -> io::Result<()> {
		let mut settled = Vec::new();
		let mut rearm = Vec::new();
		{
			let ring = &mut self.ring;
			let active = &mut self.active;
			let futex_armed = &mut self.futex_armed;
			let ready = &mut self.ready;
			let retiring = &mut self.retiring;
			for cqe in ring.completion() {
				let id = cqe.user_data();
				let more = cqueue::more(cqe.flags());
				if retiring.contains(&id) {
					if !more {
						retiring.remove(&id);
					}
					continue;
				}
				// Only a hangup poll is multishot, and it reports nothing until
				// the mask names the hangup.
				if more {
					if hung(cqe.result()) && active.remove(&id).is_some() {
						retiring.insert(id);
						settled.push(id);
						ready.push_back((id, Ok(Response::Done)));
					}
					continue;
				}
				let entry = active.remove(&id);
				if let Some(Active {
					step: Step::Hangup { fd },
					..
				}) = &entry && cqe.result() >= 0
					&& !hung(cqe.result())
				{
					// The kernel ended the multishot without a hangup (a CQ it
					// could not post to): the wait is still owed.
					rearm.push((id, *fd));
					continue;
				}
				if id == CANCEL_TOKEN {
					continue;
				}
				if id == FUTEX_TOKEN {
					// Woken, refused because the word had moved on, or cancelled: each
					// one just ends the wait.
					*futex_armed = false;
					continue;
				}
				let moved = i64::from(cqe.result());
				if moved < 0 {
					let errno = -moved as i32;
					let error = if errno == Errno::CANCELED.raw_os_error() {
						Error::cancelled()
					} else {
						Error::from(io::Error::from_raw_os_error(errno))
					};
					ready.push_back((id, Err(error)));
					continue;
				}
				let moved = moved as usize;
				let Some(mut active) = entry else {
					ready.push_back((id, Ok(Response::Done)));
					continue;
				};
				if active.step.kind() == Kind::CountWithFds {
					if let Some(mut control) = active.control.take() {
						active.fds.append(&mut control.take_fds());
					}
				}
				let response = match active.step.kind() {
					Kind::Count => Response::Count(moved),
					Kind::CountWithFds => Response::CountWithFds(moved, active.fds),
					Kind::Accepted => {
						let handle = RawHandle::from_raw(moved);
						// SAFETY: the kernel created this descriptor for the
						// accept, and the response is its only owner.
						Response::Accepted(unsafe { from_raw_socket(handle) })
					}
					Kind::Done => Response::Done,
				};
				ready.push_back((id, Ok(response)));
			}
		}
		for id in settled {
			let entry = opcode::AsyncCancel::new(id).build().user_data(CANCEL_TOKEN);
			if let Err(error) = push(&mut self.ring, entry) {
				log::warn!(id, error:%; "a reported hangup poll stays armed");
			}
		}
		for (id, fd) in rearm {
			if let Err(error) = self.submit(id, Step::Hangup { fd }) {
				self.ready.push_back((id, Err(Error::from(error))));
			}
		}
		Ok(())
	}

	fn prepare(
		&mut self,
		step: &mut Step,
		control: &mut Option<Control>,
		id: u64,
	) -> io::Result<(squeue::Entry, IoVecs)> {
		let (entry, buffers) = match step {
			Step::Read { fd, fdoff, buffers } => {
				let buffers = std::mem::take(buffers);
				let entry = opcode::Readv::new(
					types::Fd(fd.as_raw_fd()),
					buffers.as_ptr().cast(),
					count(&buffers)?,
				)
				.offset(*fdoff)
				.build()
				.user_data(id);
				(entry, buffers)
			}
			Step::Write { fd, fdoff, buffers } => {
				let buffers = std::mem::take(buffers);
				let entry = opcode::Writev::new(
					types::Fd(fd.as_raw_fd()),
					buffers.as_ptr().cast(),
					count(&buffers)?,
				)
				.offset(*fdoff)
				.build()
				.user_data(id);
				(entry, buffers)
			}
			Step::Fsync { fd } => {
				// An IOPOLL ring only takes operations the kernel can poll
				// for; a fsync has no overlapped form, and io_uring answers
				// EINVAL for it.
				if self.iopoll {
					return Err(io::Error::new(
						io::ErrorKind::Unsupported,
						"fsync cannot be submitted on an IOPOLL ring; give it a ring of its own",
					));
				}
				(
					opcode::Fsync::new(types::Fd(fd.as_raw_fd()))
						.build()
						.user_data(id),
					IoVecs::new(),
				)
			}
			Step::Flush { fd } => {
				// Like a fsync, a close has no form an IOPOLL ring can poll
				// for.
				if self.iopoll {
					return Err(io::Error::new(
						io::ErrorKind::Unsupported,
						"flush cannot be submitted on an IOPOLL ring; give it a ring of its own",
					));
				}
				(
					opcode::Close::new(types::Fd(fd.as_raw_fd()))
						.build()
						.user_data(id),
					IoVecs::new(),
				)
			}
			Step::Fallocate {
				fd,
				offset,
				len,
				mode,
			} => {
				if self.iopoll {
					return Err(io::Error::new(
						io::ErrorKind::Unsupported,
						"fallocate cannot be submitted on an IOPOLL ring; give it a ring of its own",
					));
				}
				(
					opcode::Fallocate::new(types::Fd(fd.as_raw_fd()), *len)
						.offset(*offset)
						.mode(*mode)
						.build()
						.user_data(id),
					IoVecs::new(),
				)
			}
			Step::Accept { fd } => {
				// An accept cannot be polled for, and only a ring that can be
				// polled for has anything to wait on.
				if self.iopoll {
					return Err(io::Error::new(
						io::ErrorKind::Unsupported,
						"an accept cannot complete on an IOPOLL ring; give it a ring of its own",
					));
				}
				let flags = (rustix::net::SocketFlags::CLOEXEC | rustix::net::SocketFlags::NONBLOCK)
					.bits() as i32;
				let entry = opcode::Accept::new(
					types::Fd(fd.as_raw_fd()),
					std::ptr::null_mut(),
					std::ptr::null_mut(),
				)
				.flags(flags)
				.build()
				.user_data(id);
				(entry, IoVecs::new())
			}
			Step::Recv { fd, buffers } => {
				let buffers = std::mem::take(buffers);
				let [buf] = buffers.as_slice() else {
					return Err(io::Error::new(
						io::ErrorKind::Unsupported,
						"a socket receive needs exactly one buffer",
					));
				};
				let len = u32::try_from(buf.len()).map_err(|_| {
					io::Error::new(io::ErrorKind::InvalidInput, "buffer longer than 4 GiB")
				})?;
				let entry =
					opcode::Recv::new(types::Fd(fd.as_raw_fd()), buf.as_ptr() as *mut u8, len)
						.build()
						.user_data(id);
				(entry, buffers)
			}
			Step::RecvWithFds { fd, buffers } => {
				let buffers = std::mem::take(buffers);
				let [buf] = buffers.as_slice() else {
					return Err(io::Error::new(
						io::ErrorKind::Unsupported,
						"a socket receive needs exactly one buffer",
					));
				};
				let mut area = Control::receive(buf.as_ptr() as *mut u8, buf.len());
				let message = area.message();
				*control = Some(area);
				let entry = opcode::RecvMsg::new(types::Fd(fd.as_raw_fd()), message)
					.flags(libc::MSG_CMSG_CLOEXEC as u32)
					.build()
					.user_data(id);
				(entry, buffers)
			}
			Step::Send { fd, buffers, fds } => {
				let buffers = std::mem::take(buffers);
				let [buf] = buffers.as_slice() else {
					return Err(io::Error::new(
						io::ErrorKind::Unsupported,
						"a socket send needs exactly one buffer",
					));
				};
				if fds.is_empty() {
					let len = u32::try_from(buf.len()).map_err(|_| {
						io::Error::new(io::ErrorKind::InvalidInput, "buffer longer than 4 GiB")
					})?;
					let entry = opcode::Send::new(types::Fd(fd.as_raw_fd()), buf.as_ptr(), len)
						.build()
						.user_data(id);
					(entry, buffers)
				} else {
					let mut area = Control::send(buf.as_ptr(), buf.len(), std::mem::take(fds));
					let message = area.message();
					*control = Some(area);
					let entry = opcode::SendMsg::new(types::Fd(fd.as_raw_fd()), message)
						.build()
						.user_data(id);
					(entry, buffers)
				}
			}
			Step::Readable { fd } => {
				let entry = opcode::PollAdd::new(types::Fd(fd.as_raw_fd()), readable_mask());
				(entry.build().user_data(id), IoVecs::new())
			}
			Step::Writable { fd } => {
				let entry = opcode::PollAdd::new(types::Fd(fd.as_raw_fd()), writable_mask());
				(entry.build().user_data(id), IoVecs::new())
			}
			Step::Hangup { fd } => {
				// The kernel ORs IO_POLL_UNMASK (EPOLLERR | EPOLLHUP | EPOLLNVAL |
				// EPOLLRDHUP, io_uring/poll.c) into every poll's events, so even an
				// empty mask fires on a half-close. Multishot keeps the poll armed
				// past those; `reap` waits for a mask with HUP or ERR.
				let entry = opcode::PollAdd::new(types::Fd(fd.as_raw_fd()), 0).multi(true);
				(entry.build().user_data(id), IoVecs::new())
			}
		};
		Ok((entry, buffers))
	}

	fn submit(&mut self, id: u64, step: Step) -> io::Result<()> {
		let mut step = step;
		let mut control = None;
		let (entry, buffers) = self.prepare(&mut step, &mut control, id)?;
		self.active.insert(
			id,
			Active {
				step,
				_buffers: buffers,
				control,
				fds: Vec::new(),
			},
		);
		// SQPOLL consumes submissions itself but needs a syscall to wake its sleeping thread.
		let result = push(&mut self.ring, entry).and_then(|()| {
			if self.sqpoll {
				self.ring.submit()?;
			}
			Ok(())
		});
		if let Err(error) = result {
			self.active.remove(&id);
			return Err(error);
		}
		Ok(())
	}
}

impl Drop for Uring {
	fn drop(&mut self) {
		// A queued futex wait points at the park's state word, which the reactor
		// frees with its last `Arc`. Cancel it and reap its completion first, so
		// the ring lets go before the word can go.
		if !self.futex_armed {
			return;
		}
		let entry = opcode::AsyncCancel::new(FUTEX_TOKEN)
			.build()
			.user_data(CANCEL_TOKEN);
		if push(&mut self.ring, entry).is_err() {
			return;
		}
		while self.futex_armed {
			if self.ring.submit_and_wait(1).is_err() || self.reap().is_err() {
				return;
			}
		}
	}
}

impl Backend for Uring {
	fn read(&mut self, id: u64, fd: RawHandle, fdoff: u64, buffers: IoVecs) -> io::Result<()> {
		self.submit(id, Step::Read { fd, fdoff, buffers })
	}

	fn write(&mut self, id: u64, fd: RawHandle, fdoff: u64, buffers: IoVecs) -> io::Result<()> {
		self.submit(id, Step::Write { fd, fdoff, buffers })
	}

	fn fsync(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		self.submit(id, Step::Fsync { fd })
	}

	fn flush(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		let dup = unsafe { libc::dup(fd.as_raw_fd()) };
		if dup < 0 {
			return Err(io::Error::last_os_error());
		}
		let dup = RawHandle::from_raw(dup as usize);
		let result = self.submit(id, Step::Flush { fd: dup });
		if result.is_err() {
			// Submission failed: close the duplicate. Do not retry EINTR; Linux already closed it and may reuse the number.
			unsafe { libc::close(dup.as_raw_fd()) };
		}
		result
	}

	fn allocate(&mut self, id: u64, fd: RawHandle, offset: u64, len: u64) -> io::Result<()> {
		self.submit(
			id,
			Step::Fallocate {
				fd,
				offset,
				len,
				mode: 0,
			},
		)
	}

	fn punch(&mut self, id: u64, fd: RawHandle, offset: u64, len: u64) -> io::Result<()> {
		self.submit(
			id,
			Step::Fallocate {
				fd,
				offset,
				len,
				mode: libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
			},
		)
	}

	fn recv(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()> {
		self.submit(id, Step::Recv { fd, buffers })
	}

	fn recv_with_fds(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()> {
		self.submit(id, Step::RecvWithFds { fd, buffers })
	}

	fn send(&mut self, id: u64, fd: RawHandle, buffers: IoVecs) -> io::Result<()> {
		self.submit(
			id,
			Step::Send {
				fd,
				buffers,
				fds: Vec::new(),
			},
		)
	}

	fn send_with_fds(
		&mut self,
		id: u64,
		fd: RawHandle,
		buffers: IoVecs,
		fds: Vec<OwnedDescriptor>,
	) -> io::Result<()> {
		self.submit(id, Step::Send { fd, buffers, fds })
	}

	fn accept(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		self.submit(id, Step::Accept { fd })
	}

	fn readable(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		self.submit(id, Step::Readable { fd })
	}

	fn writable(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		self.submit(id, Step::Writable { fd })
	}

	fn hangup(&mut self, id: u64, fd: RawHandle) -> io::Result<()> {
		self.submit(id, Step::Hangup { fd })
	}

	fn cancel(&mut self, id: u64) {
		let entry = opcode::AsyncCancel::new(id).build().user_data(CANCEL_TOKEN);
		let _ = push(&mut self.ring, entry).and_then(|()| self.ring.submit());
	}

	fn poll(&mut self, timeout: Option<Duration>, park: &Park) -> io::Result<u64> {
		if let Some((id, _)) = self.ready.front() {
			return Ok(*id);
		}
		if self.iopoll {
			self.poll_devices(timeout, park)?;
		} else if timeout == Some(Duration::ZERO) {
			self.ring.submit()?;
			self.reap()?;
		} else {
			self.arm_futex(park)?;
			self.enter_wait(timeout)?;
			self.reap()?;
		}
		match self.ready.front() {
			Some((id, _)) => Ok(*id),
			None => Err(io::Error::from(io::ErrorKind::WouldBlock)),
		}
	}

	fn take(&mut self, id: u64) -> Result<Response, Error> {
		match self.ready.pop_front() {
			Some((found, response)) if found == id => response,
			Some((found, response)) => {
				self.ready.push_front((found, response));
				Err(Error::from(io::Error::other(format!(
					"take({id}) found a completion for {found}"
				))))
			}
			None => Err(Error::from(io::Error::new(
				io::ErrorKind::NotFound,
				"no completion to take",
			))),
		}
	}

	fn doorbell(&self) -> Arc<dyn Doorbell> {
		if self.iopoll {
			Arc::new(Spin)
		} else {
			Arc::new(FutexWake)
		}
	}
}

/// Whether a poll's result mask names the peer gone or the socket failed.
fn hung(result: i32) -> bool {
	let gone = rustix::event::PollFlags::HUP | rustix::event::PollFlags::ERR;
	result >= 0 && (result as u32) & (gone.bits() as u32) != 0
}

/// Return a full-ring error to the caller rather than spinning for space.
fn push(ring: &mut IoUring, entry: squeue::Entry) -> io::Result<()> {
	if try_push(ring, &entry) {
		return Ok(());
	}
	ring.submit()?;
	if try_push(ring, &entry) {
		return Ok(());
	}
	Err(io::Error::new(
		io::ErrorKind::WouldBlock,
		"the submission queue is full",
	))
}

fn try_push(ring: &mut IoUring, entry: &squeue::Entry) -> bool {
	let mut submission = ring.submission();
	unsafe { submission.push(entry) }.is_ok()
}

fn count(bufs: &[IoVec]) -> io::Result<u32> {
	u32::try_from(bufs.len())
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many buffers"))
}

#[cfg(test)]
mod tests {
	use super::*;

	/// `NO_IOWAIT` rides every blocking wait on a kernel that reports the
	/// feature, unless the `iowait` feature withholds it.
	#[test]
	fn the_no_iowait_flag_follows_the_kernel_and_the_feature() -> io::Result<()> {
		let mut uring = Uring::new(&Config::default())?;
		uring.no_iowait = false;
		assert!(!uring.wait_flags().contains(EnterFlags::NO_IOWAIT));
		uring.no_iowait = true;
		assert_eq!(
			uring.wait_flags().contains(EnterFlags::NO_IOWAIT),
			!cfg!(feature = "iowait"),
		);
		Ok(())
	}

	/// A kernel without `NO_IOWAIT` takes the same `io_uring_enter` wait.
	#[test]
	fn a_blocking_wait_works_without_the_feature() -> io::Result<()> {
		let mut uring = Uring::new(&Config::default())?;
		let park = Arc::new(Park::new(uring.doorbell()));
		uring.no_iowait = false;
		let start = Instant::now();
		let mut waited = None;
		park.park(|| {
			waited = Some(uring.poll(Some(Duration::from_millis(5)), &park));
			Ok(())
		})?;
		let waited = waited.expect("the poll ran");
		assert!(
			matches!(waited, Err(ref error) if error.kind() == io::ErrorKind::WouldBlock),
			"an empty wait reports nothing completed: {waited:?}"
		);
		assert!(
			start.elapsed() >= Duration::from_millis(5),
			"the wait did not hold for its deadline"
		);
		Ok(())
	}
}
