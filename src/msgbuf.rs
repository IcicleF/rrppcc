use std::ptr::NonNull;
use std::{mem, ptr, slice};

use crate::pkthdr::*;
use crate::transport::{ControlMsg, LKey, RKey, UdTransport};
use crate::util::{buddy::BuddyAllocator, buffer::*, likely::*};

/// Message buffer that can contain application data for
/// requests or accommodate responses.
pub struct MsgBuf {
    /// Pointer to the first *application data* byte.
    data: NonNull<u8>,

    /// Max data bytes in the MsgBuf.
    /// This number must be a multiple of 8.
    max_len: usize,

    /// Valid data bytes in the MsgBuf.
    len: usize,

    /// Backing buffer.
    buffer: Buffer,

    /// Padding to 64 bytes.
    _padding: [u8; 8],
}

unsafe impl Send for MsgBuf {}
unsafe impl Sync for MsgBuf {}

impl MsgBuf {
    /// Maximum application data bytes in a single `MsgBuf`.
    pub const MAX_DATA_LEN: usize =
        BuddyAllocator::MAX_ALLOC_SIZE - mem::size_of::<PacketHeader>() - 64;

    /// Return the needed buffer size for the given data length.
    #[inline(always)]
    pub(crate) const fn buf_len(data_len: usize) -> usize {
        // Roundup data length to multiplicity of 8.
        let max_len = data_len.next_multiple_of(8);

        // For short messages, header + data buffer.
        // For long messages: header + data buffer + control message.
        let metadata_len = mem::size_of::<PacketHeader>()
            + if likely(max_len <= UdTransport::max_data_in_pkt()) {
                0
            } else {
                mem::size_of::<ControlMsg>()
            };
        max_len + metadata_len
    }

    /// Create a new `MsgBuf` on owned buffer.
    ///
    /// This static method should be used to create a `MsgBuf` that is mutable
    /// by the user. If the `MsgBuf` will never be mutated (e.g., a large request
    /// for the request handler), use [`owned_immutable()`] instead.
    #[inline]
    pub(crate) fn owned(buf: Buffer, data_len: usize) -> Self {
        // Roundup data length to multiplicity of 8.
        assert!(data_len < Self::MAX_DATA_LEN);
        let max_len = data_len.next_multiple_of(8);

        // Ensure that the buffer has enough space.
        let buf_avail_len = if unlikely(buf.len() % 8 != 0) {
            buf.len() - (buf.len() % 8)
        } else {
            buf.len()
        };
        assert!(
            Self::buf_len(data_len) <= buf_avail_len,
            "buffer too small: {} (avail {}) < {}",
            buf.len(),
            buf_avail_len,
            Self::buf_len(data_len)
        );

        let rkey = buf.rkey();

        // The return value.
        let ret = Self {
            // SAFETY: guaranteed not null.
            data: unsafe { NonNull::new_unchecked(buf.as_ptr()) },
            max_len,
            len: data_len,
            buffer: buf,
            _padding: [0; 8],
        };

        // If short message, return it directly ...
        if likely(max_len <= UdTransport::max_data_in_pkt()) {
            return ret;
        }

        // ... otherwise, we need to fill in the control message.
        let ctrl = ret.ctrl_msg();

        // SAFETY: `MsgBuf::ctrl_msg()` ensures buffer validity.
        unsafe {
            ptr::write(
                ctrl,
                ControlMsg {
                    addr: ret.as_ptr() as _,
                    rkey,
                },
            )
        };
        ret
    }

    /// Create a new `MsgBuf` on owned buffer, but guarantee that the buffer
    /// will never be mutated. Therefore, we do not need to store the header,
    /// nor the control message. Only application data, no extra metadata.
    #[inline]
    pub(crate) fn owned_immutable(buf: Buffer, data_len: usize) -> Self {
        // Roundup data length to multiplicity of 8.
        let data_len = data_len.next_multiple_of(8);
        assert!(data_len < Self::MAX_DATA_LEN);

        Self {
            // SAFETY: guaranteed not null.
            data: unsafe { NonNull::new_unchecked(buf.as_ptr()) },
            max_len: data_len,
            len: data_len,
            buffer: buf,
            _padding: [0; 8],
        }
    }

    /// Create a new `MsgBuf` on not-owned buffer.
    /// It must not be remote-accessible.
    ///
    /// # Safety
    ///
    /// The header must point to a valid `PacketHeader` right before actual payload data.
    /// Note that the payload can be either application or control data.
    #[inline]
    pub(crate) unsafe fn borrowed(hdr: NonNull<PacketHeader>, data_len: usize, lkey: LKey) -> Self {
        Self {
            data: NonNull::new_unchecked(hdr.as_ptr().add(1) as *mut u8),
            max_len: data_len,
            len: data_len,
            buffer: Buffer::fake(lkey, 0),
            _padding: [0; 8],
        }
    }

    /// Create a placeholder `MsgBuf`.
    /// This should only be useful when initializing `SSlot`s.
    #[inline]
    pub(crate) fn dummy() -> Self {
        Self {
            data: NonNull::dangling(),
            max_len: 0,
            len: 0,
            buffer: Buffer::fake(0, 0),
            _padding: [0; 8],
        }
    }

    /// Clone a `MsgBuf` as borrowed.
    /// The resulting `MsgBuf` will not do anything when dropped.
    #[inline]
    pub(crate) fn clone_borrowed(&self) -> Self {
        Self {
            data: self.data,
            max_len: self.max_len,
            len: self.len,
            buffer: Buffer::fake(self.lkey(), self.rkey()),
            _padding: [0; 8],
        }
    }

    /// Get a pointer to a packet header.
    #[inline]
    pub(crate) fn pkt_hdr(&self) -> *mut PacketHeader {
        // SAFETY: the entire MsgBuf must be within the same allocated buffer.
        let hdr = unsafe { self.data.as_ptr().sub(mem::size_of::<PacketHeader>()) };
        debug_assert!(!hdr.is_null());
        debug_assert!(
            (hdr as usize) % mem::align_of::<PacketHeader>() == 0,
            "misaligned header"
        );
        hdr as _
    }

    /// Get a pointer to the control message.
    ///
    /// # Panics
    ///
    /// Panic (in debug mode) if the message buffer is not large enough to hold the control message.
    #[inline]
    pub(crate) fn ctrl_msg(&self) -> *mut ControlMsg {
        debug_assert!(
            self.max_len > UdTransport::max_data_in_pkt(),
            "MsgBuf size {} too small to hold control message",
            self.max_len
        );

        // SAFETY: the entire MsgBuf must be within the same allocated buffer.
        let ctrl = unsafe { self.data.as_ptr().add(self.max_len) };
        debug_assert!(!ctrl.is_null());
        debug_assert!(
            (ctrl as usize) % mem::align_of::<ControlMsg>() == 0,
            "misaligned control message"
        );
        ctrl as _
    }

    /// Get the local key of the backing buffer.
    #[inline(always)]
    pub(crate) fn lkey(&self) -> LKey {
        self.buffer.lkey()
    }

    /// Get the remote key of the backing buffer.
    #[inline(always)]
    pub(crate) fn rkey(&self) -> RKey {
        self.buffer.rkey()
    }

    /// Return `true` if the packet header and the application data can fit in one packet.
    #[inline(always)]
    pub(crate) fn is_small(&self) -> bool {
        self.len <= UdTransport::max_data_in_pkt()
    }
}

impl MsgBuf {
    /// Return a pointer to the first *application data* byte.
    #[inline(always)]
    pub fn as_ptr(&self) -> *mut u8 {
        self.data.as_ptr()
    }

    /// Return the length of application data.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return the capacity of application data.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.max_len
    }

    /// Set the application data length message buffer.
    ///
    /// # Panics
    ///
    /// Panic if `len` is larger than the capacity.
    #[inline(always)]
    pub fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.max_len,
            "len {} > MsgBuf capacity {}",
            len,
            self.max_len
        );
        self.len = len;
    }

    /// View the application data as a `[u8]` slice.
    ///
    /// # Safety
    ///
    /// This method has the same safety requirements as [`std::slice::from_raw_parts()`].
    #[inline(always)]
    pub unsafe fn as_slice(&self) -> &[u8] {
        slice::from_raw_parts(self.data.as_ptr(), self.len)
    }

    /// View the application data as a mutable `[u8]` slice.
    ///
    /// # Safety
    ///
    /// This method has the same safety requirements as [`std::slice::from_raw_parts_mut()`].
    #[inline(always)]
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        slice::from_raw_parts_mut(self.data.as_ptr(), self.len)
    }
}
