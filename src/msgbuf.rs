use std::ptr::NonNull;
use std::{mem, slice};

use crate::pkthdr::*;
use crate::transport::LKey;
use crate::util::{buddy::BuddyAllocator, buffer::*};

pub struct MsgBuf {
    /// Pointer to the first *application data* byte.
    data: NonNull<u8>,

    /// Max data bytes in the MsgBuf.
    max_len: usize,

    /// Valid data bytes in the MsgBuf.
    len: usize,

    /// Backing buffer.
    buffer: Buffer,
}

unsafe impl Send for MsgBuf {}
unsafe impl Sync for MsgBuf {}

impl MsgBuf {
    /// Maximum application data bytes in a single `MsgBuf`.
    pub const MAX_DATA_LEN: usize = BuddyAllocator::MAX_ALLOC_SIZE - mem::size_of::<PacketHeader>();

    /// Create a new MsgBuf on owned buffer.
    #[inline]
    pub(crate) fn owned(buf: Buffer, data_len: usize) -> Self {
        assert!(data_len < Self::MAX_DATA_LEN);

        let overall_len = data_len + mem::size_of::<PacketHeader>();
        assert!(
            overall_len <= buf.len(),
            "buffer too small: {} < {}",
            buf.len(),
            overall_len
        );

        Self {
            // SAFETY: guaranteed not null.
            data: unsafe { NonNull::new_unchecked(buf.as_ptr()) },
            max_len: buf.len() - mem::size_of::<PacketHeader>(),
            len: data_len,
            buffer: buf,
        }
    }

    /// Create a new MsgBuf on not-owned buffer.
    ///
    /// # Safety
    ///
    /// The header must point to a valid `PacketHeader` right before application data.
    #[inline]
    pub(crate) unsafe fn borrowed(hdr: NonNull<PacketHeader>, data_len: usize, lkey: LKey) -> Self {
        Self {
            data: NonNull::new_unchecked(hdr.as_ptr().add(1) as *mut u8),
            max_len: data_len,
            len: data_len,
            buffer: Buffer::fake(lkey),
        }
    }

    /// Clone a borrowed `MsgBuf`.
    ///
    /// # Panics
    ///
    /// Panic (in debug mode) if the `MsgBuf` is not borrowed.
    #[inline]
    pub(crate) fn clone_borrowed(&self) -> Self {
        debug_assert!(self.buffer.is_fake(), "attempting to clone a owned MsgBuf");
        Self {
            data: self.data,
            max_len: self.max_len,
            len: self.len,
            buffer: Buffer::fake(self.lkey()),
        }
    }

    /// Get a pointer to a packet header.
    #[inline]
    pub(crate) fn pkt_hdr(&self) -> *mut PacketHeader {
        // SAFETY: header & application data must be within the same allocated buffer.
        let hdr = unsafe { self.data.as_ptr().sub(mem::size_of::<PacketHeader>()) };
        debug_assert!(!hdr.is_null());
        debug_assert!(
            (hdr as usize) % mem::align_of::<PacketHeader>() == 0,
            "misaligned header"
        );
        hdr as _
    }

    /// Get the memory handle of the packet buffer.
    #[inline(always)]
    pub(crate) fn lkey(&self) -> LKey {
        self.buffer.lkey()
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
    pub unsafe fn as_mut_slice(&self) -> &mut [u8] {
        slice::from_raw_parts_mut(self.data.as_ptr(), self.len)
    }
}
