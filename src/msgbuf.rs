use std::ptr::NonNull;
use std::{cmp, mem, slice};

use crate::pkthdr::*;
use crate::transport::{LocalKey, UnreliableTransport};
use crate::util::{buffer::*, likely::*, math::*};

pub struct MsgBuf {
    /// Pointer to the first *application data* byte.
    data: NonNull<u8>,

    /// Max data bytes in the MsgBuf.
    max_len: usize,

    /// Valid data bytes in the MsgBuf.
    len: usize,

    /// Max number of packets in the MsgBuf.
    max_pkts: usize,

    /// Backing buffer.
    buffer: Buffer,
}

unsafe impl Send for MsgBuf {}
unsafe impl Sync for MsgBuf {}

/// Protected methods.
impl MsgBuf {
    /// Create a new MsgBuf on owned buffer.
    pub(crate) fn owned<Tp: UnreliableTransport>(buf: Buffer, data_len: usize) -> Self {
        let max_pkts = if data_len == 0 {
            1
        } else {
            (data_len - 1) / Tp::max_data_per_pkt() + 1
        };

        let overall_len = roundup(data_len, 8) + max_pkts * mem::size_of::<PacketHeader>();
        assert!(
            overall_len <= buf.len(),
            "buffer too small: {} < {}",
            buf.len(),
            overall_len
        );

        Self {
            // SAFETY: guaranteed not null.
            data: unsafe { NonNull::new_unchecked(buf.as_ptr()) },
            max_pkts,
            max_len: data_len,
            len: data_len,
            buffer: buf,
        }
    }

    /// Create a new MsgBuf on not-owned buffer.
    ///
    /// # Safety
    ///
    /// The header must point to a valid `PacketHeader` right before application data.
    pub(crate) unsafe fn borrowed(hdr: NonNull<u8>, len: usize, lkey: LocalKey) -> Self {
        Self {
            data: NonNull::new_unchecked(hdr.as_ptr().add(mem::size_of::<PacketHeader>())),
            max_pkts: 1,
            max_len: len,
            len,
            buffer: Buffer::fake(lkey),
        }
    }

    /// Get a pointer to a packet header.
    #[inline]
    pub(crate) fn pkt_hdr(&self, pkt_idx: usize) -> *mut PacketHeader {
        debug_assert!(
            pkt_idx < self.max_pkts,
            "invalid packet index: max {}, got {}",
            self.max_pkts - 1,
            pkt_idx
        );

        // SAFETY: header & application data must be within the same allocated buffer.
        let hdr = unsafe {
            if likely(pkt_idx == 0) {
                self.data.as_ptr().sub(mem::size_of::<PacketHeader>())
            } else {
                self.data
                    .as_ptr()
                    .add(roundup(self.max_len, 8) + (pkt_idx - 1) * mem::size_of::<PacketHeader>())
            }
        };
        debug_assert!(
            (hdr as usize) % mem::align_of::<PacketHeader>() == 0,
            "misaligned header"
        );
        hdr as _
    }

    /// Get the size of a packet.
    #[inline]
    pub(crate) fn pkt_size(&self, pkt_idx: usize, max_data_per_pkt: usize) -> usize {
        debug_assert!(
            pkt_idx < self.max_pkts,
            "invalid packet index: max {}, got {}",
            self.max_pkts - 1,
            pkt_idx
        );

        let offset = pkt_idx * max_data_per_pkt;
        mem::size_of::<PacketHeader>() + cmp::min(max_data_per_pkt, self.len - offset)
    }

    /// Get the memory handle of the packet buffer.
    #[inline(always)]
    pub(crate) fn lkey(&self) -> LocalKey {
        self.buffer.lkey()
    }
}

/// Public methods.
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
