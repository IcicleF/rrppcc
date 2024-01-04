use std::ptr::{self, NonNull};

use crate::transport::LKey;
use crate::util::buddy::*;

pub(crate) struct Buffer {
    /// Start address of the buffer.
    buf: NonNull<u8>,

    /// Length of the buffer.
    len: usize,

    /// Memory handle.
    lkey: LKey,

    /// Pointer to the buddy allocator.
    owner: *mut BuddyAllocator,
}

impl Buffer {
    /// A real buffer that will be deallocated when dropped.
    #[inline]
    pub fn real(owner: *mut BuddyAllocator, buf: NonNull<u8>, len: usize, lkey: LKey) -> Self {
        Self {
            buf,
            len,
            lkey,
            owner,
        }
    }

    /// A fake buffer that only serves to record a LKey, and does nothing when dropped.
    #[inline]
    pub fn fake(lkey: LKey) -> Self {
        Self {
            buf: NonNull::dangling(),
            len: 0,
            lkey,
            owner: ptr::null_mut(),
        }
    }

    /// Get the start address of the buffer.
    #[inline(always)]
    pub fn as_ptr(&self) -> *mut u8 {
        self.buf.as_ptr()
    }

    /// Get the length of the buffer.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the memory handle of the buffer.
    #[inline(always)]
    pub fn lkey(&self) -> LKey {
        self.lkey
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(owner) = NonNull::new(self.owner) {
            // Return the buffer to the allocator.
            // SAFETY: if the owner is not null, it must point to a valid BuddyAllocator.
            unsafe { BuddyAllocator::free_by_ptr(owner, self) };
        }
    }
}
