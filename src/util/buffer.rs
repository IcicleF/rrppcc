use std::ptr::NonNull;
use std::rc::Rc;

use crate::transport::{LKey, RKey};
use crate::util::buddy::*;

pub(crate) struct Buffer {
    /// Start address of the buffer.
    buf: NonNull<u8>,

    /// Length of the buffer.
    len: usize,

    /// Local memory handle.
    lkey: LKey,

    /// Remote memory handle.
    rkey: RKey,

    /// Pointer to the buddy allocator.
    owner: Option<Rc<BuddyAllocator>>,
}

impl Buffer {
    /// A real buffer that will be deallocated when dropped.
    ///
    /// This constructor does not set `owner`. Use `set_owner` to set it.
    #[inline]
    pub fn real(buf: NonNull<u8>, len: usize, lkey: LKey, rkey: RKey) -> Self {
        Self {
            buf,
            len,
            lkey,
            rkey,
            owner: None,
        }
    }

    /// A fake buffer that only serves to record `(LKey, RKey)`, and does nothing when dropped.
    #[inline]
    pub fn fake(lkey: LKey, rkey: RKey) -> Self {
        Self {
            buf: NonNull::dangling(),
            len: 0,
            lkey,
            rkey,
            owner: None,
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

    /// Get the local key of the buffer.
    #[inline(always)]
    pub fn lkey(&self) -> LKey {
        self.lkey
    }

    /// Get the remote key of the buffer.
    #[inline(always)]
    pub fn rkey(&self) -> RKey {
        self.rkey
    }

    /// Set the owner of the buffer.
    #[inline(always)]
    pub fn set_owner(&mut self, owner: &Rc<BuddyAllocator>) {
        self.owner = Some(owner.clone());
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(ref owner) = self.owner {
            owner.free(self);
        }
    }
}
