use crate::transport::LocalKey;
use std::ptr::NonNull;

pub(crate) struct Buffer {
    /// Start address of the buffer.
    buf: NonNull<u8>,

    /// Length of the buffer.
    len: usize,

    /// Memory handle.
    lkey: LocalKey,
}

impl Buffer {
    /// A real buffer.
    pub fn real(buf: NonNull<u8>, len: usize, lkey: LocalKey) -> Self {
        Self { buf, len, lkey }
    }

    /// A fake buffer.
    pub fn fake(lkey: LocalKey) -> Self {
        Self {
            buf: NonNull::dangling(),
            len: 0,
            lkey,
        }
    }

    /// Get the start address of the buffer.
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.buf.as_ptr()
    }

    /// Get the length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the memory handle of the buffer.
    #[inline]
    pub fn lkey(&self) -> LocalKey {
        self.lkey
    }

    /// Split the buffer into two buffers.
    #[inline]
    pub fn split(self) -> (Buffer, Buffer) {
        let len = self.len / 2;
        let buf1 = Self::real(self.buf, len, self.lkey);
        let buf2 = Self::real(
            // SAFETY: guaranteed not null, within the same allocated memory buffer.
            unsafe { NonNull::new_unchecked(self.buf.as_ptr().add(len)) },
            len,
            self.lkey,
        );
        (buf1, buf2)
    }
}
