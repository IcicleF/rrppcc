use crate::transport::UnreliableTransport;
use crate::util::{buffer::*, huge_alloc::*};
use std::ptr::NonNull;

pub(crate) struct BuddyAllocator {
    /// Buddy system.
    buddy: [Vec<Buffer>; Self::NUM_CLASSES],

    /// Allocated memory registry.
    #[allow(dead_code)]
    mem_registry: Vec<HugeAlloc>,

    /// Next allocation size.
    next_alloc: usize,
}

impl BuddyAllocator {
    /// Buffer exhausted, allocate new memory.
    fn reserve_memory<Tp: UnreliableTransport>(&mut self, tp: &mut Tp) {
        let len = self.next_alloc;
        self.next_alloc *= 2;
        debug_assert!(len % Self::MAX_ALLOC_SIZE == 0);

        let mem = alloc_raw(len);
        let lkey = unsafe { tp.reg_mem(mem.ptr, len) };

        for i in 0..(len / Self::MAX_ALLOC_SIZE) {
            let buf = Buffer::real(
                // SAFETY: guaranteed not null, within the same allocated memory buffer.
                unsafe { NonNull::new_unchecked(mem.ptr.add(i * Self::MAX_ALLOC_SIZE)) },
                Self::MAX_ALLOC_SIZE,
                lkey,
            );
            self.buddy[Self::NUM_CLASSES - 1].push(buf);
        }
        self.mem_registry.push(mem);
    }

    /// Get the class of a given size.
    #[inline]
    const fn class(len: usize) -> usize {
        let len = len.next_power_of_two();
        if len < Self::MIN_ALLOC_SIZE {
            0
        } else {
            (len / Self::MIN_ALLOC_SIZE).trailing_zeros() as usize
        }
    }

    /// Split a buffer of the given class into two buffers of the next lower class.
    #[inline]
    fn split(&mut self, class: usize) {
        debug_assert!(class >= 1 && class < Self::NUM_CLASSES);
        debug_assert!(!self.buddy[class].is_empty());

        let buf = self.buddy[class].pop().unwrap();
        let (buf1, buf2) = buf.split();
        self.buddy[class - 1].push(buf1);
        self.buddy[class - 1].push(buf2);
    }
}

impl BuddyAllocator {
    const MIN_ALLOC_SIZE: usize = 1 << 6;
    const MAX_ALLOC_SIZE: usize = 1 << 24;
    const NUM_CLASSES: usize =
        (Self::MAX_ALLOC_SIZE / Self::MIN_ALLOC_SIZE).trailing_zeros() as usize + 1;

    /// Create a new buddy allocator with no pre-allocation.
    pub fn new() -> Self {
        Self {
            buddy: Default::default(),
            mem_registry: Vec::new(),
            next_alloc: Self::MAX_ALLOC_SIZE,
        }
    }

    /// Allocate a new buffer with at least the given length.
    pub fn alloc<Tp: UnreliableTransport>(&mut self, len: usize, tp: &mut Tp) -> Buffer {
        assert!(
            len <= Self::MAX_ALLOC_SIZE,
            "requested buffer too large (maximum: {}MB)",
            Self::MAX_ALLOC_SIZE >> 20
        );

        let class = Self::class(len);
        if self.buddy[class].is_empty() {
            let higher_class =
                ((class + 1)..Self::NUM_CLASSES).find(|&c| !self.buddy[c].is_empty());
            let higher_class = higher_class.unwrap_or_else(|| {
                self.reserve_memory(tp);
                Self::NUM_CLASSES - 1
            });

            debug_assert!(!self.buddy[higher_class].is_empty());
            for i in ((class + 1)..=higher_class).rev() {
                self.split(i);
            }
            debug_assert!(!self.buddy[class].is_empty());
        }
        self.buddy[class].pop().unwrap()
    }

    /// Free a buffer.
    pub fn free(&mut self, buf: Buffer) {
        let class = Self::class(buf.len());
        self.buddy[class].push(buf);
    }
}

impl Default for BuddyAllocator {
    fn default() -> Self {
        Self::new()
    }
}
