#[cold]
const fn cold() {}

/// Hints to the compiler that the given branch is likely to be taken.
#[inline]
pub const fn likely(b: bool) -> bool {
    if !b {
        cold();
    }
    b
}

/// Hints to the compiler that the given branch is unlikely to be taken.
#[inline]
pub const fn unlikely(b: bool) -> bool {
    if b {
        cold();
    }
    b
}
