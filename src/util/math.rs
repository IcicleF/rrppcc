pub const fn roundup(x: usize, n: usize) -> usize {
    assert!(n.is_power_of_two());
    (x + n - 1) & !(n - 1)
}
