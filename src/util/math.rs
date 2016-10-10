
use num::{Integer, Zero, One};
use std::ops::{Add, Sub};

/// Round n down to nearest multiple of m
pub fn round_down<T>(n: T, m: T) -> T
    where T: Integer + Add<T> + Sub<T> + Copy
{
    if n >= Zero::zero() {
        (n / m) * m
    } else {
        ((n - m + One::one()) / m) * m
    }
}

/// Round n up to nearest multiple of m
pub fn round_up<T>(n: T, m: T) -> T
    where T: Integer + Add<T> + Sub<T> + Copy
{
    if n >= Zero::zero() {
        ((n + m - One::one()) / m) * m
    } else {
        (n / m) * m
    }
}