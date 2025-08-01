use core::fmt;
use std::{
    error::Error,
    num::{IntErrorKind, ParseIntError},
    ops::{
        Add, AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Div,
        DivAssign, Mul, MulAssign, Not, Rem, RemAssign, Shl, Shr, Sub, SubAssign,
    },
};

use num::{
    Bounded, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, FromPrimitive, Num, NumCast, One,
    PrimInt, Saturating, ToPrimitive, Unsigned, Zero,
    cast::AsPrimitive,
    traits::{SaturatingAdd, SaturatingMul, SaturatingSub},
};
use zerocopy::{Immutable, IntoBytes, TryFromBytes, Unaligned};

// The U24 type depends on the native endianness being little-endian
static_assertions::assert_cfg!(target_endian = "little");

#[derive(
    Debug,
    TryFromBytes,
    IntoBytes,
    Immutable,
    Unaligned,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
)]
#[repr(u8)]
enum ZeroByte {
    #[default]
    Zero = 0,
}

/// Little-endian encoded u24
#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromBytes, IntoBytes, Immutable, Default)]
#[repr(C, align(4))]
#[allow(non_camel_case_types)]
pub struct u24 {
    data: [u8; 3],
    msb: ZeroByte,
}

static_assertions::assert_eq_size!(u24, u32);
static_assertions::assert_eq_align!(u24, u32);

impl u24 {
    pub const MAX: u24 = Self {
        data: [0xFF, 0xFF, 0xFF],
        msb: ZeroByte::Zero,
    };
    pub const MIN: u24 = Self {
        data: [0x0, 0x0, 0x0],
        msb: ZeroByte::Zero,
    };
    const U32_DATA_MASK: u32 = 0x00_FFFFFF;

    pub const ZERO: u24 = Self::MIN;
    pub const ONE: u24 = Self {
        data: [0x1, 0x0, 0x0],
        msb: ZeroByte::Zero,
    };

    pub const BITS: u32 = 24;

    #[inline]
    pub const fn from_le_bytes(bytes: [u8; 3]) -> Self {
        Self { data: bytes, msb: ZeroByte::Zero }
    }

    #[inline]
    pub const fn to_le_bytes(self) -> [u8; 3] {
        self.data
    }

    #[inline]
    pub const fn into_u32(self) -> u32 {
        zerocopy::transmute!(self)
    }

    #[inline]
    pub const fn truncating_from_u32(v: u32) -> Self {
        // SAFETY:
        // 1. we mask the MSB to 0
        // 2. both types have the same size and alignment
        unsafe { std::mem::transmute(v & Self::U32_DATA_MASK) }
    }

    #[inline]
    pub const fn checked_from_u32(v: u32) -> Option<Self> {
        if v > Self::MAX.into_u32() {
            None
        } else {
            Some(Self::truncating_from_u32(v))
        }
    }

    #[inline]
    pub const fn saturating_from_u32(v: u32) -> Self {
        match Self::checked_from_u32(v) {
            Some(v) => v,
            None => Self::MAX,
        }
    }

    #[inline]
    pub(crate) const fn must_from_u32(v: u32) -> Self {
        #[cfg(debug_assertions)]
        if v > Self::MAX.into_u32() {
            panic!("value out of range for u24");
        }
        Self::truncating_from_u32(v)
    }

    #[inline]
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.into_u32().checked_add(other.into_u32()) {
            Some(v) if v > Self::MAX.into_u32() => None,
            Some(v) => Some(Self::truncating_from_u32(v)),
            None => None,
        }
    }

    #[inline]
    pub const fn checked_sub(self, other: Self) -> Option<Self> {
        match self.into_u32().checked_sub(other.into_u32()) {
            // no need to check if v > MAX since u32::checked_sub will return
            // None rather than wrapping
            Some(v) => Some(Self::truncating_from_u32(v)),
            None => None,
        }
    }

    #[inline]
    pub const fn checked_mul(self, other: Self) -> Option<Self> {
        match self.into_u32().checked_mul(other.into_u32()) {
            Some(v) if v > Self::MAX.into_u32() => None,
            Some(v) => Some(Self::truncating_from_u32(v)),
            None => None,
        }
    }

    #[inline]
    pub const fn checked_div(self, other: Self) -> Option<Self> {
        match self.into_u32().checked_div(other.into_u32()) {
            // no need to check if v > MAX since u32::checked_div will return
            // None rather than wrapping
            Some(v) => Some(Self::truncating_from_u32(v)),
            None => None,
        }
    }

    #[inline]
    pub const fn saturating_add(self, other: Self) -> Self {
        match self.checked_add(other) {
            Some(v) => v,
            None => Self::MAX,
        }
    }

    #[inline]
    pub const fn saturating_sub(self, other: Self) -> Self {
        match self.checked_sub(other) {
            Some(v) => v,
            None => Self::MIN,
        }
    }

    #[inline]
    pub const fn saturating_mul(self, other: Self) -> Self {
        match self.checked_mul(other) {
            Some(v) => v,
            None => Self::MAX,
        }
    }
}

impl PrimInt for u24 {
    #[inline]
    fn count_ones(self) -> u32 {
        self.into_u32().count_ones()
    }

    #[inline]
    fn count_zeros(self) -> u32 {
        // to count the number of zeros we instead count the number of ones
        // contained in the negated and masked u32 repr. This is to ensure we
        // don't include the MSB in the count.
        (!self.into_u32() & Self::U32_DATA_MASK).count_ones()
    }

    #[inline]
    fn leading_zeros(self) -> u32 {
        if self == Self::ZERO {
            24
        } else {
            // we need to shift left one byte to skip the MSB.
            (self.into_u32() << 8).leading_zeros()
        }
    }

    #[inline]
    fn trailing_zeros(self) -> u32 {
        if self == Self::ZERO {
            24
        } else {
            self.into_u32().trailing_zeros()
        }
    }

    #[inline]
    fn rotate_left(self, n: u32) -> Self {
        let n = n % 24; // Handle rotation > 24 bits
        let x = self.into_u32();
        Self::truncating_from_u32((x << n) | (x >> (24 - n)))
    }

    #[inline]
    fn rotate_right(self, n: u32) -> Self {
        let n = n % 24; // Handle rotation > 24 bits
        let x = self.into_u32();
        Self::truncating_from_u32((x >> n) | (x << (24 - n)))
    }

    #[inline]
    fn signed_shl(self, n: u32) -> Self {
        Self::truncating_from_u32(((self.into_u32() as i32) << n) as u32)
    }

    #[inline]
    fn signed_shr(self, n: u32) -> Self {
        Self::truncating_from_u32(((self.into_u32() as i32) >> n) as u32)
    }

    #[inline]
    fn unsigned_shl(self, n: u32) -> Self {
        self << (n as usize)
    }

    #[inline]
    fn unsigned_shr(self, n: u32) -> Self {
        self >> (n as usize)
    }

    #[inline]
    fn swap_bytes(self) -> Self {
        let d = self.data;
        Self {
            data: [d[2], d[1], d[0]],
            msb: ZeroByte::Zero,
        }
    }

    #[inline]
    fn from_be(x: Self) -> Self {
        x.swap_bytes()
    }

    #[inline]
    fn from_le(x: Self) -> Self {
        x
    }

    #[inline]
    fn to_be(self) -> Self {
        self.swap_bytes()
    }

    #[inline]
    fn to_le(self) -> Self {
        self
    }

    #[inline]
    fn pow(self, exp: u32) -> Self {
        Self::must_from_u32(self.into_u32().pow(exp))
    }
}

impl Zero for u24 {
    fn zero() -> Self {
        Self::MIN
    }

    fn is_zero(&self) -> bool {
        *self == Self::MIN
    }
}

impl One for u24 {
    fn one() -> Self {
        Self::ONE
    }
}

#[derive(Debug)]
pub struct ParseU24Err(IntErrorKind);

impl fmt::Display for ParseU24Err {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            IntErrorKind::Empty => write!(f, "cannot parse integer from empty string"),
            IntErrorKind::InvalidDigit => write!(f, "invalid digit found in string"),
            IntErrorKind::PosOverflow => write!(f, "number too large to fit in target type"),
            IntErrorKind::NegOverflow => write!(f, "number too small to fit in target type"),
            IntErrorKind::Zero => write!(f, "number would be zero for non-zero type"),
            other => write!(f, "unknown error: {other:?}"),
        }
    }
}

impl Error for ParseU24Err {}

impl From<ParseIntError> for ParseU24Err {
    fn from(err: ParseIntError) -> Self {
        Self(err.kind().clone())
    }
}

impl PartialOrd for u24 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.into_u32().partial_cmp(&other.into_u32())
    }
}

impl Ord for u24 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.into_u32().cmp(&other.into_u32())
    }
}

impl Num for u24 {
    type FromStrRadixErr = ParseU24Err;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        let v = u32::from_str_radix(str, radix)?;
        if v > Self::MAX.into_u32() {
            Err(ParseU24Err(IntErrorKind::PosOverflow))
        } else {
            Ok(Self::truncating_from_u32(v))
        }
    }
}

macro_rules! impl_bin_op {
    ($(($op:ident, $meth:ident, $assign_op:ident, $assign_meth:ident, $op_fn:ident),)*) => {
        $(
            impl_bin_op!(@ u24, $op, $meth, $assign_op, $assign_meth, $op_fn);
            impl_bin_op!(@ &u24, $op, $meth, $assign_op, $assign_meth, $op_fn);
        )*
    };

    (@ $ty:ty, $op:ident, $meth:ident, $assign_op:ident, $assign_meth:ident, $op_fn:ident) => {
        impl $op<$ty> for u24 {
            type Output = Self;

            #[inline(always)]
            fn $meth(self, other: $ty) -> Self {
                Self::truncating_from_u32(Self::into_u32(self).$op_fn(other.into_u32()))
            }
        }

        impl $op<$ty> for &u24 {
            type Output = u24;

            #[inline(always)]
            fn $meth(self, other: $ty) -> u24 {
                <u24 as $op<$ty>>::$meth(*self, other)
            }
        }

        impl $assign_op<$ty> for u24 {
            #[inline(always)]
            fn $assign_meth(&mut self, rhs: $ty) {
                *self = $op::$meth(*self, rhs)
            }
        }
    }
}

impl_bin_op!(
    (Add, add, AddAssign, add_assign, wrapping_add),
    (Sub, sub, SubAssign, sub_assign, wrapping_sub),
    (Mul, mul, MulAssign, mul_assign, wrapping_mul),
    (Div, div, DivAssign, div_assign, wrapping_div),
    (Rem, rem, RemAssign, rem_assign, wrapping_rem),
    (BitAnd, bitand, BitAndAssign, bitand_assign, bitand),
    (BitOr, bitor, BitOrAssign, bitor_assign, bitor),
    (BitXor, bitxor, BitXorAssign, bitxor_assign, bitxor),
);

impl Shl<usize> for u24 {
    type Output = u24;

    #[inline]
    fn shl(self, rhs: usize) -> Self::Output {
        Self::truncating_from_u32(self.into_u32() << rhs)
    }
}

impl Shr<usize> for u24 {
    type Output = u24;

    #[inline]
    fn shr(self, rhs: usize) -> Self::Output {
        Self::truncating_from_u32(self.into_u32() >> rhs)
    }
}

impl Not for u24 {
    type Output = u24;

    #[inline]
    fn not(self) -> Self::Output {
        Self::truncating_from_u32(!self.into_u32())
    }
}

impl Unsigned for u24 {}

macro_rules! forward_impl {
    ($(($trait:ty, $method:ident, $return:ty),)*) => {
        $(
            impl $trait for u24 {
                #[inline]
                fn $method(&self, other: &Self) -> $return {
                    Self::$method(*self, *other)
                }
            }
        )*
    };
}

forward_impl!(
    (CheckedAdd, checked_add, Option<u24>),
    (CheckedSub, checked_sub, Option<u24>),
    (CheckedMul, checked_mul, Option<u24>),
    (CheckedDiv, checked_div, Option<u24>),
    (SaturatingAdd, saturating_add, u24),
    (SaturatingSub, saturating_sub, u24),
    (SaturatingMul, saturating_mul, u24),
);

impl Saturating for u24 {
    #[inline]
    fn saturating_add(self, v: Self) -> Self {
        Self::saturating_add(self, v)
    }

    #[inline]
    fn saturating_sub(self, v: Self) -> Self {
        Self::saturating_sub(self, v)
    }
}

impl NumCast for u24 {
    #[inline]
    fn from<T: num::ToPrimitive>(n: T) -> Option<Self> {
        n.to_u32().and_then(Self::checked_from_u32)
    }
}

impl ToPrimitive for u24 {
    #[inline]
    fn to_i64(&self) -> Option<i64> {
        Some(Self::into_u32(*self) as i64)
    }

    #[inline]
    fn to_u64(&self) -> Option<u64> {
        Some(Self::into_u32(*self) as u64)
    }

    #[inline]
    fn to_u32(&self) -> Option<u32> {
        Some(Self::into_u32(*self))
    }
}

impl FromPrimitive for u24 {
    #[inline]
    fn from_i64(n: i64) -> Option<Self> {
        <u32 as FromPrimitive>::from_i64(n).and_then(Self::checked_from_u32)
    }

    #[inline]
    fn from_u64(n: u64) -> Option<Self> {
        <u32 as FromPrimitive>::from_u64(n).and_then(Self::checked_from_u32)
    }
}

impl Bounded for u24 {
    #[inline]
    fn min_value() -> Self {
        Self::MIN
    }

    #[inline]
    fn max_value() -> Self {
        Self::MAX
    }
}

macro_rules! impl_as {
    ($($ty:ty),*) => {
        $(
            impl AsPrimitive<$ty> for u24 {
                #[inline]
                fn as_(self) -> $ty {
                    self.into_u32() as $ty
                }
            }

            impl AsPrimitive<u24> for $ty {
                #[inline]
                fn as_(self) -> u24 {
                    u24::truncating_from_u32(self.as_())
                }
            }
        )*
    };
}

impl_as!(usize, u64, u32, u16, u8, i64, i32, i16, i8);

impl AsPrimitive<u24> for u24 {
    #[inline]
    fn as_(self) -> u24 {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num::{Bounded, FromPrimitive, Num, NumCast, One, PrimInt, ToPrimitive, Zero};

    #[test]
    fn test_constants() {
        assert_eq!(u24::MIN.into_u32(), 0);
        assert_eq!(u24::MAX.into_u32(), 0x00_FFFFFF);
        assert_eq!(u24::ZERO.into_u32(), 0);
        assert_eq!(u24::ONE.into_u32(), 1);
        assert_eq!(u24::BITS, 24);
    }

    #[test]
    fn test_size_and_alignment() {
        assert_eq!(std::mem::size_of::<u24>(), 4);
        assert_eq!(std::mem::align_of::<u24>(), 4);
        assert_eq!(std::mem::size_of::<u24>(), std::mem::size_of::<u32>());
        assert_eq!(std::mem::align_of::<u24>(), std::mem::align_of::<u32>());
    }

    #[test]
    fn test_byte_conversions() {
        let val = u24::from_le_bytes([0x34, 0x12, 0xAB]);
        assert_eq!(val.to_le_bytes(), [0x34, 0x12, 0xAB]);
        assert_eq!(val.into_u32(), 0x00_AB1234);

        let val = u24::from_le_bytes([0xFF, 0xFF, 0xFF]);
        assert_eq!(val, u24::MAX);
        assert_eq!(val.into_u32(), 0x00_FFFFFF);
    }

    #[test]
    fn test_u32_conversions() {
        // Valid conversions
        assert_eq!(u24::checked_from_u32(0), Some(u24::MIN));
        assert_eq!(u24::checked_from_u32(1), Some(u24::ONE));
        assert_eq!(u24::checked_from_u32(0xFF_FFFF), Some(u24::MAX));

        // Invalid conversions (overflow)
        assert_eq!(u24::checked_from_u32(0x01_000000), None);
        assert_eq!(u24::checked_from_u32(u32::MAX), None);

        // Truncating conversions
        assert_eq!(
            u24::truncating_from_u32(0x01_234567).into_u32(),
            0x00_234567
        );
        assert_eq!(u24::truncating_from_u32(u32::MAX).into_u32(), 0x00_FFFFFF);

        // Saturating conversions
        assert_eq!(u24::saturating_from_u32(0), u24::MIN);
        assert_eq!(u24::saturating_from_u32(0xFF_FFFF), u24::MAX);
        assert_eq!(u24::saturating_from_u32(0x01_000000), u24::MAX);
        assert_eq!(u24::saturating_from_u32(u32::MAX), u24::MAX);
    }

    #[test]
    fn test_arithmetic_basic() {
        let a = u24::truncating_from_u32(100);
        let b = u24::truncating_from_u32(200);

        assert_eq!((a + b).into_u32(), 300);
        assert_eq!((b - a).into_u32(), 100);
        assert_eq!((a * b).into_u32(), 20000);
        assert_eq!((b / a).into_u32(), 2);
        assert_eq!((b % a).into_u32(), 0);
    }

    #[test]
    fn test_arithmetic_wrapping() {
        let max = u24::MAX;
        let one = u24::ONE;

        // Test wrapping addition
        let result = max + one;
        assert_eq!(result.into_u32(), 0); // Should wrap to 0

        // Test wrapping subtraction
        let zero = u24::ZERO;
        let result = zero - one;
        assert_eq!(result.into_u32(), 0x00_FFFFFF); // Should wrap to MAX
    }

    #[test]
    fn test_checked_arithmetic() {
        let a = u24::truncating_from_u32(0xFF_FFFE);
        let b = u24::truncating_from_u32(2);
        let one = u24::ONE;

        // Checked addition
        assert_eq!(a.checked_add(one), Some(u24::MAX));
        assert_eq!(a.checked_add(b), None); // Overflow
        assert_eq!(u24::MAX.checked_add(one), None); // Overflow

        // Checked subtraction
        assert_eq!(b.checked_sub(one), Some(one));
        assert_eq!(u24::ZERO.checked_sub(one), None); // Underflow

        // Checked multiplication
        let small = u24::truncating_from_u32(1000);
        assert_eq!(small.checked_mul(small).unwrap().into_u32(), 1_000_000);
        assert_eq!(u24::MAX.checked_mul(b), None); // Overflow

        // Checked division
        assert_eq!(b.checked_div(one), Some(b));
        assert_eq!(b.checked_div(u24::ZERO), None); // Division by zero
    }

    #[test]
    fn test_saturating_arithmetic() {
        let max = u24::MAX;
        let one = u24::ONE;
        let zero = u24::ZERO;

        // Saturating addition
        assert_eq!(max.saturating_add(one), max);
        assert_eq!(one.saturating_add(one).into_u32(), 2);

        // Saturating subtraction
        assert_eq!(zero.saturating_sub(one), zero);
        assert_eq!(one.saturating_sub(one), zero);

        // Saturating multiplication
        assert_eq!(max.saturating_mul(u24::truncating_from_u32(2)), max);
    }

    #[test]
    fn test_bitwise_operations() {
        let a = u24::truncating_from_u32(0xAA_AAAA);
        let b = u24::truncating_from_u32(0x55_5555);
        let all_ones = u24::MAX;

        assert_eq!((a & b).into_u32(), 0);
        assert_eq!((a | b).into_u32(), 0xFF_FFFF);
        assert_eq!((a ^ b).into_u32(), 0xFF_FFFF);
        assert_eq!((!u24::ZERO).into_u32(), 0xFF_FFFF);
        assert_eq!((!all_ones).into_u32(), 0);
    }

    #[test]
    fn test_shift_operations() {
        let val = u24::truncating_from_u32(0x12_3456);

        // Left shift
        assert_eq!((val << 4).into_u32(), 0x23_4560);
        assert_eq!((val << 8).into_u32(), 0x34_5600);

        // Right shift
        assert_eq!((val >> 4).into_u32(), 0x01_2345);
        assert_eq!((val >> 8).into_u32(), 0x00_1234);

        // Edge cases
        assert_eq!((val << 24).into_u32(), 0); // Shift out all bits
        assert_eq!((val >> 24).into_u32(), 0); // Shift out all bits
    }

    #[test]
    fn test_rotation() {
        let val = u24::truncating_from_u32(0x12_3456);

        // Left rotation
        assert_eq!(val.rotate_left(4).into_u32(), 0x23_4561);
        assert_eq!(val.rotate_left(8).into_u32(), 0x34_5612);
        assert_eq!(val.rotate_left(24).into_u32(), val.into_u32()); // Full rotation

        // Right rotation
        assert_eq!(val.rotate_right(4).into_u32(), 0x61_2345);
        assert_eq!(val.rotate_right(8).into_u32(), 0x56_1234);
        assert_eq!(val.rotate_right(24).into_u32(), val.into_u32()); // Full rotation
    }

    #[test]
    fn test_bit_counting() {
        let val = u24::truncating_from_u32(0xFF_0000);
        assert_eq!(val.count_ones(), 8);
        assert_eq!(val.count_zeros(), 16);

        let val = u24::truncating_from_u32(0x00_00FF);
        assert_eq!(val.count_ones(), 8);
        assert_eq!(val.count_zeros(), 16);

        assert_eq!(u24::ZERO.count_ones(), 0);
        assert_eq!(u24::ZERO.count_zeros(), 24);
        assert_eq!(u24::MAX.count_ones(), 24);
        assert_eq!(u24::MAX.count_zeros(), 0);
    }

    #[test]
    fn test_leading_trailing_zeros() {
        let val = u24::truncating_from_u32(0x10_0000);
        assert_eq!(val.leading_zeros(), 3); // 24 - 21 = 3 (bit 20 is set)
        assert_eq!(val.trailing_zeros(), 20);

        let val = u24::truncating_from_u32(0x00_0001);
        assert_eq!(val.leading_zeros(), 23);
        assert_eq!(val.trailing_zeros(), 0);

        assert_eq!(u24::ZERO.leading_zeros(), 24);
        assert_eq!(u24::ZERO.trailing_zeros(), 24);
        assert_eq!(u24::MAX.leading_zeros(), 0);
        assert_eq!(u24::MAX.trailing_zeros(), 0);
    }

    #[test]
    fn test_byte_swapping() {
        let val = u24::from_le_bytes([0x12, 0x34, 0x56]);
        let swapped = val.swap_bytes();
        assert_eq!(swapped.to_le_bytes(), [0x56, 0x34, 0x12]);

        // Test endianness conversions
        assert_eq!(u24::from_le(val), val);
        assert_eq!(u24::to_le(val), val);
        assert_eq!(u24::from_be(val), swapped);
        assert_eq!(u24::to_be(val), swapped);
    }

    #[test]
    fn test_comparison() {
        let a = u24::truncating_from_u32(100);
        let b = u24::truncating_from_u32(200);

        assert!(a < b);
        assert!(b > a);
        assert!(a == a);
        assert!(a != b);
        assert!(a <= a);
        assert!(a <= b);
        assert!(b >= a);
        assert!(b >= b);
    }

    #[test]
    fn test_trait_implementations() {
        // Zero trait
        assert_eq!(u24::zero(), u24::ZERO);
        assert!(u24::ZERO.is_zero());
        assert!(!u24::ONE.is_zero());

        // One trait
        assert_eq!(u24::one(), u24::ONE);

        // Bounded trait
        assert_eq!(u24::min_value(), u24::MIN);
        assert_eq!(u24::max_value(), u24::MAX);
    }

    #[test]
    fn test_string_parsing() {
        // Decimal parsing
        assert_eq!(u24::from_str_radix("0", 10).unwrap(), u24::ZERO);
        assert_eq!(u24::from_str_radix("1", 10).unwrap(), u24::ONE);
        assert_eq!(u24::from_str_radix("16777215", 10).unwrap(), u24::MAX); // 2^24 - 1

        // Hex parsing
        assert_eq!(u24::from_str_radix("FFFFFF", 16).unwrap(), u24::MAX);
        assert_eq!(
            u24::from_str_radix("123456", 16).unwrap(),
            u24::truncating_from_u32(0x12_3456)
        );

        // Error cases
        assert!(u24::from_str_radix("16777216", 10).is_err()); // 2^24, too large
        assert!(u24::from_str_radix("1000000", 16).is_err()); // > 0xFFFFFF
        assert!(u24::from_str_radix("", 10).is_err());
        assert!(u24::from_str_radix("abc", 10).is_err());
    }

    #[test]
    fn test_numeric_conversions() {
        let val = u24::truncating_from_u32(0x12_3456);

        // ToPrimitive
        assert_eq!(val.to_u32(), Some(0x12_3456));
        assert_eq!(val.to_u64(), Some(0x12_3456));
        assert_eq!(val.to_i64(), Some(0x12_3456));

        // FromPrimitive
        assert_eq!(u24::from_u64(0x12_3456), Some(val));
        assert_eq!(u24::from_i64(0x12_3456), Some(val));
        assert_eq!(u24::from_u64(0x01_000000), None); // Overflow
        assert_eq!(u24::from_i64(-1), None); // Negative

        // NumCast
        assert_eq!(<u24 as NumCast>::from(0x12_3456u32), Some(val));
        assert_eq!(<u24 as NumCast>::from(0x01_000000u32), None); // Overflow
    }

    #[test]
    fn test_assignment_operators() {
        let mut val = u24::truncating_from_u32(100);

        val += u24::truncating_from_u32(50);
        assert_eq!(val.into_u32(), 150);

        val -= u24::truncating_from_u32(25);
        assert_eq!(val.into_u32(), 125);

        val *= u24::truncating_from_u32(2);
        assert_eq!(val.into_u32(), 250);

        val /= u24::truncating_from_u32(5);
        assert_eq!(val.into_u32(), 50);

        val %= u24::truncating_from_u32(7);
        assert_eq!(val.into_u32(), 1);
    }

    #[test]
    fn test_error_display() {
        use std::error::Error;

        let parse_err = u24::from_str_radix("", 10).unwrap_err();
        assert!(!parse_err.to_string().is_empty());
        assert!(parse_err.source().is_none());

        let overflow_err = u24::from_str_radix("16777216", 10).unwrap_err();
        assert!(overflow_err.to_string().contains("too large"));
    }

    #[test]
    fn test_power_operations() {
        let base = u24::truncating_from_u32(2);
        assert_eq!(base.pow(0).into_u32(), 1);
        assert_eq!(base.pow(1).into_u32(), 2);
        assert_eq!(base.pow(10).into_u32(), 1024);

        // This should panic in debug mode if result > MAX
        let _large_base = u24::truncating_from_u32(256);
        // Note: This might panic in debug builds
        #[cfg(not(debug_assertions))]
        {
            let _result = _large_base.pow(3); // 256^3 = 16777216 > MAX
        }
    }

    #[test]
    #[should_panic]
    #[cfg(debug_assertions)]
    fn test_must_from_u32_panic() {
        u24::must_from_u32(0x01_000000); // Should panic in debug mode
    }

    #[test]
    fn test_must_from_u32_no_panic() {
        let val = u24::must_from_u32(0xFF_FFFF);
        assert_eq!(val, u24::MAX);

        #[cfg(not(debug_assertions))]
        {
            // In release mode, should truncate silently
            let val = u24::must_from_u32(0x01_000000);
            assert_eq!(val.into_u32(), 0x00_000000);
        }
    }
}
