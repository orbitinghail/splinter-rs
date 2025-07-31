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
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, TryFromBytes, IntoBytes, Immutable, Default,
)]
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
        // we need to shift left one byte to skip the MSB.
        (self.into_u32() << 8).leading_zeros()
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
        Self::truncating_from_u32((x >> n) | (x >> (24 - n)))
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
                Self::must_from_u32(Self::into_u32(self).$op_fn(other.into_u32()))
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
