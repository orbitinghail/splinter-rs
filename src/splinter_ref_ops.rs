use std::ops::{
    BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Deref, Sub, SubAssign,
};

use crate::{CowSplinter, Splinter, SplinterRef};

impl<B> PartialEq<Splinter> for SplinterRef<B>
where
    B: Deref<Target = [u8]>,
{
    #[inline]
    fn eq(&self, other: &Splinter) -> bool {
        other == self
    }
}

impl<B, B2> PartialEq<SplinterRef<B2>> for SplinterRef<B>
where
    B: Deref<Target = [u8]>,
    B2: Deref<Target = [u8]>,
{
    fn eq(&self, other: &SplinterRef<B2>) -> bool {
        self.load_unchecked() == other.load_unchecked()
    }
}

impl<B: Deref<Target = [u8]>> Eq for SplinterRef<B> {}

macro_rules! binary_bitop {
    ($a:ty, $b:ty) => {
        binary_bitop!($a, $b, BitAnd, bitand, BitAndAssign::bitand_assign);
        binary_bitop!($a, $b, BitOr, bitor, BitOrAssign::bitor_assign);
        binary_bitop!($a, $b, BitXor, bitxor, BitXorAssign::bitxor_assign);
        binary_bitop!($a, $b, Sub, sub, SubAssign::sub_assign);
    };
    ($a:ty, $b:ty, $BitOp:ident, $bitop:ident, $bitassign:path) => {
        impl<B1: Deref<Target = [u8]>, B2: Deref<Target = [u8]>> $BitOp<$b> for $a {
            type Output = Splinter;
            fn $bitop(self, rhs: $b) -> Self::Output {
                let mut out = self.decode_to_splinter();
                $bitassign(&mut out, rhs);
                out
            }
        }
    };
}

binary_bitop!(SplinterRef<B1>, SplinterRef<B2>);
binary_bitop!(SplinterRef<B1>, &SplinterRef<B2>);
binary_bitop!(&SplinterRef<B1>, SplinterRef<B2>);
binary_bitop!(&SplinterRef<B1>, &SplinterRef<B2>);

binary_bitop!(SplinterRef<B1>, CowSplinter<B2>);
binary_bitop!(SplinterRef<B1>, &CowSplinter<B2>);
binary_bitop!(&SplinterRef<B1>, CowSplinter<B2>);
binary_bitop!(&SplinterRef<B1>, &CowSplinter<B2>);
