use std::{
    mem,
    ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Deref, Sub, SubAssign},
};

use crate::{CowSplinter, Cut, PartitionRead, Splinter, SplinterRef};

impl<B: Deref<Target = [u8]>> PartialEq<SplinterRef<B>> for Splinter {
    #[inline]
    fn eq(&self, other: &SplinterRef<B>) -> bool {
        self.inner() == &other.load_unchecked()
    }
}

impl<B: Deref<Target = [u8]>> PartialEq<CowSplinter<B>> for Splinter {
    fn eq(&self, other: &CowSplinter<B>) -> bool {
        match other {
            CowSplinter::Ref(splinter_ref) => self.eq(splinter_ref),
            CowSplinter::Owned(splinter) => self.eq(splinter),
        }
    }
}

impl Cut for Splinter {
    type Out = Self;

    fn cut(&mut self, rhs: &Self) -> Self::Out {
        Self::new(self.inner_mut().cut(rhs.inner()))
    }
}

impl<B: Deref<Target = [u8]>> Cut<SplinterRef<B>> for Splinter {
    type Out = Self;

    fn cut(&mut self, rhs: &SplinterRef<B>) -> Self::Out {
        Self::new(self.inner_mut().cut(&rhs.load_unchecked()))
    }
}

impl<B: Deref<Target = [u8]>> Cut<CowSplinter<B>> for Splinter {
    type Out = Self;

    fn cut(&mut self, rhs: &CowSplinter<B>) -> Self::Out {
        match rhs {
            CowSplinter::Ref(splinter_ref) => self.cut(splinter_ref),
            CowSplinter::Owned(splinter) => self.cut(splinter),
        }
    }
}

macro_rules! binary_bitop {
    ($BitOp:tt, $bitop:ident, $bitassign:path) => {
        impl $BitOp<Splinter> for Splinter {
            type Output = Splinter;
            fn $bitop(mut self, rhs: Splinter) -> Self::Output {
                $bitassign(&mut self, rhs);
                self
            }
        }
        impl $BitOp<&Splinter> for Splinter {
            type Output = Splinter;
            fn $bitop(mut self, rhs: &Splinter) -> Self::Output {
                $bitassign(&mut self, rhs);
                self
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOp<SplinterRef<B>> for Splinter {
            type Output = Splinter;
            fn $bitop(mut self, rhs: SplinterRef<B>) -> Self::Output {
                $bitassign(&mut self, rhs);
                self
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOp<&SplinterRef<B>> for Splinter {
            type Output = Splinter;
            fn $bitop(mut self, rhs: &SplinterRef<B>) -> Self::Output {
                $bitassign(&mut self, rhs);
                self
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOp<SplinterRef<B>> for &Splinter {
            type Output = Splinter;
            fn $bitop(self, rhs: SplinterRef<B>) -> Self::Output {
                $BitOp::$bitop(self.clone(), rhs)
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOp<&SplinterRef<B>> for &Splinter {
            type Output = Splinter;
            fn $bitop(self, rhs: &SplinterRef<B>) -> Self::Output {
                $BitOp::$bitop(self.clone(), rhs)
            }
        }
    };
}

macro_rules! unary_bitassign {
    ($BitOpAssign:tt, $bitassign:ident) => {
        impl $BitOpAssign<&Splinter> for Splinter {
            fn $bitassign(&mut self, rhs: &Splinter) {
                $BitOpAssign::$bitassign(self.inner_mut(), rhs.inner())
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOpAssign<SplinterRef<B>> for Splinter {
            fn $bitassign(&mut self, rhs: SplinterRef<B>) {
                $BitOpAssign::$bitassign(self.inner_mut(), &rhs.load_unchecked())
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOpAssign<&SplinterRef<B>> for Splinter {
            fn $bitassign(&mut self, rhs: &SplinterRef<B>) {
                $BitOpAssign::$bitassign(self.inner_mut(), &rhs.load_unchecked())
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOpAssign<CowSplinter<B>> for Splinter {
            fn $bitassign(&mut self, rhs: CowSplinter<B>) {
                match rhs {
                    CowSplinter::Ref(splinter_ref) => $BitOpAssign::$bitassign(self, splinter_ref),
                    CowSplinter::Owned(splinter) => $BitOpAssign::$bitassign(self, splinter),
                }
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOpAssign<&CowSplinter<B>> for Splinter {
            fn $bitassign(&mut self, rhs: &CowSplinter<B>) {
                match rhs {
                    CowSplinter::Ref(splinter_ref) => $BitOpAssign::$bitassign(self, splinter_ref),
                    CowSplinter::Owned(splinter) => $BitOpAssign::$bitassign(self, splinter),
                }
            }
        }
    };
}

binary_bitop!(BitOr, bitor, BitOrAssign::bitor_assign);
unary_bitassign!(BitOrAssign, bitor_assign);

binary_bitop!(BitAnd, bitand, BitAndAssign::bitand_assign);
unary_bitassign!(BitAndAssign, bitand_assign);

binary_bitop!(BitXor, bitxor, BitXorAssign::bitxor_assign);
unary_bitassign!(BitXorAssign, bitxor_assign);

binary_bitop!(Sub, sub, SubAssign::sub_assign);
unary_bitassign!(SubAssign, sub_assign);

impl BitOr<&Splinter> for &Splinter {
    type Output = Splinter;
    fn bitor(self, rhs: &Splinter) -> Self::Output {
        // merge into the larger set
        if rhs.cardinality() > self.cardinality() {
            let mut result = rhs.clone();
            result.inner_mut().bitor_assign(self.inner());
            result
        } else {
            let mut result = self.clone();
            result.inner_mut().bitor_assign(rhs.inner());
            result
        }
    }
}

impl BitOr<Splinter> for &Splinter {
    type Output = Splinter;
    fn bitor(self, mut rhs: Splinter) -> Self::Output {
        rhs |= self;
        rhs
    }
}

impl BitOrAssign<Splinter> for Splinter {
    fn bitor_assign(&mut self, mut rhs: Splinter) {
        // merge into the larger set
        if rhs.cardinality() > self.cardinality() {
            mem::swap(self, &mut rhs);
        }
        self.inner_mut().bitor_assign(rhs.inner())
    }
}

impl BitAnd<&Splinter> for &Splinter {
    type Output = Splinter;
    fn bitand(self, rhs: &Splinter) -> Self::Output {
        // intersect into the smaller set
        if rhs.cardinality() < self.cardinality() {
            let mut result = rhs.clone();
            result.inner_mut().bitand_assign(self.inner());
            result
        } else {
            let mut result = self.clone();
            result.inner_mut().bitand_assign(rhs.inner());
            result
        }
    }
}

impl BitAnd<Splinter> for &Splinter {
    type Output = Splinter;
    fn bitand(self, mut rhs: Splinter) -> Self::Output {
        rhs &= self;
        rhs
    }
}

impl BitAndAssign<Splinter> for Splinter {
    fn bitand_assign(&mut self, mut rhs: Splinter) {
        // intersect into the smaller set
        if rhs.cardinality() < self.cardinality() {
            mem::swap(self, &mut rhs);
        }
        self.inner_mut().bitand_assign(rhs.inner())
    }
}

impl BitXor<&Splinter> for &Splinter {
    type Output = Splinter;
    fn bitxor(self, rhs: &Splinter) -> Self::Output {
        let mut result = self.clone();
        result.inner_mut().bitxor_assign(rhs.inner());
        result
    }
}

impl BitXor<Splinter> for &Splinter {
    type Output = Splinter;
    fn bitxor(self, mut rhs: Splinter) -> Self::Output {
        rhs ^= self;
        rhs
    }
}

impl BitXorAssign<Splinter> for Splinter {
    fn bitxor_assign(&mut self, rhs: Splinter) {
        self.inner_mut().bitxor_assign(rhs.inner())
    }
}

impl Sub<&Splinter> for &Splinter {
    type Output = Splinter;
    fn sub(self, rhs: &Splinter) -> Self::Output {
        let mut result = self.clone();
        result.inner_mut().sub_assign(rhs.inner());
        result
    }
}

impl Sub<Splinter> for &Splinter {
    type Output = Splinter;
    fn sub(self, rhs: Splinter) -> Self::Output {
        self - &rhs
    }
}

impl SubAssign<Splinter> for Splinter {
    fn sub_assign(&mut self, rhs: Splinter) {
        self.inner_mut().sub_assign(rhs.inner())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{
        BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Sub, SubAssign,
    };

    use itertools::Itertools;
    use proptest::collection::{hash_set, vec};
    use proptest::proptest;

    use crate::{Optimizable, Splinter, testutil::mksplinter, traits::Cut};

    macro_rules! test_bitop {
        ($test_name:ident, $op_method:ident, $op_assign_method:ident, $hashset_method:ident) => {
            proptest! {
                #[test]
                fn $test_name(
                    optimize: bool,
                    a in hash_set(0u32..16384, 0..1024),
                    b in hash_set(0u32..16384, 0..1024),
                ) {
                    let expected: Splinter = a.$hashset_method(&b).copied().collect();

                    let mut a = Splinter::from_iter(a);
                    let b = Splinter::from_iter(b);

                    if optimize {
                        a.optimize();
                    }

                    // test all combinations of refs
                    assert_eq!((&a).$op_method(&b), expected, "&a, &b");
                    assert_eq!((&a).$op_method(b.clone()), expected, "&a, b");
                    assert_eq!(a.clone().$op_method(&b), expected, "a, &b");
                    assert_eq!(a.clone().$op_method(b.clone()), expected, "a, b");

                    // assignment operator
                    let mut c = a.clone();
                    c.$op_assign_method(b.clone());
                    assert_eq!(c, expected, "c assign b");

                    let mut c = a.clone();
                    c.$op_assign_method(&b);
                    assert_eq!(c, expected, "c assign &b");

                    // do it all again but against a splinter ref
                    let b = b.encode_to_splinter_ref();

                    assert_eq!((&a).$op_method(&b), expected, "&a, &bref");
                    assert_eq!((&a).$op_method(b.clone()), expected, "&a, bref");
                    assert_eq!(a.clone().$op_method(&b), expected, "a, &bref");
                    assert_eq!(a.clone().$op_method(b.clone()), expected, "a, bref");

                    // assignment operator
                    let mut c = a.clone();
                    c.$op_assign_method(b.clone());
                    assert_eq!(c, expected, "c assign bref");

                    let mut c = a.clone();
                    c.$op_assign_method(&b);
                    assert_eq!(c, expected, "c assign &bref");
                }
            }
        };
    }

    proptest! {
        #[test]
        fn test_splinter_equality_proptest(values in vec(0u32..16384, 0..1024)) {
            let mut a = mksplinter(&values);
            a.optimize();
            let b = mksplinter(&values);
            assert!(a == b)
        }

        #[test]
        fn test_splinter_equality_ref_proptest(values in vec(0u32..16384, 0..1024)) {
            let mut a = mksplinter(&values);
            a.optimize();
            let b = mksplinter(&values).encode_to_splinter_ref();
            assert!(a == b)
        }

        #[test]
        fn test_splinter_equality_proptest_2(
            a in vec(0u32..16384, 0..1024),
            b in vec(0u32..16384, 0..1024),
        ) {
            let expected = itertools::equal(a.iter().sorted().dedup(), b.iter().sorted().dedup());

            let mut a = mksplinter(&a);
            a.optimize();
            let b = mksplinter(&b);

            assert!((a == b) == expected)
        }

        #[test]
        fn test_splinter_equality_ref_proptest_2(
            a in vec(0u32..16384, 0..1024),
            b in vec(0u32..16384, 0..1024),
        ) {
            let expected = itertools::equal(a.iter().sorted().dedup(), b.iter().sorted().dedup());

            let mut a = mksplinter(&a);
            a.optimize();
            let b = mksplinter(&b).encode_to_splinter_ref();

            assert!((a == b) == expected)
        }

        #[test]
        fn test_bitor_assign_proptest(
            optimize: bool,
            a in hash_set(0u32..16384, 0..1024),
            b in hash_set(0u32..16384, 0..1024),
        ) {
            let mut set: Splinter = a.iter().copied().collect();
            let other: Splinter = b.iter().copied().collect();

            if optimize {
                set.optimize();
            }

            let expected: Splinter = a.union(&b).copied().collect();
            set |= other;
            assert!(set == expected)
        }

        #[test]
        fn test_cut_proptest(
            optimize: bool,
            a in hash_set(0u32..16384, 0..1024),
            b in hash_set(0u32..16384, 0..1024),
        ) {
            let mut source: Splinter = a.iter().copied().collect();
            let other: Splinter = b.iter().copied().collect();

            if optimize {
                source.optimize();
            }

            let expected_intersection: Splinter = a.intersection(&b).copied().collect();
            let expected_remaining: Splinter = a.difference(&b).copied().collect();

            let actual_intersection = source.cut(&other);

            assert_eq!(actual_intersection,expected_intersection);
            assert_eq!(source,expected_remaining);
        }

        #[test]
        fn test_bitor_ref_proptest(
            optimize: bool,
            a in hash_set(0u32..16384, 0..1024),
            b in hash_set(0u32..16384, 0..1024),
        ) {
            let mut set: Splinter = a.iter().copied().collect();
            let other_ref = Splinter::from_iter(b.clone()).encode_to_splinter_ref();

            if optimize {
                set.optimize();
            }

            let expected: Splinter = a.union(&b).copied().collect();
            set |= other_ref;
            assert!(set == expected)
        }

        #[test]
        fn test_cut_ref_proptest(
            optimize: bool,
            a in hash_set(0u32..16384, 0..1024),
            b in hash_set(0u32..16384, 0..1024),
        ) {
            let mut source: Splinter = a.iter().copied().collect();
            let other_ref = Splinter::from_iter(b.clone()).encode_to_splinter_ref();

            if optimize {
                source.optimize();
            }

            let expected_intersection: Splinter = a.intersection(&b).copied().collect();
            let expected_remaining: Splinter = a.difference(&b).copied().collect();

            let actual_intersection = source.cut(&other_ref);

            assert_eq!(actual_intersection,expected_intersection);
            assert_eq!(source,expected_remaining);
        }
    }

    test_bitop!(test_bitor, bitor, bitor_assign, union);
    test_bitop!(test_bitand, bitand, bitand_assign, intersection);
    test_bitop!(test_bitxor, bitxor, bitxor_assign, symmetric_difference);
    test_bitop!(test_sub, sub, sub_assign, difference);
}
