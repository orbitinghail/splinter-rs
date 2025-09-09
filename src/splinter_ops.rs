use std::{
    mem,
    ops::{BitOr, BitOrAssign, Deref},
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
        impl $BitOp<Splinter> for &Splinter {
            type Output = Splinter;
            fn $bitop(self, mut rhs: Splinter) -> Self::Output {
                $bitassign(&mut rhs, self);
                rhs
            }
        }
        impl $BitOp<&Splinter> for &Splinter {
            type Output = Splinter;
            fn $bitop(self, rhs: &Splinter) -> Self::Output {
                self.clone() | rhs
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOp<SplinterRef<B>> for Splinter {
            type Output = Splinter;
            fn $bitop(mut self, rhs: SplinterRef<B>) -> Self::Output {
                self |= rhs;
                self
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOp<SplinterRef<B>> for &Splinter {
            type Output = Splinter;
            fn $bitop(self, rhs: SplinterRef<B>) -> Self::Output {
                self.clone() | rhs
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOp<&SplinterRef<B>> for Splinter {
            type Output = Splinter;
            fn $bitop(mut self, rhs: &SplinterRef<B>) -> Self::Output {
                self |= rhs;
                self
            }
        }
        impl<B: Deref<Target = [u8]>> $BitOp<&SplinterRef<B>> for &Splinter {
            type Output = Splinter;
            fn $bitop(self, rhs: &SplinterRef<B>) -> Self::Output {
                self.clone() | rhs
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

impl BitOrAssign<Splinter> for Splinter {
    fn bitor_assign(&mut self, mut rhs: Splinter) {
        // merge into the larger set
        if rhs.cardinality() > self.cardinality() {
            mem::swap(self, &mut rhs);
        }
        self.inner_mut().bitor_assign(rhs.inner())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use itertools::Itertools;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::{Optimizable, Splinter, testutil::mksplinter, traits::Cut};

    #[quickcheck]
    fn test_splinter_equality_quickcheck(values: Vec<u32>) -> TestResult {
        let mut a = mksplinter(&values);
        a.optimize();
        let b = mksplinter(&values);
        TestResult::from_bool(a == b)
    }

    #[quickcheck]
    fn test_splinter_equality_ref_quickcheck(values: Vec<u32>) -> TestResult {
        let mut a = mksplinter(&values);
        a.optimize();
        let b = mksplinter(&values).encode_to_splinter_ref();
        TestResult::from_bool(a == b)
    }

    #[quickcheck]
    fn test_splinter_equality_quickcheck_2(a: Vec<u32>, b: Vec<u32>) -> TestResult {
        let expected = itertools::equal(a.iter().sorted().dedup(), b.iter().sorted().dedup());

        let mut a = mksplinter(&a);
        a.optimize();
        let b = mksplinter(&b);

        TestResult::from_bool((a == b) == expected)
    }

    #[quickcheck]
    fn test_splinter_equality_ref_quickcheck_2(a: Vec<u32>, b: Vec<u32>) -> TestResult {
        let expected = itertools::equal(a.iter().sorted().dedup(), b.iter().sorted().dedup());

        let mut a = mksplinter(&a);
        a.optimize();
        let b = mksplinter(&b).encode_to_splinter_ref();

        TestResult::from_bool((a == b) == expected)
    }

    #[quickcheck]
    fn test_bitor_assign_quickcheck(
        optimize: bool,
        a: HashSet<u32>,
        b: HashSet<u32>,
    ) -> TestResult {
        let mut set: Splinter = a.iter().copied().collect();
        let other: Splinter = b.iter().copied().collect();

        if optimize {
            set.optimize();
        }

        let expected: Splinter = a.union(&b).copied().collect();
        set |= other;
        TestResult::from_bool(set == expected)
    }

    #[quickcheck]
    fn test_cut_quickcheck(optimize: bool, a: HashSet<u32>, b: HashSet<u32>) -> TestResult {
        let mut source: Splinter = a.iter().copied().collect();
        let other: Splinter = b.iter().copied().collect();

        if optimize {
            source.optimize();
        }

        let expected_intersection: Splinter = a.intersection(&b).copied().collect();
        let expected_remaining: Splinter = a.difference(&b).copied().collect();

        let actual_intersection = source.cut(&other);

        TestResult::from_bool(
            actual_intersection == expected_intersection && source == expected_remaining,
        )
    }

    #[quickcheck]
    fn test_bitor_ref_quickcheck(optimize: bool, a: HashSet<u32>, b: HashSet<u32>) -> TestResult {
        let mut set: Splinter = a.iter().copied().collect();
        let other_ref = Splinter::from_iter(b.clone()).encode_to_splinter_ref();

        if optimize {
            set.optimize();
        }

        let expected: Splinter = a.union(&b).copied().collect();
        set |= other_ref;
        TestResult::from_bool(set == expected)
    }

    #[quickcheck]
    fn test_cut_ref_quickcheck(optimize: bool, a: HashSet<u32>, b: HashSet<u32>) -> TestResult {
        let mut source: Splinter = a.iter().copied().collect();
        let other_ref = Splinter::from_iter(b.clone()).encode_to_splinter_ref();

        if optimize {
            source.optimize();
        }

        let expected_intersection: Splinter = a.intersection(&b).copied().collect();
        let expected_remaining: Splinter = a.difference(&b).copied().collect();

        let actual_intersection = source.cut(&other_ref);

        TestResult::from_bool(
            actual_intersection == expected_intersection && source == expected_remaining,
        )
    }
}
