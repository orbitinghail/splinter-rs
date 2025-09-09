use std::{
    mem,
    ops::{BitOrAssign, Deref},
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

impl BitOrAssign<Splinter> for Splinter {
    fn bitor_assign(&mut self, mut rhs: Splinter) {
        // merge into the larger set
        if rhs.cardinality() < self.cardinality() {
            mem::swap(self, &mut rhs);
        }
        self.inner_mut().bitor_assign(rhs.inner())
    }
}

impl BitOrAssign<&Splinter> for Splinter {
    fn bitor_assign(&mut self, rhs: &Splinter) {
        self.inner_mut().bitor_assign(rhs.inner())
    }
}

impl<B: Deref<Target = [u8]>> BitOrAssign<SplinterRef<B>> for Splinter {
    fn bitor_assign(&mut self, rhs: SplinterRef<B>) {
        self.inner_mut().bitor_assign(&rhs.load_unchecked())
    }
}

impl<B: Deref<Target = [u8]>> BitOrAssign<&SplinterRef<B>> for Splinter {
    fn bitor_assign(&mut self, rhs: &SplinterRef<B>) {
        self.inner_mut().bitor_assign(&rhs.load_unchecked())
    }
}

impl<B: Deref<Target = [u8]>> BitOrAssign<CowSplinter<B>> for Splinter {
    fn bitor_assign(&mut self, rhs: CowSplinter<B>) {
        match rhs {
            CowSplinter::Ref(splinter_ref) => self.bitor_assign(splinter_ref),
            CowSplinter::Owned(splinter) => self.bitor_assign(splinter),
        }
    }
}

impl<B: Deref<Target = [u8]>> BitOrAssign<&CowSplinter<B>> for Splinter {
    fn bitor_assign(&mut self, rhs: &CowSplinter<B>) {
        match rhs {
            CowSplinter::Ref(splinter_ref) => self.bitor_assign(splinter_ref),
            CowSplinter::Owned(splinter) => self.bitor_assign(splinter),
        }
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
