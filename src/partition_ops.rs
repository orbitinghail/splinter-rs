use crate::{
    PartitionRead, PartitionWrite,
    codec::partition_ref::{NonRecursivePartitionRef, PartitionRef},
    level::Level,
    partition::{OptimizableInner, Partition},
    traits::{Cut, Merge},
};

impl<L: Level> PartialEq for Partition<L>
where
    Self: OptimizableInner,
{
    fn eq(&self, other: &Partition<L>) -> bool {
        use Partition::*;

        match (self, other) {
            // use fast physical ops if both partitions share storage
            (Full, Full) => true,
            (Bitmap(a), Bitmap(b)) => a == b,
            (Vec(a), Vec(b)) => a == b,
            (Run(a), Run(b)) => a == b,
            (Tree(a), Tree(b)) => a == b,

            // otherwise fall back to logical ops
            (a, b) => {
                debug_assert_ne!(a.kind(), b.kind(), "should have different storage classes");
                itertools::equal(a.iter(), b.iter())
            }
        }
    }
}

impl<L: Level> PartialEq<PartitionRef<'_, L>> for Partition<L>
where
    Self: OptimizableInner,
{
    fn eq(&self, other: &PartitionRef<'_, L>) -> bool {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        match (self, other) {
            // use fast physical ops if both partitions share storage
            (Partition::Full, NonRecursive(Full)) => true,
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a == bitmap,
            (Partition::Vec(a), NonRecursive(Vec { values })) => a == values,
            (Partition::Run(a), NonRecursive(Run { runs })) => a == runs,
            (Partition::Tree(a), Tree(b)) => a == b,

            // otherwise fall back to logical ops
            (a, b) => itertools::equal(a.iter(), b.iter()),
        }
    }
}

impl<L: Level> Merge for Partition<L>
where
    Self: OptimizableInner,
{
    fn merge(&mut self, rhs: &Self) {
        use Partition::*;

        match (&mut *self, rhs) {
            // special case full
            (Full, _) => (),
            (a, Full) => *a = Full,

            // use fast physical ops if both partitions share storage
            (Bitmap(a), Bitmap(b)) => a.merge(b),
            (Vec(a), Vec(b)) => a.merge(b),
            (Run(a), Run(b)) => a.merge(b),
            (Tree(a), Tree(b)) => a.merge(b),

            // otherwise fall back to logical ops
            (a, b) => {
                for el in b.iter() {
                    a.raw_insert(el);
                }
            }
        }

        self.optimize_fast();
    }
}

impl<L: Level> Merge<PartitionRef<'_, L>> for Partition<L>
where
    Partition<L>: OptimizableInner,
{
    fn merge(&mut self, rhs: &PartitionRef<'_, L>) {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        match (&mut *self, rhs) {
            // special cases for full and empty
            (Partition::Full, _) => (),
            (_, NonRecursive(Empty)) => (),
            (a, NonRecursive(Full)) => *a = Partition::Full,

            // use fast physical ops if both partitions share storage
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a.merge(bitmap),
            (Partition::Vec(a), NonRecursive(Vec { values })) => a.merge(values),
            (Partition::Run(a), NonRecursive(Run { runs })) => a.merge(runs),
            (Partition::Tree(a), Tree(tree)) => a.merge(tree),

            // otherwise fall back to logical ops
            (a, b) => {
                for el in b.iter() {
                    a.raw_insert(el);
                }
            }
        }

        self.optimize_fast();
    }
}

impl<L: Level> Cut for Partition<L>
where
    Partition<L>: OptimizableInner,
{
    type Out = Self;

    fn cut(&mut self, rhs: &Self) -> Self::Out {
        use Partition::*;

        let mut intersection = match (&mut *self, rhs) {
            // use fast physical ops if both partitions share storage
            (a @ Full, Full) => std::mem::take(a),
            (Bitmap(a), Bitmap(b)) => a.cut(b),
            (Run(a), Run(b)) => a.cut(b),
            (Tree(a), Tree(b)) => a.cut(b),

            // fallback to general optimized logical ops
            (Vec(a), b) => a.cut(b),

            // otherwise fall back to logical ops
            (a, b) => {
                let mut intersection = Partition::default();
                for val in b.iter() {
                    if a.remove(val) {
                        intersection.raw_insert(val);
                    }
                }
                intersection
            }
        };

        self.optimize_fast();
        intersection.optimize_fast();
        intersection
    }
}

impl<L: Level> Cut<PartitionRef<'_, L>> for Partition<L>
where
    Partition<L>: OptimizableInner,
{
    type Out = Self;

    fn cut(&mut self, rhs: &PartitionRef<'_, L>) -> Self::Out {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        let mut intersection = match (&mut *self, rhs) {
            // special case empty
            (_, NonRecursive(Empty)) => Partition::default(),

            // use fast physical ops if both partitions share storage
            (a @ Partition::Full, NonRecursive(Full)) => std::mem::take(a),
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a.cut(bitmap),
            (Partition::Run(a), NonRecursive(Run { runs })) => a.cut(runs),
            (Partition::Tree(a), Tree(b)) => a.cut(b),

            // fallback to general optimized logical ops
            (Partition::Vec(a), b) => a.cut(b),

            // otherwise fall back to logical ops
            (a, b) => {
                let mut intersection = Partition::default();
                for val in b.iter() {
                    if a.remove(val) {
                        intersection.raw_insert(val);
                    }
                }
                intersection
            }
        };

        self.optimize_fast();
        intersection.optimize_fast();
        intersection
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use itertools::Itertools;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::{
        Optimizable, Splinter,
        testutil::mksplinter,
        traits::{Cut, Merge},
    };

    #[quickcheck]
    fn test_partitions_equality_quickcheck(values: Vec<u32>) -> TestResult {
        let mut a = mksplinter(&values);
        a.optimize();
        let b = mksplinter(&values);
        TestResult::from_bool(a == b)
    }

    #[quickcheck]
    fn test_partitions_equality_ref_quickcheck(values: Vec<u32>) -> TestResult {
        let mut a = mksplinter(&values);
        a.optimize();
        let b = mksplinter(&values).encode_to_splinter_ref();
        TestResult::from_bool(a == b)
    }

    #[quickcheck]
    fn test_partitions_equality_quickcheck_2(a: Vec<u32>, b: Vec<u32>) -> TestResult {
        let expected = itertools::equal(a.iter().sorted().dedup(), b.iter().sorted().dedup());

        let mut a = mksplinter(&a);
        a.optimize();
        let b = mksplinter(&b);

        TestResult::from_bool((a == b) == expected)
    }

    #[quickcheck]
    fn test_partitions_equality_ref_quickcheck_2(a: Vec<u32>, b: Vec<u32>) -> TestResult {
        let expected = itertools::equal(a.iter().sorted().dedup(), b.iter().sorted().dedup());

        let mut a = mksplinter(&a);
        a.optimize();
        let b = mksplinter(&b).encode_to_splinter_ref();

        TestResult::from_bool((a == b) == expected)
    }

    #[quickcheck]
    fn test_merge_quickcheck(optimize: bool, a: HashSet<u32>, b: HashSet<u32>) -> TestResult {
        let mut merged: Splinter = a.iter().copied().collect();
        let other: Splinter = b.iter().copied().collect();

        if optimize {
            merged.optimize();
        }

        let expected: Splinter = a.union(&b).copied().collect();
        merged.merge(&other);
        TestResult::from_bool(merged == expected)
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
    fn test_merge_ref_quickcheck(optimize: bool, a: HashSet<u32>, b: HashSet<u32>) -> TestResult {
        let mut merged: Splinter = a.iter().copied().collect();
        let other_ref = Splinter::from_iter(b.clone()).encode_to_splinter_ref();

        if optimize {
            merged.optimize();
        }

        let expected: Splinter = a.union(&b).copied().collect();
        merged.merge(&other_ref);
        TestResult::from_bool(merged == expected)
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
