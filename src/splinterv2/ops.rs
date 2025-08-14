use crate::splinterv2::{
    PartitionRead,
    codec::partition_ref::{NonRecursivePartitionRef, PartitionRef},
    level::Level,
    partition::Partition,
};

impl<L: Level> PartialEq for Partition<L> {
    fn eq(&self, other: &Partition<L>) -> bool {
        use Partition::*;

        match (self, other) {
            // use fast physical comparisons if both partitions use the same
            // storage
            (Full, Full) => true,
            (Bitmap(a), Bitmap(b)) => a == b,
            (Vec(a), Vec(b)) => a == b,
            (Run(a), Run(b)) => a == b,
            (Tree(a), Tree(b)) => a == b,

            // fall back to logical equality if the two partitions have
            // different storage classes
            (a, b) => {
                debug_assert_ne!(a.kind(), b.kind(), "should have different storage classes");
                itertools::equal(a.iter(), b.iter())
            }
        }
    }
}

impl<L: Level> PartialEq<PartitionRef<'_, L>> for Partition<L> {
    fn eq(&self, other: &PartitionRef<'_, L>) -> bool {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        match (self, other) {
            // use fast physical comparisons if both partitions use the same
            // storage
            (Partition::Full, NonRecursive(Full)) => true,
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a == *bitmap,
            (Partition::Vec(a), NonRecursive(Vec { values })) => a == *values,
            (Partition::Run(a), NonRecursive(Run { runs })) => a == *runs,
            (Partition::Tree(a), Tree(b)) => *a == *b,

            // fall back to logical equality if the two partitions have
            // different storage classes
            (a, b) => itertools::equal(a.iter(), b.iter()),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::{splinterv2::Optimizable, testutil::mksplinterv2};

    #[quickcheck]
    fn test_partitions_equality_quickcheck(values: Vec<u32>) -> TestResult {
        let mut a = mksplinterv2(&values);
        a.optimize();
        let b = mksplinterv2(&values);
        TestResult::from_bool(a == b)
    }

    #[quickcheck]
    fn test_partitions_equality_ref_quickcheck(values: Vec<u32>) -> TestResult {
        let mut a = mksplinterv2(&values);
        a.optimize();
        let b = mksplinterv2(&values).encode_to_splinter_ref();
        TestResult::from_bool(a == b)
    }

    #[quickcheck]
    fn test_partitions_equality_quickcheck_2(a: Vec<u32>, b: Vec<u32>) -> TestResult {
        let expected = itertools::equal(a.iter().sorted().dedup(), b.iter().sorted().dedup());

        let mut a = mksplinterv2(&a);
        a.optimize();
        let b = mksplinterv2(&b);

        TestResult::from_bool((a == b) == expected)
    }

    #[quickcheck]
    fn test_partitions_equality_ref_quickcheck_2(a: Vec<u32>, b: Vec<u32>) -> TestResult {
        let expected = itertools::equal(a.iter().sorted().dedup(), b.iter().sorted().dedup());

        let mut a = mksplinterv2(&a);
        a.optimize();
        let b = mksplinterv2(&b).encode_to_splinter_ref();

        TestResult::from_bool((a == b) == expected)
    }
}
