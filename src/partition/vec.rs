use std::{fmt::Debug, mem::size_of};

use bytes::BufMut;
use itertools::Itertools;

use crate::{
    codec::{Encodable, encoder::Encoder},
    count::{count_runs_sorted, count_unique_sorted},
    level::Level,
    partition::{OptimizableInner, Partition},
    segment::SplitSegment,
    traits::{Cut, Merge, PartitionRead, PartitionWrite},
    util::find_next_sorted,
};

#[derive(Clone, Eq)]
pub struct VecPartition<L: Level> {
    values: Vec<L::Value>,
}

impl<L: Level> Default for VecPartition<L> {
    fn default() -> Self {
        VecPartition { values: Vec::new() }
    }
}

impl<L: Level> Debug for VecPartition<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VecPartition<{}>({})", L::DEBUG_NAME, self.cardinality())
    }
}

impl<L: Level> VecPartition<L> {
    pub const EMPTY: Self = VecPartition { values: vec![] };

    #[inline(always)]
    pub const fn encoded_size(cardinality: usize) -> usize {
        // values + length
        let vsize = size_of::<L::ValueUnaligned>();
        (cardinality * vsize) + vsize
    }

    /// Construct an `VecPartition` from a sorted iter of unique values
    /// SAFETY: undefined behavior if the iter is not sorted or contains duplicates
    pub fn from_sorted_unique_unchecked(values: impl Iterator<Item = L::Value>) -> Self {
        VecPartition { values: values.collect() }
    }

    #[inline]
    pub fn count_runs(&self) -> usize {
        count_runs_sorted(self.iter())
    }

    pub fn sparsity_ratio(&self) -> f64 {
        let unique_segments = count_unique_sorted(self.iter().map(|v| v.segment()));
        unique_segments as f64 / self.cardinality() as f64
    }
}

impl<L: Level> FromIterator<L::Value> for VecPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let values = iter.into_iter().sorted().dedup();
        // SAFETY: the iterator is sorted and deduped
        Self::from_sorted_unique_unchecked(values)
    }
}

impl<L: Level> Encodable for VecPartition<L> {
    #[inline]
    fn encoded_size(&self) -> usize {
        Self::encoded_size(self.values.len())
    }

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>) {
        encoder.put_vec_partition::<L>(&self.values);
    }
}

impl<L: Level> PartitionRead<L> for VecPartition<L> {
    #[inline]
    fn cardinality(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn contains(&self, value: L::Value) -> bool {
        self.values.binary_search(&value).is_ok()
    }

    fn rank(&self, value: L::Value) -> usize {
        match self.values.binary_search(&value) {
            Ok(index) => index + 1,
            Err(index) => index,
        }
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        self.values.get(idx).copied()
    }

    fn last(&self) -> Option<L::Value> {
        self.values.last().copied()
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.values.iter().copied()
    }
}

impl<L: Level> PartitionWrite<L> for VecPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        match self.values.binary_search(&value) {
            // value already exists
            Ok(_) => false,
            // value doesn't exist, insert it
            Err(index) => {
                self.values.insert(index, value);
                true
            }
        }
    }

    fn remove(&mut self, value: L::Value) -> bool {
        match self.values.binary_search(&value) {
            // value exists, remove it
            Ok(index) => {
                self.values.remove(index);
                true
            }
            // value doesn't exist
            Err(_) => false,
        }
    }
}

impl<L: Level> PartialEq for VecPartition<L> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl<L: Level> PartialEq<&[L::ValueUnaligned]> for VecPartition<L> {
    fn eq(&self, other: &&[L::ValueUnaligned]) -> bool {
        itertools::equal(self.iter(), other.iter().map(|&v| v.into()))
    }
}

impl<L: Level> Merge for VecPartition<L> {
    fn merge(&mut self, rhs: &Self) {
        self.values = self.iter().merge(rhs.iter()).dedup().collect_vec();
    }
}

impl<L: Level> Merge<&[L::ValueUnaligned]> for VecPartition<L> {
    fn merge(&mut self, rhs: &&[L::ValueUnaligned]) {
        self.values = self
            .iter()
            .merge(rhs.iter().map(|&v| v.into()))
            .dedup()
            .collect_vec();
    }
}

impl<L: Level, P: PartitionRead<L>> Cut<P> for VecPartition<L>
where
    Partition<L>: OptimizableInner,
{
    type Out = Partition<L>;

    fn cut(&mut self, rhs: &P) -> Self::Out {
        let mut other = rhs.iter().peekable();

        let mut intersection = Partition::default();
        for v in self
            .values
            .extract_if(.., |val| find_next_sorted(&mut other, val).is_some())
        {
            intersection.raw_insert(v);
        }
        intersection
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::{
        level::Block,
        partition::vec::VecPartition,
        testutil::{test_partition_read, test_partition_write},
    };

    #[quickcheck]
    fn test_vec_small_read_quickcheck(set: Vec<u8>) -> TestResult {
        let expected = set.iter().copied().sorted().dedup().collect_vec();
        let partition = VecPartition::<Block>::from_iter(set);
        test_partition_read(&partition, &expected);
        TestResult::passed()
    }

    #[quickcheck]
    fn test_vec_small_write_quickcheck(set: Vec<u8>) -> TestResult {
        let mut partition = VecPartition::<Block>::from_iter(set);
        test_partition_write(&mut partition);
        TestResult::passed()
    }
}
