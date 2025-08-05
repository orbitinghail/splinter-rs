use std::fmt::Debug;

use itertools::Itertools;
use num::traits::AsPrimitive;

use crate::splinterv2::{
    count::{count_runs_sorted, count_unique_sorted},
    encode::Encodable,
    level::Level,
    segment::SplitSegment,
    traits::{PartitionRead, PartitionWrite},
};

#[derive(Clone, PartialEq, Eq)]
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
    #[inline]
    pub const fn encoded_size(cardinality: usize) -> usize {
        cardinality * (L::BITS / 8)
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

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.values.iter().copied()
    }
}

impl<L: Level> PartitionWrite<L> for VecPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        assert!(value.as_() < L::MAX_LEN, "value out of range");
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
}
