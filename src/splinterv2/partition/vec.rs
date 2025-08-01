use std::fmt::Debug;

use itertools::Itertools;
use num::traits::AsPrimitive;

use crate::splinterv2::{
    count::count_unique_sorted,
    encode::Encodable,
    level::Level,
    partition::{Partition, SPARSE_THRESHOLD},
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
    /// Construct an `VecPartition` from a sorted vector of unique values
    /// SAFETY: undefined behavior if the vector is not sorted or contains duplicates
    #[inline]
    pub fn from_sorted_unique_unchecked(values: Vec<L::Value>) -> Self {
        VecPartition { values }
    }

    pub fn optimize(&self) -> Option<Partition<L>> {
        if self.cardinality() == L::MAX_LEN {
            Some(Partition::Full)
        } else if self.cardinality() > L::VEC_LIMIT {
            Some(Partition::Bitmap(self.iter().collect()))
        } else if self.cardinality() > L::TREE_MIN && L::PREFER_TREE {
            let unique_segments = count_unique_sorted(self.values.iter().map(|v| v.segment()));
            let sparsity_ratio = unique_segments as f64 / self.cardinality() as f64;

            if sparsity_ratio < SPARSE_THRESHOLD {
                Some(Partition::Tree(self.iter().collect()))
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<L: Level> FromIterator<L::Value> for VecPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let values = iter.into_iter().sorted().dedup().collect_vec();
        // SAFETY: we just sorted and deduped the iterator
        VecPartition::from_sorted_unique_unchecked(values)
    }
}

impl<L: Level> Encodable for VecPartition<L> {
    fn encoded_size(&self) -> usize {
        self.values.len() * (L::BITS / 8)
    }
}

impl<L: Level> PartitionRead<L> for VecPartition<L> {
    fn cardinality(&self) -> usize {
        self.values.len()
    }

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
