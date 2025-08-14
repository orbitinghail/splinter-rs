use std::fmt::{self, Debug};

use bytes::BufMut;
use itertools::Itertools;
use num::traits::{AsPrimitive, Bounded};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::MultiIter;
use crate::splinterv2::{
    codec::{Encodable, encoder::Encoder},
    level::Level,
    partition::{
        bitmap::BitmapPartition, run::RunPartition, tree::TreePartition, vec::VecPartition,
    },
    traits::{Optimizable, PartitionRead, PartitionWrite, TruncateFrom},
};

pub mod bitmap;
pub mod run;
pub mod tree;
pub mod vec;

/// Tree sparsity ratio limit
const TREE_SPARSE_THRESHOLD: f64 = 0.5;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    IntoBytes,
    TryFromBytes,
    Unaligned,
    KnownLayout,
    Immutable,
    Default,
)]
#[repr(u8)]
pub enum PartitionKind {
    #[default]
    Empty = 0x00,
    Full = 0x01,
    Bitmap = 0x02,
    Vec = 0x03,
    Run = 0x04,
    Tree = 0x05,
}

impl PartitionKind {
    pub fn build<L: Level>(self) -> Partition<L> {
        match self {
            PartitionKind::Empty => Partition::default(),
            PartitionKind::Full => Partition::Full,
            PartitionKind::Bitmap => Partition::Bitmap(Default::default()),
            PartitionKind::Vec => Partition::Vec(Default::default()),
            PartitionKind::Run => Partition::Run(Default::default()),
            PartitionKind::Tree => Partition::Tree(Default::default()),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum Partition<L: Level> {
    Full,
    Bitmap(BitmapPartition<L>),
    Vec(VecPartition<L>),
    Run(RunPartition<L>),
    Tree(TreePartition<L>),
}

impl<L: Level> Partition<L> {
    pub fn kind(&self) -> PartitionKind {
        match self {
            Partition::Full => PartitionKind::Full,
            Partition::Bitmap(_) => PartitionKind::Bitmap,
            Partition::Vec(_) => PartitionKind::Vec,
            Partition::Run(_) => PartitionKind::Run,
            Partition::Tree(_) => PartitionKind::Tree,
        }
    }

    fn switch_kind(&mut self, kind: PartitionKind) {
        if self.kind() == kind {
            return;
        }

        *self = match kind {
            PartitionKind::Empty => {
                debug_assert_eq!(self.cardinality(), 0, "Partition is not empty");
                Partition::default()
            }
            PartitionKind::Full => {
                debug_assert_eq!(self.cardinality(), L::MAX_LEN, "Partition is not full");
                Partition::Full
            }
            PartitionKind::Bitmap => Partition::Bitmap(self.iter().collect()),
            PartitionKind::Vec => {
                Partition::Vec(VecPartition::from_sorted_unique_unchecked(self.iter()))
            }
            PartitionKind::Run => {
                Partition::Run(RunPartition::from_sorted_unique_unchecked(self.iter()))
            }
            PartitionKind::Tree => Partition::Tree(self.iter().collect()),
        }
    }

    fn sparsity_ratio(&self) -> f64 {
        match self {
            Partition::Full => 1.0,
            Partition::Bitmap(p) => p.sparsity_ratio(),
            Partition::Vec(p) => p.sparsity_ratio(),
            Partition::Run(p) => p.sparsity_ratio(),
            Partition::Tree(p) => p.sparsity_ratio(),
        }
    }

    fn count_runs(&self) -> usize {
        match self {
            Partition::Full => 1,
            Partition::Bitmap(p) => p.count_runs(),
            Partition::Vec(p) => p.count_runs(),
            Partition::Run(p) => p.count_runs(),
            Partition::Tree(p) => p.count_runs(),
        }
    }

    fn optimize_kind(&self, skip_run: bool) -> PartitionKind {
        let kind = self.kind();
        let cardinality = self.cardinality();

        if cardinality == L::MAX_LEN {
            return PartitionKind::Full;
        }

        if cardinality == 0 {
            if L::PREFER_TREE {
                return PartitionKind::Tree;
            } else {
                return PartitionKind::Vec;
            }
        }

        if L::PREFER_TREE && cardinality > L::TREE_MIN {
            let sparsity = self.sparsity_ratio();
            if kind == PartitionKind::Tree {
                if sparsity < TREE_SPARSE_THRESHOLD {
                    // If we are currently a tree, and this level prefers to be
                    // a tree, then we stay a tree unless we pass the sparsity threshold
                    return kind;
                }
                // too sparse, fall through to selecting partition kind by size
            } else if sparsity < TREE_SPARSE_THRESHOLD {
                return PartitionKind::Tree;
            }
        }

        let choices = [
            (
                PartitionKind::Vec,
                VecPartition::<L>::encoded_size(cardinality),
            ),
            (PartitionKind::Bitmap, BitmapPartition::<L>::ENCODED_SIZE),
            (
                PartitionKind::Run,
                if skip_run {
                    usize::MAX
                } else {
                    RunPartition::<L>::encoded_size(self.count_runs())
                },
            ),
        ];

        if let Some(idx) = choices.iter().position_min_by_key(|(_, s)| *s) {
            return choices[idx].0;
        }

        self.kind()
    }

    /// Insert a value into the partition without optimizing the partition's
    /// storage choice. You should run `Self::optimize` on this partition
    /// afterwards.
    /// Returns `true` if the insertion occurred, otherwise `false`.
    pub fn raw_insert(&mut self, value: L::Value) -> bool {
        match self {
            Partition::Full => false,
            Partition::Bitmap(partition) => partition.insert(value),
            Partition::Vec(partition) => partition.insert(value),
            Partition::Run(partition) => partition.insert(value),
            Partition::Tree(partition) => partition.insert(value),
        }
    }
}

impl<L: Level> Optimizable for Partition<L> {
    fn optimize(&mut self) {
        let this = &mut *self;
        let optimized = this.optimize_kind(false);
        if optimized != this.kind() {
            this.switch_kind(optimized);
        } else if let Partition::Tree(tree) = this {
            tree.optimize_children();
        }
    }
}

impl<L: Level> Encodable for Partition<L> {
    fn encoded_size(&self) -> usize {
        let inner_size = match self {
            Partition::Full => 0,
            Partition::Bitmap(partition) => partition.encoded_size(),
            Partition::Vec(partition) => partition.encoded_size(),
            Partition::Run(partition) => partition.encoded_size(),
            Partition::Tree(partition) => partition.encoded_size(),
        };
        // +1 for PartitionKind
        inner_size + 1
    }

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>) {
        match self {
            Partition::Full => encoder.put_full_partition(),
            Partition::Bitmap(partition) => partition.encode(encoder),
            Partition::Vec(partition) => partition.encode(encoder),
            Partition::Run(partition) => partition.encode(encoder),
            Partition::Tree(partition) => partition.encode(encoder),
        }
    }
}

impl<L: Level> Default for Partition<L> {
    fn default() -> Self {
        Partition::Vec(VecPartition::default())
    }
}

impl<L: Level> Debug for Partition<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Partition::Full => write!(f, "Full"),
            Partition::Bitmap(partition) => partition.fmt(f),
            Partition::Vec(partition) => partition.fmt(f),
            Partition::Run(partition) => partition.fmt(f),
            Partition::Tree(partition) => partition.fmt(f),
        }
    }
}

impl<L: Level> PartitionRead<L> for Partition<L> {
    fn cardinality(&self) -> usize {
        match self {
            Partition::Full => L::MAX_LEN,
            Partition::Bitmap(partition) => partition.cardinality(),
            Partition::Vec(partition) => partition.cardinality(),
            Partition::Run(partition) => partition.cardinality(),
            Partition::Tree(partition) => partition.cardinality(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Partition::Full => false,
            Partition::Bitmap(partition) => partition.is_empty(),
            Partition::Vec(partition) => partition.is_empty(),
            Partition::Run(partition) => partition.is_empty(),
            Partition::Tree(partition) => partition.is_empty(),
        }
    }

    fn contains(&self, value: L::Value) -> bool {
        match self {
            Partition::Full => true,
            Partition::Bitmap(partition) => partition.contains(value),
            Partition::Vec(partition) => partition.contains(value),
            Partition::Run(partition) => partition.contains(value),
            Partition::Tree(partition) => partition.contains(value),
        }
    }

    fn rank(&self, value: L::Value) -> usize {
        match self {
            Partition::Full => value.as_() + 1,
            Partition::Bitmap(p) => p.rank(value),
            Partition::Vec(p) => p.rank(value),
            Partition::Run(p) => p.rank(value),
            Partition::Tree(p) => p.rank(value),
        }
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        match self {
            Partition::Full => Some(L::Value::truncate_from(idx)),
            Partition::Bitmap(p) => p.select(idx),
            Partition::Vec(p) => p.select(idx),
            Partition::Run(p) => p.select(idx),
            Partition::Tree(p) => p.select(idx),
        }
    }

    fn last(&self) -> Option<L::Value> {
        match self {
            Partition::Full => Some(L::Value::max_value()),
            Partition::Bitmap(p) => p.last(),
            Partition::Vec(p) => p.last(),
            Partition::Run(p) => p.last(),
            Partition::Tree(p) => p.last(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        match self {
            Partition::Full => Iter::Full((0..L::MAX_LEN).map(L::Value::truncate_from)),
            Partition::Bitmap(p) => Iter::Bitmap(p.iter()),
            Partition::Vec(p) => Iter::Vec(p.iter()),
            Partition::Run(p) => Iter::Run(p.iter()),
            Partition::Tree(p) => Iter::Tree(p.iter()),
        }
    }
}

impl<L: Level> PartitionWrite<L> for Partition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let inserted = self.raw_insert(value);
        if inserted {
            self.switch_kind(self.optimize_kind(true));
        }
        inserted
    }
}

impl<L: Level> FromIterator<L::Value> for Partition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let mut partition = Partition::Vec(iter.into_iter().collect());
        partition.switch_kind(partition.optimize_kind(true));
        partition
    }
}

MultiIter!(Iter, Full, Bitmap, Vec, Run, Tree);

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::{
        splinterv2::{
            Partition, PartitionRead, SplinterV2,
            level::{High, Level, Low},
        },
        testutil::{SetGenV2, mkpartition, test_partition_read},
    };

    use super::PartitionKind;

    #[test]
    fn test_partition_full() {
        let splinter = Partition::<High>::Full;
        assert!(!splinter.is_empty(), "not empty");
        assert_eq!(
            splinter.cardinality(),
            <High as Level>::MAX_LEN,
            "cardinality"
        );
        assert_eq!(
            splinter.last(),
            Some(<High as Level>::Value::max_value()),
            "last"
        );
    }

    #[test]
    fn test_partitions_direct() {
        let mut setgen = SetGenV2::<Low>::new(0xDEADBEEF);

        let kinds = [
            PartitionKind::Bitmap,
            PartitionKind::Vec,
            PartitionKind::Run,
            PartitionKind::Tree,
        ];
        let sets = &[
            vec![],
            setgen.random(8),
            setgen.random(4096),
            setgen.runs(4096, 0.01),
            setgen.runs(4096, 0.2),
            setgen.runs(4096, 0.5),
            setgen.runs(4096, 0.9),
        ];

        for kind in kinds {
            for (i, set) in sets.iter().enumerate() {
                println!("Testing partition kind: {kind:?} with set {i}");

                let partition = mkpartition::<Low>(kind, &set);
                test_partition_read(&partition, &set);
            }
        }
    }

    #[quickcheck]
    fn test_partitions_quickcheck(values: Vec<u32>) -> TestResult {
        let expected = values.iter().copied().sorted().dedup().collect_vec();
        test_partition_read(&SplinterV2::from_iter(values), &expected);
        TestResult::passed()
    }
}
