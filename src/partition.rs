use std::{
    fmt::{self, Debug},
    ops::{RangeBounds, RangeInclusive},
};

use bytes::BufMut;
use itertools::Itertools;
use num::traits::{AsPrimitive, Bounded};

use crate::{
    MultiIter,
    codec::{Encodable, encoder::Encoder},
    count::count_unique_sorted,
    level::Level,
    partition::{
        bitmap::BitmapPartition, run::RunPartition, tree::TreePartition, vec::VecPartition,
    },
    partition_kind::PartitionKind,
    segment::SplitSegment,
    traits::{DefaultFull, Optimizable, PartitionRead, PartitionWrite, TruncateFrom},
    util::IteratorExt,
};

pub mod bitmap;
pub mod run;
pub mod tree;
pub mod vec;

#[derive(Clone, Eq)]
pub enum Partition<L: Level> {
    Full,
    Bitmap(BitmapPartition<L>),
    Vec(VecPartition<L>),
    Run(RunPartition<L>),
    Tree(TreePartition<L>),
}

impl<L: Level> Partition<L> {
    pub const EMPTY: Self = Self::Vec(VecPartition::EMPTY);

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

        assert!(
            L::ALLOW_TREE || kind != PartitionKind::Tree,
            "BUG: Tree partitioning is not allowed at this level"
        );

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
            PartitionKind::Run => Partition::Run(match &self {
                // optimize full partition conversion as we convert into run
                // partitions when removing values from a full splinter
                Partition::Full => RunPartition::full(),
                other => RunPartition::from_sorted_unique_unchecked(other.iter()),
            }),
            PartitionKind::Tree => Partition::Tree(match &self {
                Partition::Full | Partition::Tree(_) => {
                    // Full should never be optimized to Tree due cardinality check
                    // Tree should never be optimized into Tree due to early exit
                    unreachable!("BUG: invalid tree conversion")
                }
                Partition::Bitmap(partition) => partition.into(),
                Partition::Vec(partition) => partition.into(),
                Partition::Run(partition) => partition.into(),
            }),
        }
    }

    fn segments(&self) -> usize {
        match self {
            Partition::Full => 256,
            Partition::Vec(p) => count_unique_sorted(p.iter().map(|v| v.segment())),
            Partition::Bitmap(p) => p.segments(),
            Partition::Run(p) => p.segments(),
            Partition::Tree(p) => p.segments(),
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

    #[inline]
    pub(crate) fn optimize_fast(&mut self) {
        self.switch_kind(self.optimize_kind(true));
    }

    fn optimize_kind(&self, fast: bool) -> PartitionKind {
        let cardinality = self.cardinality();

        if cardinality == L::MAX_LEN {
            return PartitionKind::Full;
        }

        if cardinality == 0 {
            if L::ALLOW_TREE {
                return PartitionKind::Tree;
            } else {
                return PartitionKind::Vec;
            }
        }

        let choices = [
            (PartitionKind::Tree, {
                if !fast && let Partition::Tree(tree) = self {
                    // if we are already a tree, then we should only stay a tree
                    // if we are the smallest option
                    tree.encoded_size() + 1
                } else if L::ALLOW_TREE {
                    // switch to tree if this level prefers it and the
                    // estimated size is the smallest option
                    TreePartition::<L>::estimate_encoded_size(self.segments(), cardinality) + 1
                } else {
                    // otherwise we don't want to be a tree
                    usize::MAX
                }
            }),
            (
                PartitionKind::Vec,
                VecPartition::<L>::encoded_size(cardinality) + 1,
            ),
            (
                PartitionKind::Bitmap,
                BitmapPartition::<L>::ENCODED_SIZE + 1,
            ),
            (
                PartitionKind::Run,
                if let Partition::Run(run) = self {
                    // if we are already a run partition, make sure we stay there
                    // until a more optimal choice presents itself
                    run.encoded_size() + 1
                } else if fast {
                    // if we are optimizing fast, avoid switching to run
                    // partitions as counting runs can be slow
                    usize::MAX
                } else {
                    RunPartition::<L>::encoded_size(self.count_runs()) + 1
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
    /// Returns `true` if the insertion occurred, `false` otherwise.
    pub fn raw_insert(&mut self, value: L::Value) -> bool {
        match self {
            Partition::Full => false,
            Partition::Bitmap(partition) => partition.insert(value),
            Partition::Vec(partition) => partition.insert(value),
            Partition::Run(partition) => partition.insert(value),
            Partition::Tree(partition) => partition.insert(value),
        }
    }

    /// Remove a value from the partition without optimizing the partition's
    /// storage choice. You should run `Self::optimize` on this partition afterwards.
    /// Returns `true` if the removal occurred, `false` otherwise.
    pub fn raw_remove(&mut self, value: L::Value) -> bool {
        match self {
            Partition::Full => {
                self.switch_kind(PartitionKind::Run);
                self.raw_remove(value)
            }
            Partition::Bitmap(partition) => partition.remove(value),
            Partition::Vec(partition) => partition.remove(value),
            Partition::Run(partition) => partition.remove(value),
            Partition::Tree(partition) => partition.remove(value),
        }
    }
}

impl<L: Level> DefaultFull for Partition<L> {
    #[inline]
    fn full() -> Self {
        Self::Full
    }
}

impl<L: Level> Optimizable for Partition<L> {
    fn optimize(&mut self) {
        self.switch_kind(self.optimize_kind(false));
        if let Partition::Tree(tree) = self {
            tree.optimize_children();
        }
    }
}

impl<L: Level> Encodable for Partition<L> {
    fn encoded_size(&self) -> usize {
        if self.is_empty() {
            // PartitionKind::Empty
            1
        } else {
            let inner_size = match self {
                Partition::Full => 0,
                Partition::Bitmap(partition) => partition.encoded_size(),
                Partition::Vec(partition) => partition.encoded_size(),
                Partition::Run(partition) => partition.encoded_size(),
                Partition::Tree(partition) => partition.encoded_size(),
            };
            // inner + PartitionKind
            inner_size + 1
        }
    }

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>) {
        if self.is_empty() {
            encoder.put_kind(PartitionKind::Empty);
        } else {
            match self {
                Partition::Full => {
                    encoder.put_kind(PartitionKind::Full);
                }
                Partition::Bitmap(partition) => {
                    partition.encode(encoder);
                    encoder.put_kind(PartitionKind::Bitmap);
                }
                Partition::Vec(partition) => {
                    partition.encode(encoder);
                    encoder.put_kind(PartitionKind::Vec);
                }
                Partition::Run(partition) => {
                    partition.encode(encoder);
                    encoder.put_kind(PartitionKind::Run);
                }
                Partition::Tree(partition) => {
                    partition.encode(encoder);
                    encoder.put_kind(PartitionKind::Tree);
                }
            }
        }
    }
}

impl<L: Level> Default for Partition<L> {
    fn default() -> Self {
        Partition::EMPTY
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

    fn position(&self, value: L::Value) -> Option<usize> {
        match self {
            Partition::Full => Some(value.as_()),
            Partition::Bitmap(partition) => partition.position(value),
            Partition::Vec(partition) => partition.position(value),
            Partition::Run(partition) => partition.position(value),
            Partition::Tree(partition) => partition.position(value),
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
            Partition::Full => (idx < L::MAX_LEN).then(|| L::Value::truncate_from(idx)),
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
            Partition::Full => Iter::Full(
                (0..L::MAX_LEN)
                    .map(L::Value::truncate_from)
                    .with_size_hint(L::MAX_LEN),
            ),
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
            self.optimize_fast();
        }
        inserted
    }

    fn remove(&mut self, value: L::Value) -> bool {
        let removed = self.raw_remove(value);
        if removed {
            self.optimize_fast();
        }
        removed
    }

    fn remove_range<R: RangeBounds<L::Value>>(&mut self, values: R) {
        match self {
            Partition::Full => {
                self.switch_kind(PartitionKind::Run);
                self.remove_range(values)
            }
            Partition::Bitmap(partition) => partition.remove_range(values),
            Partition::Vec(partition) => partition.remove_range(values),
            Partition::Run(partition) => partition.remove_range(values),
            Partition::Tree(partition) => partition.remove_range(values),
        }
        self.optimize_fast();
    }
}

impl<L: Level> FromIterator<L::Value> for Partition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let mut partition = Partition::Vec(iter.into_iter().collect());
        partition.optimize_fast();
        partition
    }
}

impl<L: Level> From<RangeInclusive<L::Value>> for Partition<L> {
    fn from(value: RangeInclusive<L::Value>) -> Self {
        Partition::Run(value.into())
    }
}

impl<L: Level> Extend<L::Value> for Partition<L> {
    fn extend<T: IntoIterator<Item = L::Value>>(&mut self, iter: T) {
        match self {
            Partition::Full => (),
            Partition::Bitmap(partition) => partition.extend(iter),
            Partition::Vec(partition) => partition.extend(iter),
            Partition::Run(partition) => partition.extend(iter),
            Partition::Tree(partition) => partition.extend(iter),
        }
        self.optimize_fast();
    }
}

MultiIter!(Iter, Full, Bitmap, Vec, Run, Tree);

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{
        PartitionRead,
        level::{Block, High, Level, Low},
        partition::Partition,
        partition_kind::PartitionKind,
        testutil::{LevelSetGen, mkpartition, test_partition_read, test_partition_write},
        traits::TruncateFrom,
    };

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

        let block = Partition::<Block>::Full;
        test_partition_read(&block, &(0..=255).collect_vec());
    }

    #[test]
    fn test_partitions_direct() {
        let mut setgen = LevelSetGen::<Low>::new(0xDEADBEEF);

        let kinds = [
            PartitionKind::Bitmap,
            PartitionKind::Vec,
            PartitionKind::Run,
            PartitionKind::Tree,
        ];
        let sets = &[
            vec![],
            vec![0],
            vec![0, 1],
            vec![0, u16::MAX],
            vec![u16::MAX],
            setgen.random(8),
            setgen.random(4096),
            setgen.runs(4096, 0.01),
            setgen.runs(4096, 0.2),
            setgen.runs(4096, 0.5),
            setgen.runs(4096, 0.9),
            (0..Block::MAX_LEN)
                .map(|v| <Low as Level>::Value::truncate_from(v))
                .collect_vec(),
        ];

        for kind in kinds {
            for (i, set) in sets.iter().enumerate() {
                println!("Testing partition kind: {kind:?} with set {i}");

                if kind == PartitionKind::Tree && i == 5 {
                    println!("break")
                }

                let mut partition = mkpartition::<Low>(kind, &set);
                test_partition_read(&partition, &set);
                test_partition_write(&mut partition);
            }
        }
    }
}
