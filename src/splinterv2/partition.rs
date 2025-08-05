use std::fmt::{self, Debug};

use itertools::Itertools;
use num::traits::AsPrimitive;

use crate::splinterv2::{
    encode::Encodable,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionKind {
    Vec,
    Tree,
    Bitmap,
    Run,
    Full,
}

impl PartitionKind {
    /// Pick the best `PartitionKind` based on cardinality
    pub fn pick_cardinality<L: Level>(cardinality: usize) -> Self {
        if cardinality == L::MAX_LEN {
            return PartitionKind::Full;
        } else if cardinality == 0 {
            return if L::PREFER_TREE {
                PartitionKind::Tree
            } else {
                PartitionKind::Vec
            };
        }

        let vec_encoded_size = VecPartition::<L>::encoded_size(cardinality);
        let bitmap_encoded_size = BitmapPartition::<L>::ENCODED_SIZE;
        if vec_encoded_size < bitmap_encoded_size {
            PartitionKind::Vec
        } else {
            PartitionKind::Bitmap
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum Partition<L: Level> {
    Vec(VecPartition<L>),
    Tree(TreePartition<L>),
    Bitmap(BitmapPartition<L>),
    Run(RunPartition<L>),
    Full,
}

impl<L: Level> Partition<L> {
    pub fn kind(&self) -> PartitionKind {
        match self {
            Partition::Tree(_) => PartitionKind::Tree,
            Partition::Vec(_) => PartitionKind::Vec,
            Partition::Bitmap(_) => PartitionKind::Bitmap,
            Partition::Run(_) => PartitionKind::Run,
            Partition::Full => PartitionKind::Full,
        }
    }

    fn to_kind(&mut self, kind: PartitionKind) {
        if self.kind() == kind {
            return;
        }

        *self = match kind {
            PartitionKind::Vec => {
                Partition::Vec(VecPartition::from_sorted_unique_unchecked(self.iter()))
            }
            PartitionKind::Tree => Partition::Tree(self.iter().collect()),
            PartitionKind::Bitmap => Partition::Bitmap(self.iter().collect()),
            PartitionKind::Run => {
                Partition::Run(RunPartition::from_sorted_unique_unchecked(self.iter()))
            }
            PartitionKind::Full => {
                debug_assert_eq!(self.cardinality(), L::MAX_LEN, "Partition is not full");
                Partition::Full
            }
        }
    }

    fn sparsity_ratio(&self) -> f64 {
        match self {
            Partition::Vec(p) => p.sparsity_ratio(),
            Partition::Tree(p) => p.sparsity_ratio(),
            Partition::Bitmap(p) => p.sparsity_ratio(),
            Partition::Run(p) => p.sparsity_ratio(),
            Partition::Full => 1.0,
        }
    }

    fn count_runs(&self) -> usize {
        match self {
            Partition::Vec(p) => p.count_runs(),
            Partition::Tree(p) => p.count_runs(),
            Partition::Bitmap(p) => p.count_runs(),
            Partition::Run(p) => p.count_runs(),
            Partition::Full => 1,
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
}

impl<L: Level> Optimizable for Partition<L> {
    fn optimize(&mut self) {
        let this = &mut *self;
        let optimized = this.optimize_kind(false);
        if optimized != this.kind() {
            this.to_kind(optimized);
        } else if let Partition::Tree(tree) = this {
            tree.optimize_children();
        }
    }
}

impl<L: Level> Encodable for Partition<L> {
    fn encoded_size(&self) -> usize {
        match self {
            Partition::Tree(partition) => partition.encoded_size(),
            Partition::Vec(partition) => partition.encoded_size(),
            Partition::Bitmap(partition) => partition.encoded_size(),
            Partition::Run(partition) => partition.encoded_size(),
            Partition::Full => 1, // TODO: this is an estimate
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
            Partition::Tree(partition) => partition.fmt(f),
            Partition::Vec(partition) => partition.fmt(f),
            Partition::Bitmap(partition) => partition.fmt(f),
            Partition::Run(partition) => partition.fmt(f),
            Partition::Full => write!(f, "Full"),
        }
    }
}

impl<L: Level> PartitionRead<L> for Partition<L> {
    fn cardinality(&self) -> usize {
        match self {
            Partition::Tree(partition) => partition.cardinality(),
            Partition::Vec(partition) => partition.cardinality(),
            Partition::Bitmap(partition) => partition.cardinality(),
            Partition::Run(partition) => partition.cardinality(),
            Partition::Full => L::MAX_LEN,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Partition::Tree(partition) => partition.is_empty(),
            Partition::Vec(partition) => partition.is_empty(),
            Partition::Bitmap(partition) => partition.is_empty(),
            Partition::Run(partition) => partition.is_empty(),
            Partition::Full => false,
        }
    }

    fn contains(&self, value: L::Value) -> bool {
        debug_assert!(value.as_() < L::MAX_LEN, "value out of range");

        match self {
            Partition::Tree(partition) => partition.contains(value),
            Partition::Vec(partition) => partition.contains(value),
            Partition::Bitmap(partition) => partition.contains(value),
            Partition::Run(partition) => partition.contains(value),
            Partition::Full => true,
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        match self {
            Partition::Tree(p) => Iter::Tree(p.iter()),
            Partition::Vec(p) => Iter::Vec(p.iter()),
            Partition::Bitmap(p) => Iter::Bitmap(p.iter()),
            Partition::Run(p) => Iter::Run(p.iter()),
            Partition::Full => Iter::Full((0..L::MAX_LEN).map(L::Value::truncate_from)),
        }
    }
}

impl<L: Level> PartitionWrite<L> for Partition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let inserted = match self {
            Partition::Tree(partition) => partition.insert(value),
            Partition::Vec(partition) => partition.insert(value),
            Partition::Bitmap(partition) => partition.insert(value),
            Partition::Run(partition) => partition.insert(value),
            Partition::Full => false,
        };

        if inserted {
            self.to_kind(self.optimize_kind(true));
        }

        inserted
    }
}

impl<L: Level> FromIterator<L::Value> for Partition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let mut partition = Partition::Vec(iter.into_iter().collect());
        partition.to_kind(partition.optimize_kind(true));
        partition
    }
}

enum Iter<TI, VI, BI, RI, FI> {
    Tree(TI),
    Vec(VI),
    Bitmap(BI),
    Run(RI),
    Full(FI),
}

impl<
    T,
    TI: Iterator<Item = T>,
    VI: Iterator<Item = T>,
    BI: Iterator<Item = T>,
    RI: Iterator<Item = T>,
    FI: Iterator<Item = T>,
> Iterator for Iter<TI, VI, BI, RI, FI>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Tree(iter) => iter.next(),
            Iter::Vec(iter) => iter.next(),
            Iter::Bitmap(iter) => iter.next(),
            Iter::Run(iter) => iter.next(),
            Iter::Full(iter) => iter.next(),
        }
    }
}
