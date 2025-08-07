use std::fmt::{self, Debug};

use bytes::BufMut;
use itertools::Itertools;
use num::traits::AsPrimitive;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionKind {
    Full,
    Bitmap,
    Vec,
    Run,
    Tree,
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

    fn to_kind(&mut self, kind: PartitionKind) {
        if self.kind() == kind {
            return;
        }

        *self = match kind {
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
            Partition::Full => 1, // TODO: this is an estimate
            Partition::Bitmap(partition) => partition.encoded_size(),
            Partition::Vec(partition) => partition.encoded_size(),
            Partition::Run(partition) => partition.encoded_size(),
            Partition::Tree(partition) => partition.encoded_size(),
        }
    }

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>) {
        match self {
            Partition::Full => encoder.put_full_container(),
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
        debug_assert!(value.as_() < L::MAX_LEN, "value out of range");

        match self {
            Partition::Full => true,
            Partition::Bitmap(partition) => partition.contains(value),
            Partition::Vec(partition) => partition.contains(value),
            Partition::Run(partition) => partition.contains(value),
            Partition::Tree(partition) => partition.contains(value),
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
        let inserted = match self {
            Partition::Full => false,
            Partition::Bitmap(partition) => partition.insert(value),
            Partition::Vec(partition) => partition.insert(value),
            Partition::Run(partition) => partition.insert(value),
            Partition::Tree(partition) => partition.insert(value),
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

enum Iter<FI, BI, VI, RI, TI> {
    Full(FI),
    Bitmap(BI),
    Vec(VI),
    Run(RI),
    Tree(TI),
}

impl<
    T,
    FI: Iterator<Item = T>,
    BI: Iterator<Item = T>,
    VI: Iterator<Item = T>,
    RI: Iterator<Item = T>,
    TI: Iterator<Item = T>,
> Iterator for Iter<FI, BI, VI, RI, TI>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Full(iter) => iter.next(),
            Iter::Bitmap(iter) => iter.next(),
            Iter::Vec(iter) => iter.next(),
            Iter::Run(iter) => iter.next(),
            Iter::Tree(iter) => iter.next(),
        }
    }
}
