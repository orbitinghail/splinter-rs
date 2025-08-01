use std::fmt::{self, Debug};

use num::traits::AsPrimitive;

use crate::splinterv2::{
    level::Level,
    partition::{bitmap::BitmapPartition, tree::TreePartition, vec::VecPartition},
    traits::{PartitionRead, PartitionWrite, TruncateFrom},
};

pub mod bitmap;
pub mod tree;
pub mod vec;

pub enum Partition<L: Level> {
    Vec(VecPartition<L>),
    Tree(TreePartition<L>),
    Bitmap(BitmapPartition<L>),
    Full,
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
            Partition::Full => L::MAX_LEN,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Partition::Tree(partition) => partition.is_empty(),
            Partition::Vec(partition) => partition.is_empty(),
            Partition::Bitmap(partition) => partition.is_empty(),
            Partition::Full => false,
        }
    }

    fn contains(&self, value: L::Value) -> bool {
        debug_assert!(value.as_() < L::MAX_LEN, "value out of range");

        match self {
            Partition::Tree(partition) => partition.contains(value),
            Partition::Vec(partition) => partition.contains(value),
            Partition::Bitmap(partition) => partition.contains(value),
            Partition::Full => true,
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        use itertools::Either::*;
        match self {
            Partition::Tree(partition) => Left(Left(partition.iter())),
            Partition::Vec(partition) => Left(Right(partition.iter())),
            Partition::Bitmap(partition) => Right(Left(partition.iter())),
            Partition::Full => Right(Right((0..L::MAX_LEN).map(L::Value::truncate_from))),
        }
    }

    fn serialized_size(&self) -> usize {
        match self {
            Partition::Tree(partition) => partition.serialized_size(),
            Partition::Vec(partition) => partition.serialized_size(),
            Partition::Bitmap(partition) => partition.serialized_size(),
            Partition::Full => 1,
        }
    }
}

impl<L: Level> PartitionWrite<L> for Partition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let inserted = match self {
            Partition::Tree(partition) => partition.insert(value),
            Partition::Vec(partition) => partition.insert(value),
            Partition::Bitmap(partition) => partition.insert(value),
            Partition::Full => false,
        };

        if inserted {
            let new_partition = match self {
                Partition::Tree(p) => p.maybe_change_storage(),
                Partition::Vec(p) => p.maybe_change_storage(),
                Partition::Bitmap(p) => p.maybe_change_storage(),
                _ => None,
            };

            if let Some(new_partition) = new_partition {
                *self = new_partition;
            }
        }

        inserted
    }
}

impl<L: Level> FromIterator<L::Value> for Partition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let partition: VecPartition<L> = iter.into_iter().collect();
        if let Some(p) = partition.maybe_change_storage() {
            p
        } else {
            Partition::Vec(partition)
        }
    }
}
