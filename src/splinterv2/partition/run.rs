use std::{fmt::Debug, ops::RangeInclusive};

use num::{PrimInt, cast::AsPrimitive, traits::ConstOne};
use rangemap::{RangeInclusiveSet, StepFns};

use crate::splinterv2::{
    PartitionWrite,
    encode::Encodable,
    level::Level,
    partition::Partition,
    traits::{Optimizable, PartitionRead},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NumStep;
impl<T: PrimInt + ConstOne> StepFns<T> for NumStep {
    fn add_one(start: &T) -> T {
        *start + T::ONE
    }

    fn sub_one(start: &T) -> T {
        *start - T::ONE
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RunPartition<L: Level> {
    runs: RangeInclusiveSet<L::Value, NumStep>,
}

impl<L: Level> Default for RunPartition<L> {
    fn default() -> Self {
        RunPartition { runs: Default::default() }
    }
}

impl<L: Level> Debug for RunPartition<L>
where
    L::Value: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RunPartition<{}>({})", L::DEBUG_NAME, self.cardinality())
    }
}

impl<L: Level> Optimizable<Partition<L>> for RunPartition<L> {
    fn shallow_optimize(&self) -> Option<Partition<L>> {
        todo!()
    }
}

impl<L: Level> FromIterator<L::Value> for RunPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(_iter: I) -> Self {
        todo!()
    }
}

impl<L: Level> Encodable for RunPartition<L> {
    fn encoded_size(&self) -> usize {
        let val_size = L::BITS / 8;
        self.runs.len() * (val_size * 2)
    }
}

impl<L: Level> PartitionRead<L> for RunPartition<L> {
    fn cardinality(&self) -> usize {
        self.runs
            .iter()
            .map(|run| (*run.end() - *run.start() + L::Value::ONE).as_())
            .sum()
    }

    fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    fn contains(&self, value: <L as Level>::Value) -> bool {
        self.runs.contains(&value)
    }

    fn iter(&self) -> impl Iterator<Item = <L as Level>::Value> {
        self.runs.iter().flat_map(RunIter::from)
    }
}

impl<L: Level> PartitionWrite<L> for RunPartition<L> {
    fn insert(&mut self, value: <L as Level>::Value) -> bool {
        // TODO: ideally self.runs.insert would return some signal when it
        // changes the underlying btree
        if self.runs.contains(&value) {
            false
        } else {
            self.runs.insert(value..=value);
            true
        }
    }
}

struct RunIter<T> {
    start: T,
    end: T,
}

impl<T: Copy> From<&RangeInclusive<T>> for RunIter<T> {
    fn from(range: &RangeInclusive<T>) -> Self {
        Self { start: *range.start(), end: *range.end() }
    }
}

impl<T: PrimInt + ConstOne> Iterator for RunIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start > self.end {
            None
        } else {
            let value = self.start;
            self.start = self.start + T::ONE;
            Some(value)
        }
    }
}
