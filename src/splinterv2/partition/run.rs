use std::{fmt::Debug, ops::RangeInclusive};

use bytes::BufMut;
use itertools::{FoldWhile, Itertools};
use num::{PrimInt, cast::AsPrimitive, traits::ConstOne};
use rangemap::{RangeInclusiveSet, StepFns};

use crate::splinterv2::{
    PartitionWrite,
    codec::{Encodable, encoder::Encoder},
    count::count_unique_sorted,
    level::Level,
    segment::SplitSegment,
    traits::{PartitionRead, TruncateFrom},
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

pub(crate) trait Run<L: Level> {
    fn len(&self) -> usize;
    fn rank(&self, v: L::Value) -> usize;
    fn select(&self, idx: usize) -> Option<L::Value>;
    fn last(&self) -> L::Value;
}

impl<L: Level> Run<L> for RangeInclusive<L::Value> {
    #[inline]
    fn len(&self) -> usize {
        (*self.end() - *self.start() + L::Value::ONE).as_()
    }

    #[inline]
    fn rank(&self, v: L::Value) -> usize {
        (v.min(*self.end()) - *self.start() + L::Value::ONE).as_()
    }

    #[inline]
    fn select(&self, idx: usize) -> Option<L::Value> {
        let n = *self.start() + L::Value::truncate_from(idx);
        (n <= *self.end()).then_some(n)
    }

    #[inline]
    fn last(&self) -> L::Value {
        *self.end()
    }
}

pub(crate) fn run_rank<L, I, R>(iter: I, value: L::Value) -> usize
where
    L: Level,
    I: IntoIterator<Item = R>,
    R: Run<L>,
{
    iter.into_iter()
        .fold_while(0, |acc, run| {
            if value <= run.last() {
                FoldWhile::Continue(acc + run.rank(value))
            } else {
                FoldWhile::Done(acc)
            }
        })
        .into_inner()
}

pub(crate) fn run_select<L, I, R>(iter: I, mut n: usize) -> Option<L::Value>
where
    L: Level,
    I: IntoIterator<Item = R>,
    R: Run<L>,
{
    for run in iter.into_iter() {
        let len = run.len();
        if n < len {
            return run.select(n);
        }
        n -= len;
    }
    None
}

#[derive(Clone, PartialEq, Eq)]
pub struct RunPartition<L: Level> {
    runs: RangeInclusiveSet<L::Value, NumStep>,
}

impl<L: Level> RunPartition<L> {
    #[inline]
    pub const fn encoded_size(runs: usize) -> usize {
        let val_size = std::mem::size_of::<L::ValueUnaligned>();
        runs * (val_size * 2)
    }

    /// Construct an `RunPartition` from a sorted iter of unique values
    /// SAFETY: undefined behavior if the iter is not sorted or contains duplicates
    pub fn from_sorted_unique_unchecked(mut values: impl Iterator<Item = L::Value>) -> Self {
        let Some(first) = values.next() else {
            return RunPartition::default();
        };
        let mut runs = RangeInclusiveSet::<L::Value, NumStep>::default();
        let mut cursor = (first, first);
        for value in values {
            // since the input iterator is sorted and unique, we only need to
            // check if the next value is adjacent to the current range
            if cursor.1 + L::Value::ONE == value {
                cursor.1 = value;
            } else {
                runs.insert(cursor.0..=cursor.1);
                cursor = (value, value);
            }
        }
        runs.insert(cursor.0..=cursor.1);
        RunPartition { runs }
    }

    #[inline]
    pub fn count_runs(&self) -> usize {
        self.runs.len()
    }

    #[inline]
    pub fn sparsity_ratio(&self) -> f64 {
        let segments = self
            .runs
            .iter()
            .flat_map(|r| r.start().segment()..=r.end().segment());
        let unique_segments = count_unique_sorted(segments);
        unique_segments as f64 / self.cardinality() as f64
    }
}

impl<L: Level> Default for RunPartition<L> {
    fn default() -> Self {
        RunPartition {
            runs: RangeInclusiveSet::<L::Value, NumStep>::new_with_step_fns(),
        }
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

impl<L: Level> FromIterator<L::Value> for RunPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let values = iter.into_iter().sorted().dedup();
        // SAFETY: the iterator is sorted and deduped
        Self::from_sorted_unique_unchecked(values)
    }
}

impl<L: Level> Encodable for RunPartition<L> {
    #[inline]
    fn encoded_size(&self) -> usize {
        Self::encoded_size(self.runs.len())
    }

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>) {
        if self.is_empty() {
            encoder.put_empty_partition();
        } else {
            encoder.put_run_partition::<L>(self.runs.iter());
        }
    }
}

impl<L: Level> PartitionRead<L> for RunPartition<L> {
    fn cardinality(&self) -> usize {
        self.runs.iter().map(|run| Run::<L>::len(run)).sum()
    }

    fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    fn contains(&self, value: L::Value) -> bool {
        self.runs.contains(&value)
    }

    fn rank(&self, value: L::Value) -> usize {
        run_rank::<L, _, _>(self.runs.iter().cloned(), value)
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        run_select::<L, _, _>(self.runs.iter().cloned(), idx)
    }

    fn last(&self) -> Option<L::Value> {
        self.runs.last().map(|r| r.end()).copied()
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.runs.iter().flat_map(RunIter::from)
    }
}

impl<L: Level> PartitionWrite<L> for RunPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
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

pub(crate) struct RunIter<T> {
    start: T,
    end: T,
}

impl<T> RunIter<T> {
    pub fn new(start: T, end: T) -> Self {
        Self { start, end }
    }
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

#[cfg(test)]
mod tests {
    use crate::splinterv2::level::Block;

    use super::*;

    #[test]
    fn test_run_partition() {
        let mut partition = RunPartition::<Block>::default();
        assert!(partition.insert(1));
        assert!(partition.insert(2));
        assert!(partition.insert(3));
        assert!(!partition.insert(2));
        assert_eq!(partition.cardinality(), 3);
        assert!(partition.contains(1));
        assert!(partition.contains(2));
        assert!(partition.contains(3));
        assert!(!partition.contains(4));
        assert_eq!(partition.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn test_run_partition_from_iter() {
        let vals = [1, 2, 5, 7, 8, 11];
        let partition = RunPartition::<Block>::from_sorted_unique_unchecked(vals.iter().copied());

        itertools::assert_equal(
            partition.runs.iter().cloned(),
            [1..=2, 5..=5, 7..=8, 11..=11],
        );

        itertools::assert_equal(vals.into_iter(), partition.iter());
    }
}
