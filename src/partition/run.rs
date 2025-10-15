use std::{
    fmt::Debug,
    iter::{self, FusedIterator},
    mem::size_of,
    ops::{BitAndAssign, BitOrAssign, BitXorAssign, RangeBounds, RangeInclusive, SubAssign},
};

use bytes::BufMut;
use itertools::{FoldWhile, Itertools};
use num::{PrimInt, cast::AsPrimitive, traits::ConstOne};
use range_set_blaze::{Integer, RangeSetBlaze, SortedDisjoint, SortedStarts};

use crate::{
    PartitionWrite,
    codec::{Encodable, encoder::Encoder, runs_ref::RunsRef},
    count::count_unique_sorted,
    level::Level,
    partition::Partition,
    segment::SplitSegment,
    traits::{Complement, Cut, PartitionRead, TruncateFrom},
    util::RangeExt,
};

pub(crate) trait Run<T> {
    fn len(&self) -> usize;
    fn position(&self, v: T) -> usize;
    fn rank(&self, v: T) -> usize;
    fn select(&self, idx: usize) -> Option<T>;
    fn first(&self) -> T;
    fn last(&self) -> T;
}

impl<T> Run<T> for RangeInclusive<T>
where
    T: PrimInt + AsPrimitive<usize> + TruncateFrom<usize>,
{
    #[inline]
    fn len(&self) -> usize {
        self.end().as_() - self.start().as_() + 1
    }

    #[inline]
    fn position(&self, v: T) -> usize {
        v.min(*self.end()).as_() - self.start().as_()
    }

    #[inline]
    fn rank(&self, v: T) -> usize {
        v.min(*self.end()).as_() - self.start().as_() + 1
    }

    #[inline]
    fn select(&self, idx: usize) -> Option<T> {
        let n = *self.start() + T::truncate_from(idx);
        (n <= *self.end()).then_some(n)
    }

    #[inline]
    fn first(&self) -> T {
        *self.start()
    }

    #[inline]
    fn last(&self) -> T {
        *self.end()
    }
}

pub(crate) fn run_position<T, I>(iter: I, value: T) -> Option<usize>
where
    T: PrimInt + AsPrimitive<usize> + TruncateFrom<usize>,
    I: IntoIterator<Item = RangeInclusive<T>>,
{
    let mut found = false;

    let pos = iter
        .into_iter()
        .fold_while(0, |acc, run| {
            if value < run.first() {
                FoldWhile::Done(acc)
            } else if value <= run.last() {
                found = true;
                FoldWhile::Done(acc + run.position(value))
            } else {
                FoldWhile::Continue(acc + run.len())
            }
        })
        .into_inner();

    found.then_some(pos)
}

pub(crate) fn run_rank<T, I>(iter: I, value: T) -> usize
where
    T: PrimInt + AsPrimitive<usize> + TruncateFrom<usize>,
    I: IntoIterator<Item = RangeInclusive<T>>,
{
    iter.into_iter()
        .fold_while(0, |acc, run| {
            if value < run.first() {
                FoldWhile::Done(acc)
            } else if value <= run.last() {
                FoldWhile::Done(acc + run.rank(value))
            } else {
                FoldWhile::Continue(acc + run.len())
            }
        })
        .into_inner()
}

pub(crate) fn run_select<T, I>(iter: I, mut n: usize) -> Option<T>
where
    T: PrimInt + AsPrimitive<usize> + TruncateFrom<usize>,
    I: IntoIterator<Item = RangeInclusive<T>>,
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

#[derive(Clone, Eq)]
pub struct RunPartition<L: Level> {
    runs: RangeSetBlaze<L::Value>,
}

impl<L: Level> RunPartition<L> {
    #[inline]
    pub const fn encoded_size(runs: usize) -> usize {
        let vsize = size_of::<L::ValueUnaligned>();
        // runs + len
        (runs * vsize * 2) + vsize
    }

    /// Construct an `RunPartition` from a sorted iter of unique values
    /// SAFETY: undefined behavior if the iter is not sorted or contains duplicates
    pub fn from_sorted_unique_unchecked(values: impl Iterator<Item = L::Value>) -> Self {
        Self {
            runs: MergeRuns::new(values).into_range_set_blaze(),
        }
    }

    #[inline]
    pub fn count_runs(&self) -> usize {
        self.runs.ranges().len()
    }

    pub fn segments(&self) -> usize {
        let segments = self
            .runs
            .ranges()
            .flat_map(|r| r.start().segment()..=r.end().segment());
        count_unique_sorted(segments)
    }
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
        write!(
            f,
            "RunPartition<{}>{{ cardinality: {}, ranges: {} }}",
            L::DEBUG_NAME,
            self.cardinality(),
            self.runs.ranges_len()
        )
    }
}

impl<L: Level> FromIterator<L::Value> for RunPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        Self { runs: RangeSetBlaze::from_iter(iter) }
    }
}

impl<L: Level> Encodable for RunPartition<L> {
    #[inline]
    fn encoded_size(&self) -> usize {
        Self::encoded_size(self.count_runs())
    }

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>) {
        encoder.put_run_partition::<L>(self.runs.ranges());
    }
}

impl<L: Level> PartitionRead<L> for RunPartition<L> {
    fn cardinality(&self) -> usize {
        // SAFETY: this is safe so long as L::Value is smaller than 2^53
        <L::Value as Integer>::safe_len_to_f64_lossy(self.runs.len()) as usize
    }

    fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    fn contains(&self, value: L::Value) -> bool {
        self.runs.contains(value)
    }

    fn position(&self, value: L::Value) -> Option<usize> {
        run_position(self.runs.ranges(), value)
    }

    fn rank(&self, value: L::Value) -> usize {
        run_rank(self.runs.ranges(), value)
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        run_select(self.runs.ranges(), idx)
    }

    fn last(&self) -> Option<L::Value> {
        self.runs.last()
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.runs.iter()
    }
}

impl<L: Level> PartitionWrite<L> for RunPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        self.runs.insert(value)
    }

    fn remove(&mut self, value: L::Value) -> bool {
        self.runs.remove(value)
    }

    fn remove_range<R: RangeBounds<L::Value>>(&mut self, values: R) {
        if let Some(values) = values.try_into_inclusive() {
            let set = RangeSetBlaze::from_iter([values]);
            self.runs = &self.runs - set;
        }
    }
}

impl<L: Level> PartialEq for RunPartition<L> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.runs == other.runs
    }
}

impl<L: Level> PartialEq<&RunsRef<'_, L>> for &RunPartition<L> {
    fn eq(&self, other: &&RunsRef<'_, L>) -> bool {
        itertools::equal(self.runs.ranges(), other.ranges())
    }
}

impl<L: Level> BitOrAssign<&RunPartition<L>> for RunPartition<L> {
    fn bitor_assign(&mut self, rhs: &RunPartition<L>) {
        self.runs |= &rhs.runs;
    }
}

impl<L: Level> BitOrAssign<&RunsRef<'_, L>> for RunPartition<L> {
    fn bitor_assign(&mut self, rhs: &RunsRef<'_, L>) {
        self.runs.extend(rhs.ranges())
    }
}

impl<L: Level> BitAndAssign<&RunPartition<L>> for RunPartition<L> {
    fn bitand_assign(&mut self, rhs: &RunPartition<L>) {
        self.runs = &self.runs & &rhs.runs;
    }
}

impl<L: Level> BitAndAssign<&RunsRef<'_, L>> for RunPartition<L> {
    fn bitand_assign(&mut self, rhs: &RunsRef<'_, L>) {
        self.runs = (self.runs.ranges() & rhs.ranges()).into_range_set_blaze();
    }
}

impl<L: Level> BitXorAssign<&RunPartition<L>> for RunPartition<L> {
    fn bitxor_assign(&mut self, rhs: &RunPartition<L>) {
        self.runs = &self.runs ^ &rhs.runs;
    }
}

impl<L: Level> BitXorAssign<&RunsRef<'_, L>> for RunPartition<L> {
    fn bitxor_assign(&mut self, rhs: &RunsRef<'_, L>) {
        self.runs = (self.runs.ranges() ^ rhs.ranges()).into_range_set_blaze();
    }
}

impl<L: Level> SubAssign<&RunPartition<L>> for RunPartition<L> {
    fn sub_assign(&mut self, rhs: &RunPartition<L>) {
        self.runs = &self.runs - &rhs.runs;
    }
}

impl<L: Level> SubAssign<&RunsRef<'_, L>> for RunPartition<L> {
    fn sub_assign(&mut self, rhs: &RunsRef<'_, L>) {
        self.runs = (self.runs.ranges() - rhs.ranges()).into_range_set_blaze();
    }
}

impl<L: Level> Cut for RunPartition<L> {
    type Out = Partition<L>;

    fn cut(&mut self, rhs: &Self) -> Self::Out {
        let intersection = (self.runs.ranges() & rhs.runs.ranges()).into_range_set_blaze();
        self.runs = (self.runs.ranges() - intersection.ranges()).into_range_set_blaze();
        Partition::Run(Self { runs: intersection })
    }
}

impl<L: Level> Cut<RunsRef<'_, L>> for RunPartition<L> {
    type Out = Partition<L>;

    fn cut(&mut self, rhs: &RunsRef<'_, L>) -> Self::Out {
        let intersection = (self.runs.ranges() & rhs.ranges()).into_range_set_blaze();
        self.runs = (self.runs.ranges() - intersection.ranges()).into_range_set_blaze();
        Partition::Run(Self { runs: intersection })
    }
}

impl<L: Level> Complement for RunPartition<L> {
    fn complement(&mut self) {
        self.runs = self.runs.ranges().complement().into_range_set_blaze();
    }
}

impl<L: Level> From<&RunsRef<'_, L>> for RunPartition<L> {
    fn from(value: &RunsRef<'_, L>) -> Self {
        Self {
            runs: value.ranges().into_range_set_blaze(),
        }
    }
}

impl<L: Level> From<RangeInclusive<L::Value>> for RunPartition<L> {
    fn from(value: RangeInclusive<L::Value>) -> Self {
        Self {
            runs: RangeSetBlaze::from_iter(iter::once(value)),
        }
    }
}

impl<L: Level> Extend<L::Value> for RunPartition<L> {
    #[inline]
    fn extend<T: IntoIterator<Item = L::Value>>(&mut self, iter: T) {
        self.runs |= RangeSetBlaze::from_iter(iter);
    }
}

#[must_use]
pub(crate) struct MergeRuns<I, T> {
    inner: I,
    run: Option<(T, T)>,
}

impl<I, T> MergeRuns<I, T>
where
    T: PrimInt + ConstOne,
    I: Iterator<Item = T>,
{
    pub(crate) fn new(mut inner: I) -> Self {
        let run = inner.next().map(|x| (x, x));
        Self { inner, run }
    }
}

impl<I, T> FusedIterator for MergeRuns<I, T>
where
    T: PrimInt + ConstOne,
    I: Iterator<Item = T>,
{
}

impl<I, T> Iterator for MergeRuns<I, T>
where
    T: PrimInt + ConstOne,
    I: Iterator<Item = T>,
{
    type Item = RangeInclusive<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cursor) = self.run.as_mut() {
            for next in self.inner.by_ref() {
                if cursor.1 + T::ONE == next {
                    cursor.1 = next;
                } else {
                    let run = cursor.0..=cursor.1;
                    *cursor = (next, next);
                    return Some(run);
                }
            }
        }
        self.run.take().map(|(a, b)| a..=b)
    }
}

impl<I, T> SortedStarts<T> for MergeRuns<I, T>
where
    T: PrimInt + ConstOne + range_set_blaze::Integer,
    I: Iterator<Item = T>,
{
}

impl<I, T> SortedDisjoint<T> for MergeRuns<I, T>
where
    T: PrimInt + ConstOne + range_set_blaze::Integer,
    I: Iterator<Item = T>,
{
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use proptest::proptest;

    use crate::{
        level::Block,
        testutil::{test_partition_read, test_partition_write},
    };

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

        itertools::assert_equal(partition.runs.ranges(), [1..=2, 5..=5, 7..=8, 11..=11]);
        itertools::assert_equal(vals.into_iter(), partition.iter());
    }

    #[test]
    fn test_merge_runs() {
        let vals = [1, 2, 3, 5, 7, 8, 10];
        let merged = MergeRuns::new(vals.into_iter());
        itertools::assert_equal(merged, [1..=3, 5..=5, 7..=8, 10..=10]);
    }

    #[test]
    fn test_run_write() {
        let mut partition = RunPartition::<Block>::from_iter(0..=255);
        test_partition_write(&mut partition);
    }

    proptest! {
        #[test]
        fn test_run_small_read_proptest(set: HashSet<u8>) {
            let expected = set.iter().copied().sorted().collect_vec();
            let partition = RunPartition::<Block>::from_iter(set);
            test_partition_read(&partition, &expected);
        }

    }
}
