use std::{iter::FusedIterator, ops::RangeInclusive};

use range_set_blaze::{SortedDisjoint, SortedStarts};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::{
    PartitionRead,
    codec::{DecodeErr, partition_ref::decode_len_from_suffix},
    level::Level,
    partition::run::{Run, run_rank, run_select},
};

#[derive(Debug, Clone)]
pub struct RunsRef<'a, L: Level> {
    runs: &'a [EncodedRun<L>],
}

impl<'a, L: Level> RunsRef<'a, L> {
    pub(super) fn from_suffix(data: &'a [u8]) -> Result<Self, DecodeErr> {
        let (data, runs) = decode_len_from_suffix::<L>(data)?;
        let bytes = runs * size_of::<L::ValueUnaligned>() * 2;
        DecodeErr::ensure_bytes_available(data, bytes)?;
        let range = (data.len() - bytes)..data.len();
        Ok(Self {
            runs: <[EncodedRun<L>]>::ref_from_bytes(&data[range])?,
        })
    }

    pub fn ranges(&self) -> RangesIter<'_, L> {
        RangesIter { inner: self.runs.iter() }
    }

    pub fn to_iter(self) -> impl Iterator<Item = L::Value> {
        self.runs
            .iter()
            .flat_map(|r| num::iter::range_inclusive(r.start.into(), r.end.into()))
    }
}

impl<L: Level> PartitionRead<L> for RunsRef<'_, L> {
    #[inline]
    fn cardinality(&self) -> usize {
        self.ranges().map(|run| run.len()).sum()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    #[inline]
    fn contains(&self, value: <L as Level>::Value) -> bool {
        self.ranges().any(|run| run.contains(&value))
    }

    #[inline]
    fn rank(&self, value: <L as Level>::Value) -> usize {
        run_rank(self.ranges(), value)
    }

    #[inline]
    fn select(&self, idx: usize) -> Option<<L as Level>::Value> {
        run_select(self.ranges(), idx)
    }

    #[inline]
    fn last(&self) -> Option<<L as Level>::Value> {
        self.runs.last().map(|v| v.end.into())
    }

    fn iter(&self) -> impl Iterator<Item = <L as Level>::Value> {
        self.ranges()
            .flat_map(|r| num::iter::range_inclusive(r.first(), r.last()))
    }
}

impl<L: Level> PartialEq for RunsRef<'_, L> {
    fn eq(&self, other: &Self) -> bool {
        self.runs == other.runs
    }
}

#[derive(Debug, IntoBytes, FromBytes, Unaligned, KnownLayout, Immutable, Clone)]
#[repr(C)]
#[doc(hidden)]
pub struct EncodedRun<L: Level> {
    /// inclusive start
    start: L::ValueUnaligned,
    /// inclusive end
    end: L::ValueUnaligned,
}

impl<L: Level> From<&EncodedRun<L>> for RangeInclusive<L::Value> {
    #[inline]
    fn from(value: &EncodedRun<L>) -> Self {
        value.start.into()..=value.end.into()
    }
}

impl<L: Level> From<RangeInclusive<L::Value>> for EncodedRun<L> {
    fn from(range: RangeInclusive<L::Value>) -> Self {
        let start = (*range.start()).into();
        let end = (*range.end()).into();
        EncodedRun { start, end }
    }
}

impl<L: Level> PartialEq for EncodedRun<L> {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end
    }
}

pub struct RangesIter<'a, L: Level> {
    inner: std::slice::Iter<'a, EncodedRun<L>>,
}

impl<'a, L: Level> Iterator for RangesIter<'a, L> {
    type Item = RangeInclusive<L::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| r.into())
    }
}

impl<L: Level> DoubleEndedIterator for RangesIter<'_, L> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|r| r.into())
    }
}

impl<L: Level> FusedIterator for RangesIter<'_, L> {}
impl<L: Level> SortedStarts<L::Value> for RangesIter<'_, L> {}
impl<L: Level> SortedDisjoint<L::Value> for RangesIter<'_, L> {}
impl<L: Level> ExactSizeIterator for RangesIter<'_, L> {}
