use std::ops::RangeInclusive;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::splinterv2::{
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

    pub fn ranges(&self) -> impl Iterator<Item = RangeInclusive<L::Value>> {
        self.runs.iter().map(|r| r.into())
    }

    pub fn into_iter(self) -> impl Iterator<Item = L::Value> {
        self.runs
            .into_iter()
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
