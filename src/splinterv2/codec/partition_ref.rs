use std::ops::RangeInclusive;

use bitvec::{order::Lsb0, slice::BitSlice};
use either::Either;
use num::traits::{AsPrimitive, ConstOne};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::{
    MultiIter,
    splinterv2::{
        PartitionRead,
        codec::{DecodeErr, tree_ref::TreeRef},
        level::Level,
        partition::{
            PartitionKind,
            bitmap::BitmapPartition,
            run::{Run, RunIter, RunPartition, run_rank, run_select},
        },
        traits::TruncateFrom,
    },
};

#[derive(Debug, IntoBytes, FromBytes, Unaligned, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct EncodedRun<L: Level> {
    /// inclusive start
    start: L::ValueUnaligned,
    /// inclusive end
    end: L::ValueUnaligned,
}

impl<L: Level> EncodedRun<L> {
    pub fn len(&self) -> usize {
        (self.end.into() - self.start.into() + L::Value::ONE).as_()
    }

    pub fn contains(&self, value: L::ValueUnaligned) -> bool {
        self.start <= value && value <= self.end
    }

    pub fn iter(&self) -> RunIter<L::Value> {
        RunIter::new(self.start.into(), self.end.into())
    }
}

impl<L: Level> From<&RangeInclusive<L::Value>> for EncodedRun<L> {
    fn from(range: &RangeInclusive<L::Value>) -> Self {
        let start = (*range.start()).into();
        let end = (*range.end()).into();
        EncodedRun { start, end }
    }
}

impl<L: Level> Run<L> for &EncodedRun<L> {
    fn len(&self) -> usize {
        EncodedRun::len(self)
    }

    fn rank(&self, v: L::Value) -> usize {
        (v.min(self.end.into()) - self.start.into() + L::Value::ONE).as_()
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        let n = self.start.into() + L::Value::truncate_from(idx);
        (n <= self.end.into()).then_some(n)
    }

    fn last(&self) -> L::Value {
        self.end.into()
    }
}

pub(super) fn decode_len<L: Level>(data: &[u8]) -> Result<(&[u8], usize), DecodeErr> {
    let (data, len) = L::ValueUnaligned::try_read_from_suffix(data)?;
    // length is decremented when stored
    Ok((data, len.into().as_() + 1))
}

#[derive(Debug, Clone)]
pub(crate) enum NonRecursivePartitionRef<'a, L: Level> {
    Empty,
    Full,
    Bitmap { bitmap: &'a BitSlice<u8, Lsb0> },
    Vec { values: &'a [L::ValueUnaligned] },
    Run { runs: &'a [EncodedRun<L>] },
}

impl<'a, L: Level> NonRecursivePartitionRef<'a, L> {
    pub fn from_suffix_with_kind(kind: PartitionKind, data: &'a [u8]) -> Result<Self, DecodeErr> {
        match kind {
            PartitionKind::Empty => Ok(Self::Empty),
            PartitionKind::Full => Ok(Self::Full),
            PartitionKind::Bitmap => {
                let len = BitmapPartition::<L>::ENCODED_SIZE;
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                let bitmap: &[u8] = zerocopy::transmute_ref!(&data[range]);
                Ok(Self::Bitmap { bitmap: BitSlice::from_slice(bitmap) })
            }
            PartitionKind::Vec => {
                let (data, len) = decode_len::<L>(data)?;
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                Ok(Self::Vec {
                    values: zerocopy::transmute_ref!(&data[range]),
                })
            }
            PartitionKind::Run => {
                let (data, runs) = decode_len::<L>(data)?;
                let len = RunPartition::<L>::encoded_size(runs);
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                Ok(Self::Run {
                    runs: <[EncodedRun<L>]>::ref_from_bytes(&data[range])?,
                })
            }
            PartitionKind::Tree => unreachable!("non-recursive"),
        }
    }
}

impl<'a, L: Level> PartitionRead<L> for NonRecursivePartitionRef<'a, L> {
    fn cardinality(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Full => L::MAX_LEN,
            Self::Bitmap { bitmap } => bitmap.count_ones(),
            Self::Vec { values } => values.len(),
            Self::Run { runs } => runs.iter().map(|run| run.len()).sum(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Full => false,
            Self::Bitmap { bitmap } => bitmap.not_any(),
            Self::Vec { values } => values.is_empty(),
            Self::Run { runs } => runs.is_empty(),
        }
    }

    fn contains(&self, value: L::Value) -> bool {
        match self {
            Self::Empty => false,
            Self::Full => true,
            Self::Bitmap { bitmap } => bitmap.get(value.as_()).is_some(),
            Self::Vec { values } => values.binary_search(&value.into()).is_ok(),
            Self::Run { runs } => runs.iter().any(|run| run.contains(value.into())),
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        match self {
            Self::Empty => Iter::Empty(std::iter::empty()),
            Self::Full => Iter::Full((0..L::MAX_LEN).map(L::Value::truncate_from)),
            Self::Bitmap { bitmap } => {
                Iter::Bitmap(bitmap.iter_ones().map(L::Value::truncate_from))
            }
            Self::Vec { values } => Iter::Vec(values.iter().map(|&v| v.into())),
            Self::Run { runs } => Iter::Run(runs.iter().flat_map(|run| run.iter())),
        }
    }

    fn rank(&self, value: L::Value) -> usize {
        match self {
            Self::Empty => 0,
            Self::Full => value.as_() + 1,
            Self::Bitmap { bitmap } => {
                let prefix = bitmap.get(0..=value.as_());
                prefix.unwrap().count_ones()
            }
            Self::Vec { values } => match values.binary_search(&value.into()) {
                Ok(index) => index + 1,
                Err(index) => index,
            },
            Self::Run { runs } => run_rank(runs.iter(), value),
        }
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        match self {
            Self::Empty => None,
            Self::Full => Some(L::Value::truncate_from(idx)),
            Self::Bitmap { bitmap } => bitmap.iter_ones().nth(idx).map(L::Value::truncate_from),
            Self::Vec { values } => values.get(idx).map(|&v| v.into()),
            Self::Run { runs } => run_select(runs.iter(), idx),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PartitionRef<'a, L: Level> {
    NonRecursive(NonRecursivePartitionRef<'a, L>),
    Tree(TreeRef<'a, L>),
}

impl<'a, L: Level> PartitionRef<'a, L> {
    pub fn from_suffix(data: &'a [u8]) -> Result<Self, DecodeErr> {
        let (data, kind) = PartitionKind::try_read_from_suffix(data)?;
        Self::from_suffix_with_kind(kind, data)
    }

    pub fn from_suffix_with_kind(kind: PartitionKind, data: &'a [u8]) -> Result<Self, DecodeErr> {
        match kind {
            PartitionKind::Tree => Ok(Self::Tree(TreeRef::from_suffix(data)?)),
            kind => Ok(Self::NonRecursive(
                NonRecursivePartitionRef::from_suffix_with_kind(kind, data)?,
            )),
        }
    }
}

impl<'a, L: Level> PartitionRead<L> for PartitionRef<'a, L> {
    fn cardinality(&self) -> usize {
        match self {
            Self::NonRecursive(p) => p.cardinality(),
            Self::Tree(p) => p.cardinality(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::NonRecursive(p) => p.is_empty(),
            Self::Tree(p) => p.is_empty(),
        }
    }

    fn contains(&self, value: L::Value) -> bool {
        match self {
            Self::NonRecursive(p) => p.contains(value),
            Self::Tree(p) => p.contains(value),
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        match self {
            Self::NonRecursive(p) => Either::Left(p.iter()),
            Self::Tree(p) => Either::Right(p.iter()),
        }
    }

    fn rank(&self, value: L::Value) -> usize {
        match self {
            Self::NonRecursive(p) => p.rank(value),
            Self::Tree(p) => p.rank(value),
        }
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        match self {
            Self::NonRecursive(p) => p.select(idx),
            Self::Tree(p) => p.select(idx),
        }
    }
}

MultiIter!(Iter, Empty, Full, Bitmap, Vec, Run);

impl<'a, L: Level> IntoIterator for NonRecursivePartitionRef<'a, L> {
    type Item = L::Value;

    type IntoIter = Box<dyn Iterator<Item = L::Value> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Empty => Box::new(std::iter::empty()),
            Self::Full => Box::new((0..L::MAX_LEN).map(L::Value::truncate_from)),
            Self::Bitmap { bitmap } => Box::new(bitmap.iter_ones().map(L::Value::truncate_from)),
            Self::Vec { values } => Box::new(values.iter().map(|&v| v.into())),
            Self::Run { runs } => Box::new(runs.iter().flat_map(|run| run.iter())),
        }
    }
}

impl<'a, L: Level> IntoIterator for PartitionRef<'a, L> {
    type Item = L::Value;

    type IntoIter = Box<dyn Iterator<Item = L::Value> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::NonRecursive(p) => p.into_iter(),
            Self::Tree(tree_ref) => tree_ref.into_iter(),
        }
    }
}
