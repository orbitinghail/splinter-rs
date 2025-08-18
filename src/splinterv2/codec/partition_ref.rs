use bitvec::{order::Lsb0, slice::BitSlice};
use either::Either;
use num::traits::{AsPrimitive, Bounded};
use zerocopy::{FromBytes, TryFromBytes};

use crate::{
    MultiIter,
    splinterv2::{
        PartitionRead,
        codec::{DecodeErr, runs_ref::RunsRef, tree_ref::TreeRef},
        level::{Block, Level},
        partition::{PartitionKind, bitmap::BitmapPartition},
        traits::TruncateFrom,
    },
};

pub(super) fn decode_len_from_suffix<L: Level>(data: &[u8]) -> Result<(&[u8], usize), DecodeErr> {
    let (data, len) = L::ValueUnaligned::try_read_from_suffix(data)?;
    // length is decremented when stored
    Ok((data, len.into().as_() + 1))
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum NonRecursivePartitionRef<'a, L: Level> {
    Empty,
    Full,
    Bitmap { bitmap: &'a BitSlice<u8, Lsb0> },
    Vec { values: &'a [L::ValueUnaligned] },
    Run { runs: RunsRef<'a, L> },
}

impl<'a, L: Level> NonRecursivePartitionRef<'a, L> {
    pub fn from_suffix_with_kind(kind: PartitionKind, data: &'a [u8]) -> Result<Self, DecodeErr> {
        match kind {
            PartitionKind::Empty => Ok(Self::Empty),
            PartitionKind::Full => Ok(Self::Full),
            PartitionKind::Bitmap => {
                let bytes = BitmapPartition::<L>::ENCODED_SIZE;
                DecodeErr::ensure_bytes_available(data, bytes)?;
                let range = (data.len() - bytes)..data.len();
                Ok(Self::Bitmap {
                    bitmap: BitSlice::from_slice(&data[range]),
                })
            }
            PartitionKind::Vec => {
                let (data, len) = decode_len_from_suffix::<L>(data)?;
                let bytes = len * size_of::<L::ValueUnaligned>();
                DecodeErr::ensure_bytes_available(data, bytes)?;
                let range = (data.len() - bytes)..data.len();
                Ok(Self::Vec {
                    values: <[L::ValueUnaligned]>::ref_from_bytes_with_elems(&data[range], len)?,
                })
            }
            PartitionKind::Run => Ok(Self::Run { runs: RunsRef::from_suffix(data)? }),
            PartitionKind::Tree => unreachable!("non-recursive"),
        }
    }

    #[cfg(test)]
    pub fn kind(&self) -> PartitionKind {
        match self {
            Self::Empty => PartitionKind::default(),
            Self::Full => PartitionKind::Full,
            Self::Bitmap { .. } => PartitionKind::Bitmap,
            Self::Vec { .. } => PartitionKind::Vec,
            Self::Run { .. } => PartitionKind::Run,
        }
    }
}

impl<'a> NonRecursivePartitionRef<'a, Block> {
    pub(crate) fn tree_segments_from_suffix(
        kind: PartitionKind,
        num_children: usize,
        data: &'a [u8],
    ) -> Result<Self, DecodeErr> {
        match kind {
            PartitionKind::Full => Ok(Self::Full),
            PartitionKind::Bitmap => {
                assert!(
                    num_children > 32 && num_children < 256,
                    "num_children out of range"
                );
                let bytes = BitmapPartition::<Block>::ENCODED_SIZE;
                DecodeErr::ensure_bytes_available(data, bytes)?;
                let range = (data.len() - bytes)..data.len();
                Ok(Self::Bitmap {
                    bitmap: BitSlice::from_slice(&data[range]),
                })
            }
            PartitionKind::Vec => {
                let bytes = num_children * size_of::<<Block as Level>::ValueUnaligned>();
                DecodeErr::ensure_bytes_available(data, bytes)?;
                let range = (data.len() - bytes)..data.len();
                Ok(Self::Vec {
                    values: <[<Block as Level>::ValueUnaligned]>::ref_from_bytes_with_elems(
                        &data[range],
                        num_children,
                    )?,
                })
            }
            _ => unreachable!("tree segments must be one of Full, Bitmap, or Vec"),
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
            Self::Run { runs } => runs.cardinality(),
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
            Self::Bitmap { bitmap } => *bitmap.get(value.as_()).unwrap(),
            Self::Vec { values } => values.binary_search(&value.into()).is_ok(),
            Self::Run { runs } => runs.contains(value),
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
            Self::Run { runs } => runs.rank(value),
        }
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        match self {
            Self::Empty => None,
            Self::Full => Some(L::Value::truncate_from(idx)),
            Self::Bitmap { bitmap } => bitmap.iter_ones().nth(idx).map(L::Value::truncate_from),
            Self::Vec { values } => values.get(idx).map(|&v| v.into()),
            Self::Run { runs } => runs.select(idx),
        }
    }

    fn last(&self) -> Option<L::Value> {
        match self {
            Self::Empty => None,
            Self::Full => Some(L::Value::max_value()),
            Self::Bitmap { bitmap } => bitmap.last_one().map(L::Value::truncate_from),
            Self::Vec { values } => values.last().map(|&v| v.into()),
            Self::Run { runs } => runs.last(),
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
            Self::Run { runs } => Iter::Run(runs.iter()),
        }
    }
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum PartitionRef<'a, L: Level> {
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

    #[cfg(test)]
    pub fn kind(&self) -> PartitionKind {
        match self {
            Self::NonRecursive(p) => p.kind(),
            Self::Tree(_) => PartitionKind::Tree,
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

    fn last(&self) -> Option<L::Value> {
        match self {
            Self::NonRecursive(p) => p.last(),
            Self::Tree(p) => p.last(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        match self {
            Self::NonRecursive(p) => Either::Left(p.iter()),
            Self::Tree(p) => Either::Right(p.iter()),
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
            Self::Run { runs } => Box::new(runs.to_iter()),
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
