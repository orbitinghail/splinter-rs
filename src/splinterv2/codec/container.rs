use std::ops::RangeInclusive;

use bitvec::{order::Lsb0, slice::BitSlice};
use num::traits::AsPrimitive;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::splinterv2::{
    PartitionRead,
    codec::DecodeErr,
    level::{Block, Level},
    partition::{bitmap::BitmapPartition, run::RunPartition, vec::VecPartition},
    segment::Segment,
};

#[derive(Debug, IntoBytes, TryFromBytes, Unaligned, KnownLayout, Immutable)]
#[repr(u8)]
pub(crate) enum ContainerKind {
    Full = 0x01,
    Bitmap = 0x02,
    Vec = 0x03,
    Run = 0x04,
    Tree = 0x05,
}

#[derive(Debug, IntoBytes, FromBytes, Unaligned, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct EncodedRun<L: Level> {
    start: L::ValueUnaligned,
    end: L::ValueUnaligned,
}

impl<L: Level> From<&RangeInclusive<L::Value>> for EncodedRun<L> {
    fn from(range: &RangeInclusive<L::Value>) -> Self {
        let start = (*range.start()).into();
        let end = (*range.end()).into();
        EncodedRun { start, end }
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) enum Container<'a, L: Level> {
    Full,
    Bitmap {
        bitmap: &'a BitSlice<u8, Lsb0>,
    },
    Vec {
        values: &'a [L::ValueUnaligned],
    },
    Run {
        runs: &'a [EncodedRun<L>],
    },
    Tree {
        num_children: usize,
        segments: &'a [Segment],
        offsets: &'a [L::ValueUnaligned],
        children: &'a [u8],
    },
}

fn decode_len<L: Level>(data: &[u8]) -> Result<(&[u8], usize), DecodeErr> {
    let (data, len) = L::ValueUnaligned::try_read_from_suffix(data)?;
    // length is decremented when stored
    Ok((data, len.into().as_() + 1))
}

impl<'a, L: Level> Container<'a, L> {
    pub fn from_suffix(data: &'a [u8]) -> Result<Self, DecodeErr> {
        let (data, kind) = ContainerKind::try_read_from_suffix(data)?;

        match kind {
            ContainerKind::Full => Ok(Container::Full),
            ContainerKind::Bitmap => {
                let len = BitmapPartition::<L>::ENCODED_SIZE;
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                let bitmap: &[u8] = zerocopy::transmute_ref!(&data[range]);
                Ok(Container::Bitmap { bitmap: BitSlice::from_slice(bitmap) })
            }
            ContainerKind::Vec => {
                let (data, len) = decode_len::<L>(data)?;
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                Ok(Container::Vec {
                    values: zerocopy::transmute_ref!(&data[range]),
                })
            }
            ContainerKind::Run => {
                let (data, runs) = decode_len::<L>(data)?;
                let len = RunPartition::<L>::encoded_size(runs);
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                Ok(Container::Run {
                    runs: <[EncodedRun<L>]>::ref_from_bytes(&data[range])?,
                })
            }
            ContainerKind::Tree => {
                let (data, num_children) = decode_len::<Block>(data)?;
                let segments_size = {
                    if num_children == 256 {
                        0
                    } else {
                        let as_vec = VecPartition::<Block>::encoded_size(num_children);
                        let as_bmp = BitmapPartition::<Block>::ENCODED_SIZE;
                        as_vec.min(as_bmp)
                    }
                };
                let offsets_size = num_children * size_of::<L::ValueUnaligned>();

                DecodeErr::ensure_length_available(data, segments_size)?;
                DecodeErr::ensure_length_available(data, offsets_size)?;

                let segments_range = (data.len() - segments_size)..data.len();
                let offsets_range = (segments_range.start - offsets_size)..segments_range.start;
                let data_range = 0..offsets_range.start;

                Ok(Container::Tree {
                    num_children,
                    segments: zerocopy::transmute_ref!(&data[segments_range]),
                    offsets: zerocopy::transmute_ref!(&data[offsets_range]),
                    children: &data[data_range],
                })
            }
        }
    }
}

impl<'a, L: Level> PartitionRead<L> for Container<'a, L> {
    fn cardinality(&self) -> usize {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn contains(&self, value: <L as Level>::Value) -> bool {
        todo!()
    }

    fn iter(&self) -> impl Iterator<Item = <L as Level>::Value> {
        todo!();
        std::iter::empty()
    }
}
