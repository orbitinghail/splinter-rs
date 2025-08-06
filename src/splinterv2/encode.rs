//! TODO: Encoder/Encodable
//! The idea is to build a serde style encoder which can be abstracted over
//! various destination types, while making heavy use of `bytes::bufmut` and zerocopy.
//!
//! The main benefit of Splinter's tail encoding is that the offset indexes are
//! emitted after each partition. This allows us to efficiently seek through the
//! encoding without making copies.
//!
//! However, we could still put offset indexes in the beginning by reserving
//! space ahead of time. Since cardinality is no longer expensive to compute, we
//! should be able to easily do this with the `BufMut` API.
//!
//! The other benefit of tail encoding is that we can encode directly into a
//! socket/stream. We aren't currently taking advantage of this.

use std::convert::Infallible;

use bytes::BufMut;
use num::traits::AsPrimitive;
use thiserror::Error;
use zerocopy::{
    ConvertError, FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
};

use crate::splinterv2::{
    level::{Block, Level},
    partition::{bitmap::BitmapPartition, run::RunPartition, vec::VecPartition},
    segment::Segment,
};

pub trait Encodable {
    fn encoded_size(&self) -> usize;

    fn encode(&self, buf: &mut impl BufMut);
}

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("not enough bytes")]
    Length,

    #[error("invalid encoding")]
    Validity,
}

impl DecodeErr {
    #[inline]
    fn ensure_length_available(data: &[u8], len: usize) -> Result<(), DecodeErr> {
        if data.len() < len {
            Err(Self::Length)
        } else {
            Ok(())
        }
    }
}

impl<S, V> From<ConvertError<Infallible, S, V>> for DecodeErr {
    fn from(err: ConvertError<Infallible, S, V>) -> Self {
        match err {
            ConvertError::Alignment(_) => unreachable!("Infallible alignment"),
            ConvertError::Size(_) => DecodeErr::Length,
            ConvertError::Validity(_) => DecodeErr::Validity,
        }
    }
}

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

#[derive(Debug)]
#[repr(C)]
pub(crate) enum Container<'a, L: Level> {
    Full,
    Bitmap {
        bitmap: &'a [u8],
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

impl<'a, L: Level> Container<'a, L> {
    pub fn from_suffix(data: &'a [u8]) -> Result<Self, DecodeErr> {
        let (data, kind) = ContainerKind::try_read_from_suffix(data)?;

        match kind {
            ContainerKind::Full => Ok(Container::Full),
            ContainerKind::Bitmap => {
                let len = BitmapPartition::<L>::ENCODED_SIZE;
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                Ok(Container::Bitmap {
                    bitmap: zerocopy::transmute_ref!(&data[range]),
                })
            }
            ContainerKind::Vec => {
                let (data, len) = L::ValueUnaligned::try_read_from_suffix(data)?;
                let len: usize = len.into().as_();
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                Ok(Container::Vec {
                    values: zerocopy::transmute_ref!(&data[range]),
                })
            }
            ContainerKind::Run => {
                let (data, runs) = L::ValueUnaligned::try_read_from_suffix(data)?;
                let runs: usize = runs.into().as_();
                let len = RunPartition::<L>::encoded_size(runs);
                DecodeErr::ensure_length_available(data, len)?;
                let range = (data.len() - len)..data.len();
                Ok(Container::Run {
                    runs: zerocopy::transmute_ref!(&data[range]),
                })
            }
            ContainerKind::Tree => {
                let (data, num_children) = L::ValueUnaligned::try_read_from_suffix(data)?;
                let num_children: usize = num_children.into().as_();
                let segments_size = {
                    let as_vec = VecPartition::<Block>::encoded_size(num_children);
                    let as_bmp = BitmapPartition::<Block>::ENCODED_SIZE;
                    as_vec.min(as_bmp)
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
