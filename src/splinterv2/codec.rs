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
use thiserror::Error;
use zerocopy::ConvertError;

use crate::splinterv2::codec::encoder::Encoder;

pub mod container;
pub mod encoder;

pub trait Encodable {
    fn encoded_size(&self) -> usize;

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>);
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

impl<A, S, V> From<ConvertError<A, S, V>> for DecodeErr {
    fn from(err: ConvertError<A, S, V>) -> Self {
        match err {
            ConvertError::Alignment(_) => panic!("All zerocopy transmutations must be unaligned"),
            ConvertError::Size(_) => DecodeErr::Length,
            ConvertError::Validity(_) => DecodeErr::Validity,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::splinterv2::{
        Encodable, Partition,
        codec::{container::Container, encoder::Encoder},
        level::{Block, Level},
        partition::vec::VecPartition,
    };

    fn test_encode_decode<L: Level>() {
        // NOTES:
        //
        // - implement PartitionRead for Container
        // - consider renaming Container to PartitionRef
        // - write round trip test for many different partition types

        // struct T {
        //     partition: Partition,
        //     check: fn(Container<'_, L>),
        // }

        // let partition = Partition::Full;
        // let mut encoder = Encoder::new(BytesMut::default());
        // partition.encode(&mut encoder);
        // let buf = encoder.into_inner();
        // let container = Container::<'_, Block>::from_suffix(&buf).unwrap();

        // let Container::Vec { values } = container else {
        //     panic!("Unexpected container type");
        // };

        // assert_eq!(values, vec![1, 3, 5, 7]);
    }

    #[test]
    fn test_encode_decode_vec() {
        let partition = VecPartition::<Block>::from_iter([1, 3, 5, 7]);
        let mut encoder = Encoder::new(BytesMut::default());
        partition.encode(&mut encoder);
        let buf = encoder.into_inner();
        let container = Container::<'_, Block>::from_suffix(&buf).unwrap();

        let Container::Vec { values } = container else {
            panic!("Unexpected container type");
        };

        assert_eq!(values, vec![1, 3, 5, 7]);
    }
}
