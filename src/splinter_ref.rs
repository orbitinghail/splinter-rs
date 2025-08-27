use std::{fmt::Debug, ops::Deref};

use bytes::Bytes;
use zerocopy::FromBytes;

use crate::{
    Splinter,
    codec::{DecodeErr, Encodable, encoder::Encoder, footer::Footer, partition_ref::PartitionRef},
    level::High,
    traits::PartitionRead,
};

#[derive(Clone)]
pub struct SplinterRef<B> {
    pub(crate) data: B,
}

impl<B: Deref<Target = [u8]>> Debug for SplinterRef<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SplinterRef")
            .field(&self.load_unchecked())
            .finish()
    }
}

impl<B> SplinterRef<B> {
    #[inline]
    pub fn inner(&self) -> &B {
        &self.data
    }

    #[inline]
    pub fn into_inner(self) -> B {
        self.data
    }
}

impl SplinterRef<Bytes> {
    #[inline]
    pub fn encode_to_bytes(&self) -> Bytes {
        self.data.clone()
    }
}

impl<B: Deref<Target = [u8]>> Encodable for SplinterRef<B> {
    #[inline]
    fn encoded_size(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn encode<T: bytes::BufMut>(&self, encoder: &mut Encoder<T>) {
        encoder.write_splinter(&self.data);
    }
}

impl<B: Deref<Target = [u8]>> SplinterRef<B> {
    pub fn decode_to_splinter(&self) -> Splinter {
        Splinter::from_iter(self.iter())
    }

    pub fn from_bytes(data: B) -> Result<Self, DecodeErr> {
        pub(crate) const SPLINTER_V1_MAGIC: [u8; 4] = [0xDA, 0xAE, 0x12, 0xDF];
        if data.len() >= SPLINTER_V1_MAGIC.len() && data.starts_with(&SPLINTER_V1_MAGIC) {
            return Err(DecodeErr::SplinterV1);
        }

        if data.len() < Footer::SIZE {
            return Err(DecodeErr::Length);
        }
        let (partitions, footer) = data.split_at(data.len() - Footer::SIZE);
        Footer::ref_from_bytes(footer)?.validate(partitions)?;
        PartitionRef::<High>::from_suffix(partitions)?;
        Ok(Self { data })
    }

    pub(crate) fn load_unchecked(&self) -> PartitionRef<'_, High> {
        let without_footer = &self.data[..(self.data.len() - Footer::SIZE)];
        PartitionRef::from_suffix(without_footer).unwrap()
    }
}

impl<B: Deref<Target = [u8]>> PartitionRead<High> for SplinterRef<B> {
    fn cardinality(&self) -> usize {
        self.load_unchecked().cardinality()
    }

    fn is_empty(&self) -> bool {
        self.load_unchecked().is_empty()
    }

    fn contains(&self, value: u32) -> bool {
        self.load_unchecked().contains(value)
    }

    fn rank(&self, value: u32) -> usize {
        self.load_unchecked().rank(value)
    }

    fn select(&self, idx: usize) -> Option<u32> {
        self.load_unchecked().select(idx)
    }

    fn last(&self) -> Option<u32> {
        self.load_unchecked().last()
    }

    fn iter(&self) -> impl Iterator<Item = u32> {
        self.load_unchecked().into_iter()
    }
}

impl<B: Deref<Target = [u8]>> PartialEq<Splinter> for SplinterRef<B> {
    #[inline]
    fn eq(&self, other: &Splinter) -> bool {
        other == self
    }
}
