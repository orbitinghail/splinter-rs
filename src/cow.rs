use std::{fmt::Debug, ops::Deref};

use bytes::{BufMut, Bytes};
use culprit::Culprit;
use either::Either;

use crate::{
    Encodable, PartitionRead, PartitionWrite, Splinter, SplinterRef,
    codec::{DecodeErr, encoder::Encoder},
    level::High,
};

// A Clone-on-write Splinter
#[derive(Clone)]
pub enum CowSplinter<B> {
    Ref(SplinterRef<B>),
    Owned(Splinter),
}

impl<B> Default for CowSplinter<B> {
    fn default() -> Self {
        Self::Owned(Splinter::default())
    }
}

impl<B: Deref<Target = [u8]>> Debug for CowSplinter<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CowSplinter::Ref(splinter_ref) => f
                .debug_tuple("CowSplinter::Ref")
                .field(splinter_ref)
                .finish(),
            CowSplinter::Owned(splinter) => {
                f.debug_tuple("CowSplinter::Owned").field(splinter).finish()
            }
        }
    }
}

impl<B, K: Into<u32>> FromIterator<K> for CowSplinter<B>
where
    B: Deref<Target = [u8]>,
{
    fn from_iter<I: IntoIterator<Item = K>>(iter: I) -> Self {
        Self::Owned(Splinter::from_iter(iter.into_iter().map(|k| k.into())))
    }
}

impl<B> From<Splinter> for CowSplinter<B> {
    fn from(splinter: Splinter) -> Self {
        Self::Owned(splinter)
    }
}

impl<B> From<SplinterRef<B>> for CowSplinter<B> {
    fn from(splinter_ref: SplinterRef<B>) -> Self {
        Self::Ref(splinter_ref)
    }
}

impl<B: Deref<Target = [u8]>> From<CowSplinter<B>> for Splinter {
    fn from(cow_splinter: CowSplinter<B>) -> Self {
        cow_splinter.into_owned()
    }
}

impl From<CowSplinter<Bytes>> for SplinterRef<Bytes> {
    fn from(cow: CowSplinter<Bytes>) -> Self {
        match cow {
            CowSplinter::Ref(splinter_ref) => splinter_ref,
            CowSplinter::Owned(splinter) => splinter.encode_to_splinter_ref(),
        }
    }
}

impl<B> CowSplinter<B> {
    pub fn from_owned(splinter: Splinter) -> Self {
        Self::Owned(splinter)
    }

    pub fn from_ref(splinter: SplinterRef<B>) -> Self {
        Self::Ref(splinter)
    }
}

impl<B: Deref<Target = [u8]>> CowSplinter<B> {
    pub fn from_bytes(data: B) -> Result<Self, Culprit<DecodeErr>> {
        Ok(Self::Ref(SplinterRef::from_bytes(data)?))
    }

    pub fn into_owned(self) -> Splinter {
        match self {
            Self::Ref(splinter_ref) => splinter_ref.decode_to_splinter(),
            Self::Owned(splinter) => splinter,
        }
    }

    pub fn to_mut(&mut self) -> &mut Splinter {
        match *self {
            Self::Ref(ref splinter_ref) => {
                *self = Self::Owned(splinter_ref.decode_to_splinter());
                match *self {
                    Self::Ref(..) => unreachable!(),
                    Self::Owned(ref mut owned) => owned,
                }
            }
            Self::Owned(ref mut owned) => owned,
        }
    }
}

impl CowSplinter<Bytes> {
    pub fn encode_to_bytes(&self) -> Bytes {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.encode_to_bytes(),
            CowSplinter::Owned(splinter) => splinter.encode_to_bytes(),
        }
    }
}

impl<B: Deref<Target = [u8]>> Encodable for CowSplinter<B> {
    fn encoded_size(&self) -> usize {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.encoded_size(),
            CowSplinter::Owned(splinter) => splinter.encoded_size(),
        }
    }

    fn encode<T: BufMut>(&self, encoder: &mut Encoder<T>) {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.encode(encoder),
            CowSplinter::Owned(splinter) => splinter.encode(encoder),
        }
    }
}

impl<B: Deref<Target = [u8]>> PartitionRead<High> for CowSplinter<B> {
    fn cardinality(&self) -> usize {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.cardinality(),
            CowSplinter::Owned(splinter) => splinter.cardinality(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.is_empty(),
            CowSplinter::Owned(splinter) => splinter.is_empty(),
        }
    }

    fn contains(&self, value: u32) -> bool {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.contains(value),
            CowSplinter::Owned(splinter) => splinter.contains(value),
        }
    }

    fn rank(&self, value: u32) -> usize {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.rank(value),
            CowSplinter::Owned(splinter) => splinter.rank(value),
        }
    }

    fn select(&self, idx: usize) -> Option<u32> {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.select(idx),
            CowSplinter::Owned(splinter) => splinter.select(idx),
        }
    }

    fn last(&self) -> Option<u32> {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.last(),
            CowSplinter::Owned(splinter) => splinter.last(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = u32> {
        match self {
            CowSplinter::Ref(splinter_ref) => Either::Left(splinter_ref.iter()),
            CowSplinter::Owned(splinter) => Either::Right(splinter.iter()),
        }
    }
}

impl<B: Deref<Target = [u8]>> PartitionWrite<High> for CowSplinter<B> {
    fn insert(&mut self, value: u32) -> bool {
        self.to_mut().insert(value)
    }

    fn remove(&mut self, value: u32) -> bool {
        self.to_mut().remove(value)
    }
}
