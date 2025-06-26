use std::fmt::Debug;

use bytes::Bytes;
use culprit::Culprit;
use either::Either;

use crate::{DecodeErr, Splinter, SplinterRead, SplinterRef, SplinterWrite, util::CopyToOwned};

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

impl<B: AsRef<[u8]>> Debug for CowSplinter<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.fmt(f),
            CowSplinter::Owned(splinter) => splinter.fmt(f),
        }
    }
}

impl<B, K: Into<u32>> FromIterator<K> for CowSplinter<B>
where
    B: AsRef<[u8]>,
{
    fn from_iter<I: IntoIterator<Item = K>>(iter: I) -> Self {
        Self::Owned(Splinter::from_iter(iter))
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

impl<B: AsRef<[u8]>> From<CowSplinter<B>> for Splinter {
    fn from(cow_splinter: CowSplinter<B>) -> Self {
        cow_splinter.into_owned()
    }
}

impl From<CowSplinter<Bytes>> for SplinterRef<Bytes> {
    fn from(cow: CowSplinter<Bytes>) -> Self {
        match cow {
            CowSplinter::Ref(splinter_ref) => splinter_ref,
            CowSplinter::Owned(splinter) => splinter.serialize_to_splinter_ref(),
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

impl<B: AsRef<[u8]>> CowSplinter<B> {
    pub fn from_bytes(data: B) -> Result<Self, Culprit<DecodeErr>> {
        Ok(Self::Ref(SplinterRef::from_bytes(data)?))
    }

    pub fn into_owned(self) -> Splinter {
        match self {
            Self::Ref(splinter_ref) => splinter_ref.copy_to_owned(),
            Self::Owned(splinter) => splinter,
        }
    }

    pub fn to_mut(&mut self) -> &mut Splinter {
        match *self {
            Self::Ref(ref splinter_ref) => {
                *self = Self::Owned(splinter_ref.copy_to_owned());
                match *self {
                    Self::Ref(..) => unreachable!(),
                    Self::Owned(ref mut owned) => owned,
                }
            }
            Self::Owned(ref mut owned) => owned,
        }
    }

    pub fn serialized_size(&self) -> usize {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.size(),
            CowSplinter::Owned(splinter) => splinter.serialized_size(),
        }
    }

    pub fn serialize<T: bytes::BufMut>(&self, out: &mut T) -> usize {
        match self {
            CowSplinter::Ref(splinter_ref) => {
                out.put_slice(splinter_ref.inner().as_ref());
                splinter_ref.size()
            }
            CowSplinter::Owned(splinter) => splinter.serialize(out),
        }
    }
}

impl CowSplinter<Bytes> {
    pub fn serialize_into_bytes<T: bytes::BufMut>(&self, out: &mut T) -> usize {
        match self {
            CowSplinter::Ref(splinter_ref) => {
                out.put(splinter_ref.inner().clone());
                splinter_ref.size()
            }
            CowSplinter::Owned(splinter) => splinter.serialize(out),
        }
    }

    pub fn serialize_to_bytes(&self) -> Bytes {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.inner().clone(),
            CowSplinter::Owned(splinter) => splinter.serialize_to_bytes(),
        }
    }
}

impl<B: AsRef<[u8]>> SplinterRead for CowSplinter<B> {
    fn is_empty(&self) -> bool {
        match self {
            CowSplinter::Ref(splinter) => splinter.is_empty(),
            CowSplinter::Owned(splinter) => splinter.is_empty(),
        }
    }

    fn contains(&self, key: u32) -> bool {
        match self {
            CowSplinter::Ref(splinter) => splinter.contains(key),
            CowSplinter::Owned(splinter) => splinter.contains(key),
        }
    }

    fn cardinality(&self) -> usize {
        match self {
            CowSplinter::Ref(splinter) => splinter.cardinality(),
            CowSplinter::Owned(splinter) => splinter.cardinality(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        match self {
            CowSplinter::Ref(splinter) => Either::Left(splinter.iter()),
            CowSplinter::Owned(splinter) => Either::Right(splinter.iter()),
        }
    }

    fn range<'a, R>(&'a self, range: R) -> impl Iterator<Item = u32> + 'a
    where
        R: std::ops::RangeBounds<u32> + 'a,
    {
        match self {
            CowSplinter::Ref(splinter) => Either::Left(splinter.range(range)),
            CowSplinter::Owned(splinter) => Either::Right(splinter.range(range)),
        }
    }

    fn last(&self) -> Option<u32> {
        match self {
            CowSplinter::Ref(splinter) => splinter.last(),
            CowSplinter::Owned(splinter) => splinter.last(),
        }
    }
}

impl<B: AsRef<[u8]>> SplinterWrite for CowSplinter<B> {
    /// Inserts a key into the splinter, converting it to an owned version if it was a reference.
    fn insert(&mut self, key: u32) -> bool {
        self.to_mut().insert(key)
    }
}
