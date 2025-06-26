use culprit::Culprit;
use either::Either;

use crate::{DecodeErr, Splinter, SplinterRead, SplinterRef, SplinterWrite, util::CopyToOwned};

// A Clone-on-write Splinter
pub enum CowSplinter<B> {
    Ref(SplinterRef<B>),
    Owned(Splinter),
}

impl<B> CowSplinter<B>
where
    B: AsRef<[u8]>,
{
    pub fn from_owned(splinter: Splinter) -> Self {
        Self::Owned(splinter)
    }

    pub fn from_ref(splinter: SplinterRef<B>) -> Self {
        Self::Ref(splinter)
    }

    pub fn from_bytes(data: B) -> Result<Self, Culprit<DecodeErr>> {
        Ok(Self::Ref(SplinterRef::from_bytes(data)?))
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
        match self {
            CowSplinter::Ref(splinter) => {
                let mut owned = splinter.copy_to_owned();
                let result = owned.insert(key);
                *self = CowSplinter::Owned(owned);
                result
            }
            CowSplinter::Owned(splinter) => splinter.insert(key),
        }
    }
}
