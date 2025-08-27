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

impl<B: Deref<Target = [u8]>, B2: Deref<Target = [u8]>> PartialEq<SplinterRef<B2>>
    for SplinterRef<B>
{
    fn eq(&self, other: &SplinterRef<B2>) -> bool {
        self.load_unchecked() == other.load_unchecked()
    }
}

#[cfg(test)]
mod test {
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::{
        Optimizable, PartitionRead, Splinter,
        testutil::{SetGen, mksplinter},
    };

    #[test]
    fn test_empty() {
        let splinter = mksplinter(&[]).encode_to_splinter_ref();

        assert_eq!(splinter.decode_to_splinter(), Splinter::default());
        assert!(!splinter.contains(0));
        assert_eq!(splinter.cardinality(), 0);
        assert_eq!(splinter.last(), None);
    }

    /// This is a regression test for a bug in the SplinterRef encoding. The bug
    /// was that we used LittleEndian encoded values to store unaligned values,
    /// which sort in reverse order from what we expect.
    #[test]
    fn test_contains_bug() {
        let mut set_gen = SetGen::new(0xDEAD_BEEF);
        let set = set_gen.random(1024);
        let lookup = set[(set.len() / 3) as usize];
        let splinter = mksplinter(&set).encode_to_splinter_ref();
        assert!(splinter.contains(lookup))
    }

    #[quickcheck]
    fn test_splinter_ref_quickcheck(set: Vec<u32>) -> bool {
        let splinter = mksplinter(&set).encode_to_splinter_ref();
        if set.is_empty() {
            !splinter.contains(123)
        } else {
            let lookup = set[set.len() / 3];
            splinter.contains(lookup)
        }
    }

    #[quickcheck]
    fn test_splinter_opt_ref_quickcheck(set: Vec<u32>) -> bool {
        let mut splinter = mksplinter(&set);
        splinter.optimize();
        let splinter = splinter.encode_to_splinter_ref();
        if set.is_empty() {
            !splinter.contains(123)
        } else {
            let lookup = set[set.len() / 3];
            splinter.contains(lookup)
        }
    }

    #[quickcheck]
    fn test_splinter_ref_eq_quickcheck(set: Vec<u32>) -> bool {
        let ref1 = mksplinter(&set).encode_to_splinter_ref();
        let ref2 = mksplinter(&set).encode_to_splinter_ref();
        ref1 == ref2
    }

    #[quickcheck]
    fn test_splinter_opt_ref_eq_quickcheck(set: Vec<u32>) -> bool {
        let mut ref1 = mksplinter(&set);
        ref1.optimize();
        let ref1 = ref1.encode_to_splinter_ref();
        let ref2 = mksplinter(&set).encode_to_splinter_ref();
        ref1 == ref2
    }

    #[quickcheck]
    fn test_splinter_ref_ne_quickcheck(set1: Vec<u32>, set2: Vec<u32>) -> TestResult {
        if set1 == set2 {
            TestResult::discard()
        } else {
            let ref1 = mksplinter(&set1).encode_to_splinter_ref();
            let ref2 = mksplinter(&set2).encode_to_splinter_ref();
            TestResult::from_bool(ref1 != ref2)
        }
    }

    #[quickcheck]
    fn test_splinter_opt_ref_ne_quickcheck(set1: Vec<u32>, set2: Vec<u32>) -> TestResult {
        if set1 == set2 {
            TestResult::discard()
        } else {
            let mut ref1 = mksplinter(&set1);
            ref1.optimize();
            let ref1 = ref1.encode_to_splinter_ref();
            let ref2 = mksplinter(&set2).encode_to_splinter_ref();
            TestResult::from_bool(ref1 != ref2)
        }
    }
}
