use std::{fmt::Debug, ops::Deref};

use bytes::Bytes;
use zerocopy::FromBytes;

use crate::{
    Splinter,
    codec::{
        DecodeErr, Encodable,
        encoder::Encoder,
        footer::{Footer, SPLINTER_V2_MAGIC},
        partition_ref::PartitionRef,
    },
    level::High,
    traits::PartitionRead,
};

/// A zero-copy reference to serialized splinter data.
///
/// `SplinterRef` allows efficient querying of compressed bitmap data without
/// deserializing the underlying structure. It wraps any type that can be
/// dereferenced to `[u8]` and provides all the same read operations as
/// [`Splinter`], but with minimal memory overhead and no allocation during
/// queries.
///
/// This is the preferred type for read-only operations on serialized splinter
/// data, especially when the data comes from files, network, or other external
/// sources.
///
/// # Type Parameter
///
/// - `B`: Any type that implements `Deref<Target = [u8]>` such as `&[u8]`,
///   `Vec<u8>`, `Bytes`, `Arc<[u8]>`, etc.
///
/// # Examples
///
/// Creating from serialized bytes:
///
/// ```
/// use splinter_rs::{Splinter, SplinterRef, PartitionWrite, PartitionRead, Encodable};
///
/// // Create and populate a splinter
/// let mut splinter = Splinter::EMPTY;
/// splinter.insert(100);
/// splinter.insert(200);
///
/// // Serialize it to bytes
/// let bytes = splinter.encode_to_bytes();
///
/// // Create a zero-copy reference
/// let splinter_ref = SplinterRef::from_bytes(bytes).unwrap();
/// assert_eq!(splinter_ref.cardinality(), 2);
/// assert!(splinter_ref.contains(100));
/// ```
///
/// Working with different buffer types:
///
/// ```
/// use splinter_rs::{Splinter, SplinterRef, PartitionWrite, PartitionRead, Encodable};
/// use std::sync::Arc;
///
/// let mut splinter = Splinter::EMPTY;
/// splinter.insert(42);
///
/// let bytes = splinter.encode_to_bytes();
/// let shared_bytes: Arc<[u8]> = bytes.to_vec().into();
///
/// let splinter_ref = SplinterRef::from_bytes(shared_bytes).unwrap();
/// assert!(splinter_ref.contains(42));
/// ```
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
    /// Returns a reference to the underlying data buffer.
    ///
    /// This provides access to the raw bytes that store the serialized splinter
    /// data.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, SplinterRef, PartitionWrite, Encodable};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// splinter.insert(42);
    /// let bytes = splinter.encode_to_bytes();
    /// let splinter_ref = SplinterRef::from_bytes(bytes).unwrap();
    ///
    /// let inner_bytes = splinter_ref.inner();
    /// assert!(!inner_bytes.is_empty());
    /// ```
    #[inline]
    pub fn inner(&self) -> &B {
        &self.data
    }

    /// Consumes the `SplinterRef` and returns the underlying data buffer.
    ///
    /// This is useful when you need to take ownership of the underlying data
    /// after you're done querying the splinter.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, SplinterRef, PartitionWrite, Encodable};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// splinter.insert(42);
    /// let bytes = splinter.encode_to_bytes();
    /// let splinter_ref = SplinterRef::from_bytes(bytes.clone()).unwrap();
    ///
    /// let recovered_bytes = splinter_ref.into_inner();
    /// assert_eq!(recovered_bytes, bytes);
    /// ```
    #[inline]
    pub fn into_inner(self) -> B {
        self.data
    }
}

impl SplinterRef<Bytes> {
    /// Returns a clone of the underlying bytes.
    ///
    /// This is efficient for `Bytes` since it uses reference counting
    /// internally and doesn't actually copy the data.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionWrite};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// splinter.insert(42);
    /// let splinter_ref = splinter.encode_to_splinter_ref();
    ///
    /// let bytes_copy = splinter_ref.encode_to_bytes();
    /// assert!(!bytes_copy.is_empty());
    /// ```
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
    /// Converts this reference back to an owned [`Splinter`].
    ///
    /// This method deserializes the underlying data and creates a new owned
    /// `Splinter` that supports mutation. This involves iterating through all
    /// values and rebuilding the data structure, so it has a cost proportional
    /// to the number of elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, SplinterRef, PartitionWrite, PartitionRead};
    ///
    /// let mut original = Splinter::EMPTY;
    /// original.insert(100);
    /// original.insert(200);
    ///
    /// let splinter_ref = original.encode_to_splinter_ref();
    /// let decoded = splinter_ref.decode_to_splinter();
    ///
    /// assert_eq!(decoded.cardinality(), 2);
    /// assert!(decoded.contains(100));
    /// assert!(decoded.contains(200));
    /// ```
    pub fn decode_to_splinter(&self) -> Splinter {
        Splinter::from_iter(self.iter())
    }

    /// Creates a `SplinterRef` from raw bytes, validating the format.
    ///
    /// This method parses and validates the serialized splinter format, checking:
    /// - Sufficient data length
    /// - Valid magic bytes
    /// - Correct checksum
    ///
    /// IMPORTANT: This method *does not* recursively verify the entire
    /// splinter, opting instead to rely on the checksum to detect any
    /// corruption. Do not use Splinter with untrusted data as it's trivial to
    /// construct a Splinter which will cause your program to panic at runtime.
    ///
    /// Returns an error if the data is corrupted or in an invalid format.
    ///
    /// # Errors
    ///
    /// - [`DecodeErr::Length`]: Not enough bytes in the buffer
    /// - [`DecodeErr::Magic`]: Invalid magic bytes
    /// - [`DecodeErr::Checksum`]: Data corruption detected
    /// - [`DecodeErr::Validity`]: Invalid internal structure
    /// - [`DecodeErr::SplinterV1`]: Data is from incompatible v1 format
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, SplinterRef, PartitionWrite, PartitionRead, Encodable};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// splinter.insert(42);
    /// let bytes = splinter.encode_to_bytes();
    ///
    /// let splinter_ref = SplinterRef::from_bytes(bytes).unwrap();
    /// assert!(splinter_ref.contains(42));
    /// ```
    ///
    /// Error handling:
    ///
    /// ```
    /// use splinter_rs::{SplinterRef, codec::DecodeErr};
    ///
    /// let invalid_bytes = vec![0u8; 5]; // Too short
    /// let result = SplinterRef::from_bytes(invalid_bytes);
    /// assert!(matches!(result, Err(DecodeErr::Length)));
    /// ```
    pub fn from_bytes(data: B) -> Result<Self, DecodeErr> {
        pub(crate) const SPLINTER_V1_MAGIC: [u8; 4] = [0xDA, 0xAE, 0x12, 0xDF];
        if data.len() >= 4
            && data.starts_with(&SPLINTER_V1_MAGIC)
            && !data.ends_with(&SPLINTER_V2_MAGIC)
        {
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

        assert_eq!(splinter.decode_to_splinter(), Splinter::EMPTY);
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
