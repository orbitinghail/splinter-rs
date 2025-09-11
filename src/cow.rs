use std::{
    fmt::Debug,
    ops::{Deref, RangeBounds},
};

use bytes::{BufMut, Bytes};
use culprit::Culprit;
use either::Either;

use crate::{
    Encodable, PartitionRead, PartitionWrite, Splinter, SplinterRef,
    codec::{DecodeErr, encoder::Encoder},
    level::High,
};

/// A clone-on-write splinter that can hold either a reference or an owned value.
///
/// `CowSplinter` is an enum that can contain either a [`SplinterRef<B>`] (for zero-copy
/// read-only access) or an owned [`Splinter`] (for mutable operations). It automatically
/// converts from read-only to owned when mutation is needed, providing an efficient
/// way to work with splinter data that might come from different sources.
///
/// This is particularly useful when you want to:
/// - Start with serialized/borrowed data for read-only operations
/// - Potentially modify the data later
/// - Avoid unnecessary copying until mutation is actually needed
/// - Or work with a collection of Splinters which may or may not be owned
///
/// # Examples
///
/// Starting with a reference and converting to owned when needed:
///
/// ```
/// use splinter_rs::{Splinter, CowSplinter, PartitionWrite, PartitionRead, Encodable};
///
/// // Create a CowSplinter from a reference
/// let original = Splinter::from_iter([100, 200]);
/// let splinter_ref = original.encode_to_splinter_ref();
/// let mut cow = CowSplinter::from_ref(splinter_ref);
///
/// // Read operations work on the reference
/// assert_eq!(cow.cardinality(), 2);
///
/// // First write operation converts to owned
/// cow.insert(300); // This triggers clone-on-write
/// assert_eq!(cow.cardinality(), 3);
/// ```
///
/// Creating from different sources:
///
/// ```
/// use splinter_rs::{Splinter, CowSplinter, PartitionWrite, PartitionRead, Encodable};
/// use bytes::Bytes;
///
/// // From owned splinter
/// let owned = Splinter::EMPTY;
/// let cow1: CowSplinter<Bytes> = CowSplinter::from_owned(owned);
///
/// // From iterator
/// let cow2 = CowSplinter::<Bytes>::from_iter([1u32, 2, 3]);
///
/// // From bytes
/// let bytes = cow2.encode_to_bytes();
/// let cow3 = CowSplinter::from_bytes(bytes).unwrap();
/// ```
#[derive(Clone)]
pub enum CowSplinter<B> {
    /// Contains a zero-copy reference to serialized data
    Ref(SplinterRef<B>),
    /// Contains an owned, mutable splinter
    Owned(Splinter),
}

impl<B> Default for CowSplinter<B> {
    fn default() -> Self {
        Self::Owned(Splinter::EMPTY)
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
    /// Creates a `CowSplinter` from an owned [`Splinter`].
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, CowSplinter, PartitionRead, Encodable};
    ///
    /// let splinter = Splinter::from_iter([42]);
    /// let cow: CowSplinter<Vec<u8>> = CowSplinter::from_owned(splinter);
    /// assert!(cow.contains(42));
    /// ```
    pub fn from_owned(splinter: Splinter) -> Self {
        splinter.into()
    }

    /// Creates a `CowSplinter` from a [`SplinterRef`].
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, CowSplinter, PartitionWrite, PartitionRead, Encodable};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// splinter.insert(42);
    /// let splinter_ref = splinter.encode_to_splinter_ref();
    ///
    /// let cow = CowSplinter::from_ref(splinter_ref);
    /// assert!(cow.contains(42));
    /// ```
    pub fn from_ref(splinter: SplinterRef<B>) -> Self {
        splinter.into()
    }
}

impl<B: Deref<Target = [u8]>> CowSplinter<B> {
    /// Creates a `CowSplinter` from raw bytes, validating the format.
    ///
    /// This is equivalent to creating a `SplinterRef` from the bytes and wrapping
    /// it in a `CowSplinter::Ref`. All the same validation rules apply.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`SplinterRef::from_bytes`].
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, CowSplinter, PartitionWrite, PartitionRead, Encodable};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// splinter.insert(42);
    /// let bytes = splinter.encode_to_bytes();
    ///
    /// let cow = CowSplinter::from_bytes(bytes).unwrap();
    /// assert!(cow.contains(42));
    /// ```
    pub fn from_bytes(data: B) -> Result<Self, Culprit<DecodeErr>> {
        Ok(Self::Ref(SplinterRef::from_bytes(data)?))
    }

    /// Converts this `CowSplinter` into an owned [`Splinter`].
    ///
    /// If this is already an owned splinter, it returns it directly.
    /// If this is a reference, it deserializes the data into a new owned splinter.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, CowSplinter, PartitionWrite, PartitionRead, Encodable};
    ///
    /// let mut original = Splinter::EMPTY;
    /// original.insert(100);
    /// let splinter_ref = original.encode_to_splinter_ref();
    /// let cow = CowSplinter::from_ref(splinter_ref);
    ///
    /// let owned = cow.into_owned();
    /// assert_eq!(owned.cardinality(), 1);
    /// assert!(owned.contains(100));
    /// ```
    pub fn into_owned(self) -> Splinter {
        match self {
            Self::Ref(splinter_ref) => splinter_ref.decode_to_splinter(),
            Self::Owned(splinter) => splinter,
        }
    }

    /// Returns a mutable reference to the underlying [`Splinter`].
    ///
    /// This method implements the "clone-on-write" behavior: if the current value
    /// is a `Ref`, it will be converted to `Owned` by deserializing the data.
    /// Subsequent calls will return the same mutable reference without additional
    /// conversions.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, CowSplinter, PartitionWrite, PartitionRead, Encodable};
    ///
    /// let mut original = Splinter::EMPTY;
    /// original.insert(100);
    /// let splinter_ref = original.encode_to_splinter_ref();
    /// let mut cow = CowSplinter::from_ref(splinter_ref);
    ///
    /// // This triggers the clone-on-write conversion
    /// let mutable_ref = cow.to_mut();
    /// mutable_ref.insert(200);
    ///
    /// assert_eq!(cow.cardinality(), 2);
    /// ```
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
    /// Returns the serialized bytes representation of this splinter.
    ///
    /// For the `Ref` variant, this clones the underlying `Bytes` (which is efficient
    /// due to reference counting). For the `Owned` variant, this encodes the splinter
    /// to bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, CowSplinter, PartitionWrite, PartitionRead, Encodable};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// splinter.insert(42);
    /// let cow = CowSplinter::from_owned(splinter);
    ///
    /// let bytes = cow.encode_to_bytes();
    /// assert!(!bytes.is_empty());
    /// ```
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

    fn position(&self, value: u32) -> Option<usize> {
        match self {
            CowSplinter::Ref(splinter_ref) => splinter_ref.position(value),
            CowSplinter::Owned(splinter) => splinter.position(value),
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
    #[inline]
    fn insert(&mut self, value: u32) -> bool {
        self.to_mut().insert(value)
    }

    #[inline]
    fn remove(&mut self, value: u32) -> bool {
        self.to_mut().remove(value)
    }

    #[inline]
    fn remove_range<R: RangeBounds<u32>>(&mut self, values: R) {
        self.to_mut().remove_range(values)
    }
}

impl<B: Deref<Target = [u8]>, B2: Deref<Target = [u8]>> PartialEq<CowSplinter<B2>>
    for CowSplinter<B>
{
    fn eq(&self, other: &CowSplinter<B2>) -> bool {
        use CowSplinter::*;
        match (self, other) {
            (Ref(l), Ref(r)) => l == r,
            (Ref(l), Owned(r)) => l == r,
            (Owned(l), Ref(r)) => l == r,
            (Owned(l), Owned(r)) => l == r,
        }
    }
}

impl<B: Deref<Target = [u8]>> Eq for CowSplinter<B> {}
