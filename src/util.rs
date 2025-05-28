use bytes::BufMut;

pub trait FromSuffix<'a> {
    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self;
}

/// A custom version of `ToOwned` to get around a conflict with the standard
/// library's `impl<T> ToOwned for T where T: Clone` and `BlockRef`.
pub trait CopyToOwned {
    type Owned;

    fn copy_to_owned(&self) -> Self::Owned;
}

pub trait SerializeContainer {
    /// Returns `true` if the object should be serialized.
    fn should_serialize(&self) -> bool;

    /// Serializes the object into the given buffer.
    ///
    /// Returns the cardinality of the object and number of bytes written.
    fn serialize<B: BufMut>(&self, out: &mut B) -> (usize, usize);
}

/// A trait for types that can report how many values they contain.
pub trait Cardinality {
    /// Returns the total number of stored values.
    fn cardinality(&self) -> usize;
}
