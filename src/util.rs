use std::iter::Peekable;

use bytes::BufMut;
use itertools::PeekingNext;

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

    /// Returns the serialized size of the container.
    fn serialized_size(&self) -> usize;
}

#[macro_export]
macro_rules! MultiIter {
    ($type:ident, $($name:ident),+) => {
        pub(crate) enum $type<$($name),+> {
            $($name($name)),+
        }

        impl<
            T, $($name: Iterator<Item=T>),+
        > Iterator for $type<$($name),+>
        {
            type Item = T;

            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    $(Self::$name(iter) => iter.next(),)+
                }
            }
        }
    };
}

pub fn find_next_sorted<I, T>(iter: &mut Peekable<I>, needle: &T) -> Option<T>
where
    I: Iterator<Item = T>,
    T: PartialOrd + PartialEq,
{
    // advance the iterator until either:
    // 1. we find the needle
    // 2. we find a value larger than the needle
    //
    while let Some(next) = iter.next_if(|v| v <= needle) {
        if &next == needle {
            return Some(next);
        }
    }
    None
}
