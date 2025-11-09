use std::ops::RangeBounds;

use crate::level::Level;
use num::cast::AsPrimitive;
use u24::u24;

pub trait PartitionRead<L: Level> {
    /// the total number of values accessible via this partition.
    fn cardinality(&self) -> usize;

    /// returns true if this partition is empty
    fn is_empty(&self) -> bool;

    /// returns true if this partition contains the given value
    fn contains(&self, value: L::Value) -> bool;

    /// returns the 0-based position of the value in the partition if it exists,
    /// otherwise returns None.
    fn position(&self, value: L::Value) -> Option<usize>;

    /// returns the number of values contained in this partition up to and
    /// including the value.
    fn rank(&self, value: L::Value) -> usize;

    /// returns the value at position `idx`.
    fn select(&self, idx: usize) -> Option<L::Value>;

    /// returns the last value in the partition
    fn last(&self) -> Option<L::Value>;

    /// returns an iterator over all values in this partition
    fn iter(&self) -> impl Iterator<Item = L::Value>;

    /// returns an iterator over all values in this partition restricted by the provided range.
    fn range<R>(&self, range: R) -> impl Iterator<Item = L::Value>
    where
        R: RangeBounds<L::Value> + Clone,
    {
        let r2 = range.clone();
        self.iter()
            .skip_while(move |s| !range.contains(s))
            .take_while(move |s| r2.contains(s))
    }

    /// returns true if this partition contains all values in the given range
    fn contains_all<R: RangeBounds<L::Value>>(&self, values: R) -> bool;

    /// returns true if this partition has a non-empty intersection with the given range
    fn contains_any<R: RangeBounds<L::Value>>(&self, values: R) -> bool;
}

pub trait PartitionWrite<L: Level> {
    /// Inserts the value into the partition unless it already exists.
    /// Returns `true` if the insertion occurred, `false` otherwise.
    fn insert(&mut self, value: L::Value) -> bool;

    /// Removes the value from the partition if it exists.
    /// Returns `true` if the removal occurred, `false` otherwise.
    fn remove(&mut self, value: L::Value) -> bool;

    /// Removes a range of values from the partition.
    fn remove_range<R: RangeBounds<L::Value>>(&mut self, values: R);
}

#[doc(hidden)]
pub trait TruncateFrom<T> {
    fn truncate_from(other: T) -> Self;
}

macro_rules! impl_truncate_from_usize {
    ($($ty:ty),*) => {
        $(
            impl TruncateFrom<usize> for $ty {
                #[inline(always)]
                fn truncate_from(other: usize) -> Self {
                    other.as_()
                }
            }
        )*
    };
}
impl_truncate_from_usize!(u32, u24, u16, u8);

pub trait Optimizable {
    /// Optimize memory usage. Should be run after batch inserts or before serialization.
    fn optimize(&mut self);
}

pub trait Cut<Rhs = Self> {
    type Out;

    /// Returns the intersection between self and rhs while removing the
    /// intersection from self
    fn cut(&mut self, rhs: &Rhs) -> Self::Out;
}

pub trait DefaultFull {
    fn full() -> Self;
}

pub trait Complement {
    // self = !self
    fn complement(&mut self);
}
