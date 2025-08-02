use crate::splinterv2::level::Level;
use num::cast::AsPrimitive;
use u24::u24;

pub trait PartitionRead<L: Level> {
    /// the total number of values accessible via this partition.
    fn cardinality(&self) -> usize;

    /// returns true if this partition is empty
    fn is_empty(&self) -> bool;

    /// returns true if this partition contains the given value
    fn contains(&self, value: L::Value) -> bool;

    /// returns an iterator over all values in this partition
    fn iter(&self) -> impl Iterator<Item = L::Value>;
}

pub trait PartitionWrite<L: Level> {
    /// Inserts the value into the partition unless it already exists.
    /// Returns `true` if the insertion occurred, otherwise `false`.
    fn insert(&mut self, value: L::Value) -> bool;
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

#[doc(hidden)]
pub trait Optimizable<T> {
    fn shallow_optimize(&self) -> Option<T>;

    fn optimize_children(&mut self) {}
}
