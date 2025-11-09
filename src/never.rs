use std::ops::{BitAndAssign, BitOrAssign, BitXorAssign, RangeBounds, SubAssign};

use bytes::BufMut;

use crate::{
    Encodable, PartitionRead, PartitionWrite,
    codec::{encoder::Encoder, partition_ref::PartitionRef},
    level::Level,
    partition::Partition,
    traits::{Complement, Cut, DefaultFull, Optimizable},
};

/// The Never type is used to terminate the Level tree. It is never constructed
/// or used. Attempting to construct the Never type via Default will result in a
/// runtime exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Never {}

impl Default for Never {
    fn default() -> Self {
        unreachable!("Never::default")
    }
}

impl DefaultFull for Never {
    fn full() -> Self {
        unreachable!("Never::full")
    }
}

impl Encodable for Never {
    fn encoded_size(&self) -> usize {
        unreachable!("Never::encoded_size")
    }

    fn encode<B: BufMut>(&self, _encoder: &mut Encoder<B>) {
        unreachable!("Never::encode")
    }
}

impl<L: Level> PartitionWrite<L> for Never {
    fn insert(&mut self, _value: L::Value) -> bool {
        unreachable!("Never::insert")
    }

    fn remove(&mut self, _value: L::Value) -> bool {
        unreachable!("Never::remove")
    }

    fn remove_range<R: RangeBounds<L::Value>>(&mut self, _values: R) {
        unreachable!("Never::remove")
    }
}

impl<L: Level> PartitionRead<L> for Never {
    fn cardinality(&self) -> usize {
        unreachable!("Never::cardinality")
    }

    fn is_empty(&self) -> bool {
        unreachable!("Never::is_empty")
    }

    fn contains(&self, _value: L::Value) -> bool {
        unreachable!("Never::contains")
    }

    fn position(&self, _value: L::Value) -> Option<usize> {
        unreachable!("Never::position")
    }

    fn rank(&self, _value: L::Value) -> usize {
        unreachable!("Never::rank")
    }

    fn select(&self, _idx: usize) -> Option<L::Value> {
        unreachable!("Never::select")
    }

    fn last(&self) -> Option<L::Value> {
        unreachable!("Never::last")
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        unreachable!("Never::iter");
        #[allow(unreachable_code)]
        std::iter::empty()
    }

    fn contains_all<R: RangeBounds<L::Value>>(&self, _values: R) -> bool {
        unreachable!("Never::contains_all")
    }

    fn contains_any<R: RangeBounds<L::Value>>(&self, _values: R) -> bool {
        unreachable!("Never::contains_any")
    }
}

impl Optimizable for Never {
    fn optimize(&mut self) {
        unreachable!("Never::optimize")
    }
}

impl Level for Never {
    const DEBUG_NAME: &'static str = "Never";

    type LevelDown = Never;
    type Down = Never;
    type Value = u8;
    type ValueUnaligned = u8;

    const BITS: usize = 8;
}

impl<L: Level> PartialEq<PartitionRef<'_, L>> for Never {
    fn eq(&self, _other: &PartitionRef<'_, L>) -> bool {
        unreachable!("Never::eq")
    }
}

impl BitOrAssign<&Never> for Never {
    fn bitor_assign(&mut self, _rhs: &Never) {
        unreachable!("Never::bitor_assign")
    }
}

impl<L: Level> BitOrAssign<&PartitionRef<'_, L>> for Never {
    fn bitor_assign(&mut self, _rhs: &PartitionRef<'_, L>) {
        unreachable!("Never::bitor_assign")
    }
}

impl BitAndAssign<&Never> for Never {
    fn bitand_assign(&mut self, _rhs: &Never) {
        unreachable!("Never::bitand_assign")
    }
}

impl<L: Level> BitAndAssign<&PartitionRef<'_, L>> for Never {
    fn bitand_assign(&mut self, _rhs: &PartitionRef<'_, L>) {
        unreachable!("Never::bitand_assign")
    }
}

impl BitXorAssign<&Never> for Never {
    fn bitxor_assign(&mut self, _rhs: &Never) {
        unreachable!("Never::bitxor_assign")
    }
}

impl<L: Level> BitXorAssign<&PartitionRef<'_, L>> for Never {
    fn bitxor_assign(&mut self, _rhs: &PartitionRef<'_, L>) {
        unreachable!("Never::bitxor_assign")
    }
}

impl SubAssign<&Never> for Never {
    fn sub_assign(&mut self, _rhs: &Never) {
        unreachable!("Never::sub_assign")
    }
}

impl<L: Level> SubAssign<&PartitionRef<'_, L>> for Never {
    fn sub_assign(&mut self, _rhs: &PartitionRef<'_, L>) {
        unreachable!("Never::sub_assign")
    }
}

impl<L: Level> From<&PartitionRef<'_, L>> for Never {
    fn from(_value: &PartitionRef<'_, L>) -> Self {
        unreachable!("Never::from")
    }
}

impl<L: Level> From<Partition<L>> for Never {
    fn from(_value: Partition<L>) -> Self {
        unreachable!("Never::from")
    }
}

impl Cut for Never {
    type Out = Never;
    fn cut(&mut self, _rhs: &Self) -> Self::Out {
        unreachable!("Never::cut")
    }
}

impl<L: Level> Cut<PartitionRef<'_, L>> for Never {
    type Out = Never;
    fn cut(&mut self, _rhs: &PartitionRef<'_, L>) -> Self::Out {
        unreachable!("Never::cut")
    }
}

impl Complement for Never {
    fn complement(&mut self) {
        unreachable!("Never::complement")
    }
}

impl<L> Extend<L> for Never {
    fn extend<T: IntoIterator<Item = L>>(&mut self, _iter: T) {
        unreachable!()
    }
}
