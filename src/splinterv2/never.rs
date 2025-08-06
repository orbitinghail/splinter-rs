use crate::splinterv2::{
    Encodable, PartitionRead, PartitionWrite, level::Level, traits::Optimizable,
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

impl Encodable for Never {
    fn encoded_size(&self) -> usize {
        unreachable!("Never::encoded_size")
    }

    fn encode(&self, _buf: &mut impl bytes::BufMut) {
        unreachable!("Never::encode")
    }
}

impl<L: Level> PartitionWrite<L> for Never {
    fn insert(&mut self, _value: L::Value) -> bool {
        unreachable!("Never::insert")
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

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        unreachable!("Never::iter");
        #[allow(unreachable_code)]
        std::iter::empty()
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
