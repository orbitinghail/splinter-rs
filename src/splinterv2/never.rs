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
        unreachable!("bug: attempted to construct Never")
    }
}

impl Encodable for Never {
    fn encoded_size(&self) -> usize {
        unreachable!()
    }
}

impl<L: Level> PartitionWrite<L> for Never {
    fn insert(&mut self, _value: L::Value) -> bool {
        unreachable!("invalid splinter")
    }
}

impl<L: Level> PartitionRead<L> for Never {
    fn cardinality(&self) -> usize {
        unreachable!("invalid splinter")
    }

    fn is_empty(&self) -> bool {
        unreachable!("invalid splinter")
    }

    fn contains(&self, _value: L::Value) -> bool {
        unreachable!("invalid splinter")
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        unreachable!("invalid splinter");
        #[allow(unreachable_code)]
        std::iter::empty()
    }
}

impl Optimizable<Never> for Never {
    fn shallow_optimize(&self) -> Option<Never> {
        unreachable!("invalid splinter")
    }

    fn optimize_children(&mut self) {
        unreachable!("invalid splinter")
    }
}

impl Level for Never {
    const DEBUG_NAME: &'static str = "Never";

    type Offset = ();
    type LevelDown = Never;
    type Down = Never;
    type Value = u8;
    const BITS: usize = 8;
}
