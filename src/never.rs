use bytes::BufMut;

use crate::{
    Encodable, PartitionRead, PartitionWrite,
    codec::{encoder::Encoder, partition_ref::PartitionRef},
    level::Level,
    traits::{Cut, Merge, Optimizable},
};

/// The Never type is used to terminate the Level tree. It is never constructed
/// or used. Attempting to construct the Never type via Default will result in a
/// runtime exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Never {}

impl Default for Never {
    fn default() -> Self {
        if cfg!(any(debug_assertions, not(feature = "checked_never"))) {
            unreachable!("Never::default")
        } else {
            unsafe extern "C" {
                fn never() -> Never;
            }
            unsafe { never() }
        }
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

impl Merge for Never {
    fn merge(&mut self, _rhs: &Self) {
        unreachable!("Never::merge")
    }
}

impl<L: Level> Merge<PartitionRef<'_, L>> for Never {
    fn merge(&mut self, _rhs: &PartitionRef<'_, L>) {
        unreachable!("Never::merge")
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
