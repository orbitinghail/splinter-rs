use std::fmt::Debug;

use bitvec::{bitbox, boxed::BitBox, order::Lsb0};
use num::traits::AsPrimitive;

use crate::splinterv2::{
    encode::Encodable,
    level::Level,
    partition::Partition,
    traits::{Optimizable, PartitionRead, PartitionWrite, TruncateFrom},
};

#[derive(Clone, PartialEq, Eq)]
pub struct BitmapPartition<L: Level> {
    bitmap: BitBox<u64, Lsb0>,
    _marker: std::marker::PhantomData<L>,
}

impl<L: Level> Default for BitmapPartition<L> {
    fn default() -> Self {
        Self {
            bitmap: bitbox![u64, Lsb0; 0; L::MAX_LEN],
            _marker: std::marker::PhantomData,
        }
    }
}

impl<L: Level> Debug for BitmapPartition<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BitmapPartition<{}>({})",
            L::DEBUG_NAME,
            self.cardinality()
        )
    }
}

impl<L: Level> Optimizable<Partition<L>> for BitmapPartition<L> {
    fn shallow_optimize(&self) -> Option<Partition<L>> {
        (self.cardinality() == L::MAX_LEN).then_some(Partition::Full)
    }
}

impl<L: Level> Encodable for BitmapPartition<L> {
    fn encoded_size(&self) -> usize {
        L::MAX_LEN / 8
    }
}

impl<L: Level> FromIterator<L::Value> for BitmapPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let mut bitmap = bitbox![u64, Lsb0; 0; L::MAX_LEN];
        for v in iter {
            bitmap.set(v.as_(), true);
        }
        BitmapPartition {
            bitmap,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<L: Level> PartitionRead<L> for BitmapPartition<L> {
    fn cardinality(&self) -> usize {
        self.bitmap.count_ones()
    }

    fn is_empty(&self) -> bool {
        self.bitmap.not_any()
    }

    fn contains(&self, value: L::Value) -> bool {
        self.bitmap.get(value.as_()).is_some()
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.bitmap.iter_ones().map(L::Value::truncate_from)
    }
}

impl<L: Level> PartitionWrite<L> for BitmapPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let mut bit = self
            .bitmap
            .get_mut(value.as_())
            .expect("value out of range");
        !bit.replace(true)
    }
}
