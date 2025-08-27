use std::{fmt::Debug, marker::PhantomData};

use bitvec::{
    bitbox,
    boxed::BitBox,
    order::{BitOrder, Lsb0},
    slice::BitSlice,
    store::BitStore,
};
use bytes::BufMut;
use num::traits::AsPrimitive;

use crate::{
    codec::{Encodable, encoder::Encoder},
    count::{count_bitmap_runs, count_unique_sorted},
    level::Level,
    partition::Partition,
    segment::SplitSegment,
    traits::{Cut, Merge, PartitionRead, PartitionWrite, TruncateFrom},
};

#[derive(Clone, Eq)]
pub struct BitmapPartition<L: Level> {
    bitmap: BitBox<u64, Lsb0>,
    _marker: std::marker::PhantomData<L>,
}

impl<L: Level> BitmapPartition<L> {
    pub const ENCODED_SIZE: usize = L::MAX_LEN / 8;

    #[inline]
    pub fn count_runs(&self) -> usize {
        count_bitmap_runs(&self.bitmap)
    }

    pub fn sparsity_ratio(&self) -> f64 {
        let unique_segments = count_unique_sorted(self.iter().map(|v| v.segment()));
        unique_segments as f64 / self.cardinality() as f64
    }

    #[inline]
    pub(crate) fn as_bitbox(&self) -> &BitBox<u64, Lsb0> {
        &self.bitmap
    }
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

impl<L: Level> Encodable for BitmapPartition<L> {
    #[inline]
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>) {
        encoder.put_bitmap_partition(&self.bitmap);
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
        // SAFETY: self.bitmap can store L::MAX_LEN bits, and L::Value is
        // restricted to [0, L::MAX_LEN)
        *unsafe { self.bitmap.get_unchecked(value.as_()) }
    }

    fn rank(&self, value: L::Value) -> usize {
        let prefix = self.bitmap.get(0..=value.as_());
        prefix.unwrap().count_ones()
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        self.bitmap
            .iter_ones()
            .nth(idx)
            .map(L::Value::truncate_from)
    }

    fn last(&self) -> Option<L::Value> {
        self.bitmap.last_one().map(L::Value::truncate_from)
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

    fn remove(&mut self, value: L::Value) -> bool {
        let mut bit = self
            .bitmap
            .get_mut(value.as_())
            .expect("value out of range");
        bit.replace(false)
    }
}

impl<L: Level> PartialEq for BitmapPartition<L> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.bitmap == other.bitmap
    }
}

impl<L, T, O> PartialEq<&BitSlice<T, O>> for BitmapPartition<L>
where
    L: Level,
    T: BitStore,
    O: BitOrder,
{
    fn eq(&self, other: &&BitSlice<T, O>) -> bool {
        self.bitmap.as_bitslice() == other
    }
}

impl<L: Level> Merge for BitmapPartition<L> {
    #[inline]
    fn merge(&mut self, rhs: &Self) {
        self.bitmap |= rhs.bitmap.as_bitslice()
    }
}

impl<L, T, O> Merge<&BitSlice<T, O>> for BitmapPartition<L>
where
    L: Level,
    T: BitStore,
    O: BitOrder,
{
    #[inline]
    fn merge(&mut self, rhs: &&BitSlice<T, O>) {
        self.bitmap |= *rhs
    }
}

impl<L: Level> Cut for BitmapPartition<L> {
    type Out = Partition<L>;

    #[inline]
    fn cut(&mut self, rhs: &Self) -> Self::Out {
        self.cut(&rhs.bitmap.as_bitslice())
    }
}

impl<L, T, O> Cut<&BitSlice<T, O>> for BitmapPartition<L>
where
    L: Level,
    T: BitStore,
    O: BitOrder,
{
    type Out = Partition<L>;

    fn cut(&mut self, rhs: &&BitSlice<T, O>) -> Self::Out {
        let intersection = self.bitmap.clone() & *rhs;
        self.bitmap &= !intersection.clone();
        Partition::Bitmap(BitmapPartition {
            bitmap: intersection,
            _marker: PhantomData,
        })
    }
}
