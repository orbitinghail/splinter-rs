use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{BitAndAssign, BitOrAssign, BitXorAssign, RangeBounds, SubAssign},
};

use bitvec::{
    bitbox,
    boxed::BitBox,
    order::{BitOrder, Lsb0},
    slice::BitSlice,
    store::BitStore,
    vec::BitVec,
};
use bytes::BufMut;
use num::traits::AsPrimitive;

use crate::{
    codec::{Encodable, encoder::Encoder},
    count::count_bitmap_runs,
    level::Level,
    partition::Partition,
    segment::Segment,
    traits::{Complement, Cut, PartitionRead, PartitionWrite, TruncateFrom},
    util::RangeExt,
};

#[derive(Clone, Eq)]
pub struct BitmapPartition<L: Level> {
    bitmap: BitBox<u64, Lsb0>,
    _marker: std::marker::PhantomData<L>,
}

impl<L: Level> BitmapPartition<L> {
    /// The encoded size of a `BitmapPartition`
    pub const ENCODED_SIZE: usize = L::MAX_LEN / 8;

    /// The number of bits associated with each `Segment` contained by this
    /// `BitmapPartition`
    pub const SEGMENT_SIZE: usize = L::MAX_LEN / 256;

    #[inline]
    pub fn count_runs(&self) -> usize {
        count_bitmap_runs(&self.bitmap)
    }

    #[inline]
    pub(crate) fn as_bitbox(&self) -> &BitBox<u64, Lsb0> {
        &self.bitmap
    }

    /// Count the number of segments in the bitmap
    pub(crate) fn segments(&self) -> usize {
        let mut count = 0;
        for (_, segment) in self.iter_segments() {
            if segment.any() {
                count += 1
            }
        }
        count
    }

    /// iterate over the unique segments in the bitmap along with their
    /// corresponding bitslice
    pub(crate) fn iter_segments(&self) -> impl Iterator<Item = (Segment, &BitSlice<u64, Lsb0>)> {
        let chunks = self.bitmap.chunks_exact(Self::SEGMENT_SIZE);
        assert!(
            chunks.remainder().is_empty(),
            "BUG: bitmap length is not a multiple of 256"
        );

        (0..=255).zip(chunks.into_iter())
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

    fn position(&self, value: L::Value) -> Option<usize> {
        self.contains(value).then(|| {
            let prefix = self.bitmap.get(0..value.as_());
            prefix.unwrap().count_ones()
        })
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

    fn remove_range<R: RangeBounds<L::Value>>(&mut self, values: R) {
        if let Some(range) = values.try_into_inclusive() {
            let range = (*range.start()).as_()..=(*range.end()).as_();
            let slice = self.bitmap.get_mut(range).unwrap();
            slice.fill(false)
        }
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

impl<L: Level> BitOrAssign<&BitmapPartition<L>> for BitmapPartition<L> {
    #[inline]
    fn bitor_assign(&mut self, rhs: &BitmapPartition<L>) {
        self.bitmap |= &rhs.bitmap
    }
}

impl<L, T, O> BitOrAssign<&BitSlice<T, O>> for BitmapPartition<L>
where
    L: Level,
    T: BitStore,
    O: BitOrder,
{
    #[inline]
    fn bitor_assign(&mut self, rhs: &BitSlice<T, O>) {
        self.bitmap |= rhs
    }
}

impl<L: Level> BitAndAssign<&BitmapPartition<L>> for BitmapPartition<L> {
    #[inline]
    fn bitand_assign(&mut self, rhs: &BitmapPartition<L>) {
        self.bitmap &= &rhs.bitmap
    }
}

impl<L, T, O> BitAndAssign<&BitSlice<T, O>> for BitmapPartition<L>
where
    L: Level,
    T: BitStore,
    O: BitOrder,
{
    #[inline]
    fn bitand_assign(&mut self, rhs: &BitSlice<T, O>) {
        self.bitmap &= rhs
    }
}

impl<L: Level> BitXorAssign<&BitmapPartition<L>> for BitmapPartition<L> {
    #[inline]
    fn bitxor_assign(&mut self, rhs: &BitmapPartition<L>) {
        self.bitmap ^= &rhs.bitmap
    }
}

impl<L, T, O> BitXorAssign<&BitSlice<T, O>> for BitmapPartition<L>
where
    L: Level,
    T: BitStore,
    O: BitOrder,
{
    #[inline]
    fn bitxor_assign(&mut self, rhs: &BitSlice<T, O>) {
        self.bitmap ^= rhs
    }
}

impl<L: Level> SubAssign<&BitmapPartition<L>> for BitmapPartition<L> {
    #[inline]
    fn sub_assign(&mut self, rhs: &BitmapPartition<L>) {
        self.bitmap &= !rhs.bitmap.clone();
    }
}

impl<L, T, O> SubAssign<&BitSlice<T, O>> for BitmapPartition<L>
where
    L: Level,
    T: BitStore,
    O: BitOrder,
{
    #[inline]
    fn sub_assign(&mut self, rhs: &BitSlice<T, O>) {
        self.bitmap &= (!BitBox::from_bitslice(rhs)).as_bitslice()
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

impl<L: Level> Complement for BitmapPartition<L> {
    fn complement(&mut self) {
        for elem in self.bitmap.as_raw_mut_slice().iter_mut() {
            elem.store_value(!elem.load_value());
        }
    }
}

impl<L, T, O> From<&BitSlice<T, O>> for BitmapPartition<L>
where
    L: Level,
    T: BitStore,
    O: BitOrder,
{
    fn from(value: &BitSlice<T, O>) -> Self {
        let mut bitvec = BitVec::new();
        bitvec.extend_from_bitslice(value);
        Self {
            bitmap: bitvec.into_boxed_bitslice(),
            _marker: PhantomData,
        }
    }
}

impl<L: Level> Extend<L::Value> for BitmapPartition<L> {
    #[inline]
    fn extend<T: IntoIterator<Item = L::Value>>(&mut self, iter: T) {
        for value in iter {
            self.bitmap.set(value.as_(), true);
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use itertools::Itertools;
    use proptest::proptest;

    use crate::{
        level::{Block, Low},
        partition::{Partition, bitmap::BitmapPartition},
        testutil::{test_partition_read, test_partition_write},
    };

    #[test]
    fn test_bitmap_write() {
        let mut partition = BitmapPartition::<Low>::from_iter(0..=16384);
        test_partition_write(&mut partition);
    }

    #[test]
    fn test_bitmap_write_2() {
        let mut partition = Partition::Bitmap(BitmapPartition::<Low>::from_iter(0..=4024));
        test_partition_write(&mut partition);
    }

    proptest! {
        #[test]
        fn test_bitmap_small_read_proptest(set: HashSet<u8>) {
            let expected = set.iter().copied().sorted().collect_vec();
            let partition = BitmapPartition::<Block>::from_iter(set);
            test_partition_read(&partition, &expected);
        }
    }
}
