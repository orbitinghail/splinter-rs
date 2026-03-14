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
    cardinality: usize,
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

        (0..=255).zip(chunks)
    }
}

impl<L: Level> Default for BitmapPartition<L> {
    fn default() -> Self {
        Self {
            bitmap: bitbox![u64, Lsb0; 0; L::MAX_LEN],
            cardinality: 0,
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
        let cardinality = bitmap.count_ones();
        BitmapPartition {
            bitmap,
            cardinality,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<L: Level> PartitionRead<L> for BitmapPartition<L> {
    fn cardinality(&self) -> usize {
        debug_assert_eq!(
            self.bitmap.count_ones(),
            self.cardinality,
            "BUG: BitmapPartition cardinality not in sync"
        );
        self.cardinality
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

    fn contains_all<R: RangeBounds<L::Value>>(&self, values: R) -> bool {
        if let Some(range) = values.try_into_inclusive() {
            let range = (*range.start()).as_()..=(*range.end()).as_();
            let slice = self.bitmap.get(range).unwrap();
            slice.all()
        } else {
            // empty range is trivially contained
            true
        }
    }

    fn contains_any<R: RangeBounds<L::Value>>(&self, values: R) -> bool {
        if let Some(range) = values.try_into_inclusive() {
            let range = (*range.start()).as_()..=(*range.end()).as_();
            let slice = self.bitmap.get(range).unwrap();
            slice.any()
        } else {
            // empty range has no intersection
            false
        }
    }
}

impl<L: Level> PartitionWrite<L> for BitmapPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let mut bit = self
            .bitmap
            .get_mut(value.as_())
            .expect("value out of range");
        let was_absent = !bit.replace(true);
        if was_absent {
            self.cardinality += 1;
        }
        was_absent
    }

    fn remove(&mut self, value: L::Value) -> bool {
        let mut bit = self
            .bitmap
            .get_mut(value.as_())
            .expect("value out of range");
        let was_present = bit.replace(false);
        if was_present {
            self.cardinality -= 1;
        }
        was_present
    }

    fn remove_range<R: RangeBounds<L::Value>>(&mut self, values: R) {
        if let Some(range) = values.try_into_inclusive() {
            let range = (*range.start()).as_()..=(*range.end()).as_();
            let slice = self.bitmap.get_mut(range).unwrap();
            slice.fill(false);
            self.cardinality = self.bitmap.count_ones();
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
        self.bitmap |= &rhs.bitmap;
        self.cardinality = self.bitmap.count_ones();
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
        self.bitmap |= rhs;
        self.cardinality = self.bitmap.count_ones();
    }
}

impl<L: Level> BitAndAssign<&BitmapPartition<L>> for BitmapPartition<L> {
    #[inline]
    fn bitand_assign(&mut self, rhs: &BitmapPartition<L>) {
        self.bitmap &= &rhs.bitmap;
        self.cardinality = self.bitmap.count_ones();
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
        self.bitmap &= rhs;
        self.cardinality = self.bitmap.count_ones();
    }
}

impl<L: Level> BitXorAssign<&BitmapPartition<L>> for BitmapPartition<L> {
    #[inline]
    fn bitxor_assign(&mut self, rhs: &BitmapPartition<L>) {
        self.bitmap ^= &rhs.bitmap;
        self.cardinality = self.bitmap.count_ones();
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
        self.bitmap ^= rhs;
        self.cardinality = self.bitmap.count_ones();
    }
}

impl<L: Level> SubAssign<&BitmapPartition<L>> for BitmapPartition<L> {
    #[inline]
    fn sub_assign(&mut self, rhs: &BitmapPartition<L>) {
        self.bitmap &= !rhs.bitmap.clone();
        self.cardinality = self.bitmap.count_ones();
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
        self.bitmap &= (!BitBox::from_bitslice(rhs)).as_bitslice();
        self.cardinality = self.bitmap.count_ones();
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
        let intersection_cardinality = intersection.count_ones();
        self.cardinality = self.bitmap.count_ones();
        Partition::Bitmap(BitmapPartition {
            bitmap: intersection,
            cardinality: intersection_cardinality,
            _marker: PhantomData,
        })
    }
}

impl<L: Level> Complement for BitmapPartition<L> {
    fn complement(&mut self) {
        for elem in self.bitmap.as_raw_mut_slice().iter_mut() {
            elem.store_value(!elem.load_value());
        }
        self.cardinality = L::MAX_LEN - self.cardinality;
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
        let bitmap = bitvec.into_boxed_bitslice();
        let cardinality = bitmap.count_ones();
        Self {
            bitmap,
            cardinality,
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
        self.cardinality = self.bitmap.count_ones()
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeSet, HashSet};

    use hegel::generators;
    use itertools::Itertools;
    use proptest::proptest;

    use crate::{
        count::count_runs_sorted,
        level::{Block, Low},
        partition::{Partition, bitmap::BitmapPartition},
        testutil::{test_partition_read, test_partition_write},
        traits::{Complement, Cut, PartitionRead, PartitionWrite},
    };

    fn sorted_unique_u16(values: Vec<u16>) -> Vec<u16> {
        values.into_iter().collect::<BTreeSet<_>>().into_iter().collect()
    }

    fn sorted_unique_u8(values: Vec<u8>) -> Vec<u8> {
        values.into_iter().collect::<BTreeSet<_>>().into_iter().collect()
    }

    fn count_segments(values: &[u16]) -> usize {
        values.iter().map(|&v| (v >> 8) as u8).collect::<BTreeSet<_>>().len()
    }

    fn position(values: &[u16], needle: u16) -> Option<usize> {
        values.binary_search(&needle).ok()
    }

    fn rank(values: &[u16], needle: u16) -> usize {
        values.partition_point(|&value| value <= needle)
    }

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

    #[test]
    fn test_bitmap_direct_bitops_match_model() {
        let lhs_values = [1u16, 2, 3, 255, 256, 1024];
        let rhs_values = [2u16, 4, 255, 300, 1024, 4096];

        let lhs_model = lhs_values.into_iter().collect::<BTreeSet<_>>();
        let rhs_model = rhs_values.into_iter().collect::<BTreeSet<_>>();

        let lhs = BitmapPartition::<Low>::from_iter(lhs_model.iter().copied());
        let rhs = BitmapPartition::<Low>::from_iter(rhs_model.iter().copied());

        assert!(lhs == lhs.as_bitbox().as_bitslice());

        let mut union = lhs.clone();
        union |= &rhs;
        assert_eq!(
            union.iter().collect::<BTreeSet<_>>(),
            lhs_model.union(&rhs_model).copied().collect()
        );

        let mut union_bits = lhs.clone();
        union_bits |= rhs.as_bitbox().as_bitslice();
        assert_eq!(union_bits.iter().collect::<BTreeSet<_>>(), union.iter().collect());

        let mut intersection = lhs.clone();
        intersection &= &rhs;
        assert_eq!(
            intersection.iter().collect::<BTreeSet<_>>(),
            lhs_model.intersection(&rhs_model).copied().collect()
        );

        let mut intersection_bits = lhs.clone();
        intersection_bits &= rhs.as_bitbox().as_bitslice();
        assert_eq!(
            intersection_bits.iter().collect::<BTreeSet<_>>(),
            intersection.iter().collect()
        );

        let mut xor = lhs.clone();
        xor ^= &rhs;
        assert_eq!(
            xor.iter().collect::<BTreeSet<_>>(),
            lhs_model.symmetric_difference(&rhs_model).copied().collect()
        );

        let mut xor_bits = lhs.clone();
        xor_bits ^= rhs.as_bitbox().as_bitslice();
        assert_eq!(xor_bits.iter().collect::<BTreeSet<_>>(), xor.iter().collect());

        let mut difference = lhs.clone();
        difference -= &rhs;
        assert_eq!(
            difference.iter().collect::<BTreeSet<_>>(),
            lhs_model.difference(&rhs_model).copied().collect()
        );

        let mut difference_bits = lhs.clone();
        difference_bits -= rhs.as_bitbox().as_bitslice();
        assert_eq!(
            difference_bits.iter().collect::<BTreeSet<_>>(),
            difference.iter().collect()
        );
    }

    proptest! {
        #[test]
        fn test_bitmap_small_read_proptest(set: HashSet<u8>) {
            let expected = set.iter().copied().sorted().collect_vec();
            let partition = BitmapPartition::<Block>::from_iter(set);
            test_partition_read(&partition, &expected);
        }
    }

    #[hegel::test]
    fn test_bitmap_low_matches_model(tc: hegel::TestCase) {
        let values = tc.draw(
            generators::vecs(generators::integers::<u16>())
                .unique()
                .max_size(512),
        );
        let probes = tc.draw(generators::vecs(generators::integers::<u16>()).max_size(64));

        let expected = sorted_unique_u16(values);
        let partition = BitmapPartition::<Low>::from_iter(expected.iter().copied());

        assert_eq!(partition.cardinality(), expected.len());
        assert_eq!(partition.iter().collect_vec(), expected);
        assert_eq!(partition.last(), expected.last().copied());
        assert_eq!(partition.count_runs(), count_runs_sorted(expected.iter().copied()));
        assert_eq!(partition.segments(), count_segments(&expected));

        for probe in probes {
            assert_eq!(partition.contains(probe), expected.binary_search(&probe).is_ok());
            assert_eq!(partition.position(probe), position(&expected, probe));
            assert_eq!(partition.rank(probe), rank(&expected, probe));
        }

        for (idx, &value) in expected.iter().enumerate() {
            assert_eq!(partition.select(idx), Some(value));
        }
        assert_eq!(partition.select(expected.len()), None);
    }

    #[hegel::test]
    fn test_bitmap_low_range_ops_match_model(tc: hegel::TestCase) {
        let values = tc.draw(
            generators::vecs(generators::integers::<u16>())
                .unique()
                .max_size(512),
        );
        let mut start = tc.draw(generators::integers::<u16>());
        let mut end = tc.draw(generators::integers::<u16>());
        if start > end {
            (start, end) = (end, start);
        }

        let expected = sorted_unique_u16(values);
        let expected_all = (start..=end).all(|value| expected.binary_search(&value).is_ok());
        let expected_any = (start..=end).any(|value| expected.binary_search(&value).is_ok());

        let mut partition = BitmapPartition::<Low>::from_iter(expected.iter().copied());
        assert_eq!(partition.contains_all(start..=end), expected_all);
        assert_eq!(partition.contains_any(start..=end), expected_any);

        partition.remove_range(start..=end);
        let after = expected
            .into_iter()
            .filter(|&value| !(start..=end).contains(&value))
            .collect_vec();

        assert_eq!(partition.iter().collect_vec(), after);
        assert_eq!(partition.cardinality(), after.len());
    }

    #[hegel::test]
    fn test_bitmap_low_cut_matches_model(tc: hegel::TestCase) {
        let lhs_values = tc.draw(
            generators::vecs(generators::integers::<u16>())
                .unique()
                .max_size(512),
        );
        let rhs_values = tc.draw(
            generators::vecs(generators::integers::<u16>())
                .unique()
                .max_size(512),
        );

        let lhs_model = sorted_unique_u16(lhs_values);
        let rhs_model = sorted_unique_u16(rhs_values);

        let mut lhs = BitmapPartition::<Low>::from_iter(lhs_model.iter().copied());
        let rhs = BitmapPartition::<Low>::from_iter(rhs_model.iter().copied());
        let cut = lhs.cut(&rhs);

        let expected_cut = lhs_model
            .iter()
            .copied()
            .filter(|value| rhs_model.binary_search(value).is_ok())
            .collect_vec();
        let expected_remaining = lhs_model
            .iter()
            .copied()
            .filter(|value| rhs_model.binary_search(value).is_err())
            .collect_vec();

        assert_eq!(cut.iter().collect_vec(), expected_cut);
        assert_eq!(lhs.iter().collect_vec(), expected_remaining);
        assert_eq!(lhs.cardinality(), expected_remaining.len());
    }

    #[hegel::test]
    fn test_bitmap_block_complement_matches_model(tc: hegel::TestCase) {
        let values = tc.draw(
            generators::vecs(generators::integers::<u8>())
                .unique()
                .max_size(256),
        );

        let values = sorted_unique_u8(values);
        let mut partition = BitmapPartition::<Block>::from_iter(values.iter().copied());
        partition.complement();

        let expected = (u8::MIN..=u8::MAX)
            .filter(|value| values.binary_search(value).is_err())
            .collect_vec();

        assert_eq!(partition.iter().collect_vec(), expected);
        assert_eq!(partition.cardinality(), expected.len());
    }
}
