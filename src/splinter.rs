use std::{fmt::Debug, ops::RangeBounds};

use bytes::Bytes;

use crate::{
    Encodable, Optimizable, SplinterRef,
    codec::{encoder::Encoder, footer::Footer},
    level::High,
    partition::Partition,
    traits::{PartitionRead, PartitionWrite},
    util::RangeExt,
};

/// A compressed bitmap optimized for small, sparse sets of 32-bit unsigned integers.
///
/// `Splinter` is the main owned data structure that can be built incrementally by inserting
/// values and then optimized for size and query performance. It uses a 256-way tree structure
/// by decomposing integers into big-endian component bytes, with nodes optimized into four
/// different storage classes: tree, vec, bitmap, and run.
///
/// For zero-copy querying of serialized data, see [`SplinterRef`].
/// For a clone-on-write wrapper, see [`crate::CowSplinter`].
///
/// # Examples
///
/// Basic usage:
///
/// ```
/// use splinter_rs::{Splinter, PartitionWrite, PartitionRead, Optimizable};
///
/// let mut splinter = Splinter::from_iter([1024, 2048, 123]);
///
/// // Check membership
/// assert!(splinter.contains(1024));
/// assert!(!splinter.contains(999));
///
/// // Get cardinality
/// assert_eq!(splinter.cardinality(), 3);
///
/// // Optimize for better compression, recommended before encoding to bytes.
/// splinter.optimize();
/// ```
///
/// Building from iterator:
///
/// ```
/// use splinter_rs::{Splinter, PartitionRead};
///
/// let values = vec![100, 200, 300, 400];
/// let splinter: Splinter = values.into_iter().collect();
///
/// assert_eq!(splinter.cardinality(), 4);
/// assert!(splinter.contains(200));
/// ```
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct Splinter(Partition<High>);

static_assertions::const_assert_eq!(std::mem::size_of::<Splinter>(), 40);

impl Splinter {
    /// An empty Splinter, suitable for usage in a const context.
    pub const EMPTY: Self = Splinter(Partition::EMPTY);

    /// A full Splinter, suitable for usage in a const context.
    pub const FULL: Self = Splinter(Partition::Full);

    /// Encodes this splinter into a [`SplinterRef`] for zero-copy querying.
    ///
    /// This method serializes the splinter data and returns a [`SplinterRef<Bytes>`]
    /// that can be used for efficient read-only operations without deserializing
    /// the underlying data structure.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionWrite, PartitionRead};
    ///
    /// let mut splinter = Splinter::from_iter([42, 1337]);
    ///
    /// let splinter_ref = splinter.encode_to_splinter_ref();
    /// assert_eq!(splinter_ref.cardinality(), 2);
    /// assert!(splinter_ref.contains(42));
    /// ```
    pub fn encode_to_splinter_ref(&self) -> SplinterRef<Bytes> {
        SplinterRef { data: self.encode_to_bytes() }
    }

    #[inline(always)]
    pub(crate) fn new(inner: Partition<High>) -> Self {
        Self(inner)
    }

    #[inline(always)]
    pub(crate) fn inner(&self) -> &Partition<High> {
        &self.0
    }

    #[inline(always)]
    pub(crate) fn inner_mut(&mut self) -> &mut Partition<High> {
        &mut self.0
    }
}

impl FromIterator<u32> for Splinter {
    fn from_iter<I: IntoIterator<Item = u32>>(iter: I) -> Self {
        Self(Partition::<High>::from_iter(iter))
    }
}

impl<R: RangeBounds<u32>> From<R> for Splinter {
    fn from(range: R) -> Self {
        if let Some(range) = range.try_into_inclusive() {
            if range.start() == &u32::MIN && range.end() == &u32::MAX {
                Self::FULL
            } else {
                Self(Partition::<High>::from(range))
            }
        } else {
            // range is empty
            Self::EMPTY
        }
    }
}

impl PartitionRead<High> for Splinter {
    /// Returns the total number of elements in this splinter.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// assert_eq!(splinter.cardinality(), 0);
    ///
    /// let splinter = Splinter::from_iter([100, 200, 300]);
    /// assert_eq!(splinter.cardinality(), 3);
    /// ```
    #[inline]
    fn cardinality(&self) -> usize {
        self.0.cardinality()
    }

    /// Returns `true` if this splinter contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// assert!(splinter.is_empty());
    ///
    /// let splinter = Splinter::from_iter([42]);
    /// assert!(!splinter.is_empty());
    /// ```
    #[inline]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns `true` if this splinter contains the specified value.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let splinter = Splinter::from_iter([42, 1337]);
    ///
    /// assert!(splinter.contains(42));
    /// assert!(splinter.contains(1337));
    /// assert!(!splinter.contains(999));
    /// ```
    #[inline]
    fn contains(&self, value: u32) -> bool {
        self.0.contains(value)
    }

    /// Returns the 0-based position of the value in this splinter if it exists.
    ///
    /// This method searches for the given value in the splinter and returns its position
    /// in the sorted sequence of all elements. If the value doesn't exist, returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let splinter = Splinter::from_iter([10, 20, 30]);
    ///
    /// assert_eq!(splinter.position(10), Some(0));
    /// assert_eq!(splinter.position(20), Some(1));
    /// assert_eq!(splinter.position(30), Some(2));
    /// assert_eq!(splinter.position(25), None); // doesn't exist
    /// ```
    #[inline]
    fn position(&self, value: u32) -> Option<usize> {
        self.0.position(value)
    }

    /// Returns the number of elements in this splinter that are less than or equal to the given value.
    ///
    /// This is also known as the "rank" of the value in the sorted sequence of all elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let splinter = Splinter::from_iter([10, 20, 30]);
    ///
    /// assert_eq!(splinter.rank(5), 0);   // No elements <= 5
    /// assert_eq!(splinter.rank(10), 1);  // One element <= 10
    /// assert_eq!(splinter.rank(25), 2);  // Two elements <= 25
    /// assert_eq!(splinter.rank(30), 3);  // Three elements <= 30
    /// assert_eq!(splinter.rank(50), 3);  // Three elements <= 50
    /// ```
    #[inline]
    fn rank(&self, value: u32) -> usize {
        self.0.rank(value)
    }

    /// Returns the element at the given index in the sorted sequence, or `None` if the index is out of bounds.
    ///
    /// The index is 0-based, so `select(0)` returns the smallest element.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let splinter = Splinter::from_iter([100, 50, 200]);
    ///
    /// assert_eq!(splinter.select(0), Some(50));   // Smallest element
    /// assert_eq!(splinter.select(1), Some(100));  // Second smallest
    /// assert_eq!(splinter.select(2), Some(200));  // Largest element
    /// assert_eq!(splinter.select(3), None);       // Out of bounds
    /// ```
    #[inline]
    fn select(&self, idx: usize) -> Option<u32> {
        self.0.select(idx)
    }

    /// Returns the largest element in this splinter, or `None` if it's empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let mut splinter = Splinter::EMPTY;
    /// assert_eq!(splinter.last(), None);
    ///
    /// let splinter = Splinter::from_iter([100, 50, 200]);
    ///
    /// assert_eq!(splinter.last(), Some(200));
    /// ```
    #[inline]
    fn last(&self) -> Option<u32> {
        self.0.last()
    }

    /// Returns an iterator over all elements in ascending order.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let splinter = Splinter::from_iter([300, 100, 200]);
    ///
    /// let values: Vec<u32> = splinter.iter().collect();
    /// assert_eq!(values, vec![100, 200, 300]);
    /// ```
    #[inline]
    fn iter(&self) -> impl Iterator<Item = u32> {
        self.0.iter()
    }

    /// Returns `true` if this splinter contains all values in the specified range.
    ///
    /// This method checks whether every value within the given range bounds is present
    /// in the splinter. An empty range is trivially contained and returns `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead};
    ///
    /// let splinter = Splinter::from_iter([10, 11, 12, 13, 14, 15, 100]);
    ///
    /// // Check if range is fully contained
    /// assert!(splinter.contains_all(10..=15));
    /// assert!(splinter.contains_all(11..=14));
    ///
    /// // Missing values mean the range is not fully contained
    /// assert!(!splinter.contains_all(10..=16));  // 16 is missing
    /// assert!(!splinter.contains_all(9..=15));   // 9 is missing
    ///
    /// // Empty ranges are trivially contained
    /// assert!(splinter.contains_all(50..50));
    /// ```
    #[inline]
    fn contains_all<R: RangeBounds<u32>>(&self, values: R) -> bool {
        self.0.contains_all(values)
    }

    /// Returns `true` if this splinter has a non-empty intersection with the specified range.
    ///
    /// This method checks whether any value within the given range is present
    /// in the splinter. Returns `false` for empty ranges.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead};
    ///
    /// let splinter = Splinter::from_iter([10, 20, 30]);
    ///
    /// // Check for any overlap
    /// assert!(splinter.contains_any(10..=15));   // Contains 10
    /// assert!(splinter.contains_any(5..=10));    // Contains 10
    /// assert!(splinter.contains_any(25..=35));   // Contains 30
    ///
    /// // No overlap
    /// assert!(!splinter.contains_any(0..=9));    // No values in range
    /// assert!(!splinter.contains_any(40..=50));  // No values in range
    ///
    /// // Empty ranges have no intersection
    /// assert!(!splinter.contains_any(50..50));
    /// ```
    #[inline]
    fn contains_any<R: RangeBounds<u32>>(&self, values: R) -> bool {
        self.0.contains_any(values)
    }
}

impl PartitionWrite<High> for Splinter {
    /// Inserts a value into this splinter.
    ///
    /// Returns `true` if the value was newly inserted, or `false` if it was already present.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionWrite, PartitionRead};
    ///
    /// let mut splinter = Splinter::EMPTY;
    ///
    /// // First insertion returns true
    /// assert!(splinter.insert(42));
    /// assert_eq!(splinter.cardinality(), 1);
    ///
    /// // Second insertion of same value returns false
    /// assert!(!splinter.insert(42));
    /// assert_eq!(splinter.cardinality(), 1);
    ///
    /// // Different value returns true
    /// assert!(splinter.insert(100));
    /// assert_eq!(splinter.cardinality(), 2);
    /// ```
    #[inline]
    fn insert(&mut self, value: u32) -> bool {
        self.0.insert(value)
    }

    /// Removes a value from this splinter.
    ///
    /// Returns `true` if the value was present and removed, or `false` if it was not present.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionWrite, PartitionRead};
    ///
    /// let mut splinter = Splinter::from_iter([42, 100]);
    /// assert_eq!(splinter.cardinality(), 2);
    ///
    /// // Remove existing value
    /// assert!(splinter.remove(42));
    /// assert_eq!(splinter.cardinality(), 1);
    /// assert!(!splinter.contains(42));
    /// assert!(splinter.contains(100));
    ///
    /// // Remove non-existent value
    /// assert!(!splinter.remove(999));
    /// assert_eq!(splinter.cardinality(), 1);
    /// ```
    #[inline]
    fn remove(&mut self, value: u32) -> bool {
        self.0.remove(value)
    }

    /// Removes a range of values from this splinter.
    ///
    /// This method removes all values that fall within the specified range bounds.
    /// The range can be inclusive, exclusive, or half-bounded using standard Rust range syntax.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, PartitionRead, PartitionWrite};
    ///
    /// let mut splinter = Splinter::from_iter(1..=10);
    ///
    /// // Remove values 3 through 7 (inclusive)
    /// splinter.remove_range(3..=7);
    /// assert!(!splinter.contains(5));
    /// assert!(splinter.contains(2));
    /// assert!(splinter.contains(8));
    ///
    /// // Remove from 9 onwards
    /// splinter.remove_range(9..);
    /// assert!(!splinter.contains(9));
    /// assert!(!splinter.contains(10));
    /// assert!(splinter.contains(8));
    /// ```
    #[inline]
    fn remove_range<R: RangeBounds<u32>>(&mut self, values: R) {
        self.0.remove_range(values);
    }
}

impl Encodable for Splinter {
    fn encoded_size(&self) -> usize {
        self.0.encoded_size() + std::mem::size_of::<Footer>()
    }

    fn encode<B: bytes::BufMut>(&self, encoder: &mut Encoder<B>) {
        self.0.encode(encoder);
        encoder.write_footer();
    }
}

impl Optimizable for Splinter {
    #[inline]
    fn optimize(&mut self) {
        self.0.optimize();
    }
}

impl Extend<u32> for Splinter {
    #[inline]
    fn extend<T: IntoIterator<Item = u32>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use crate::{
        codec::Encodable,
        level::{Level, Low},
        testutil::{SetGen, mksplinter, ratio_to_marks, test_partition_read, test_partition_write},
        traits::Optimizable,
    };
    use itertools::{Itertools, assert_equal};
    use proptest::{
        collection::{hash_set, vec},
        proptest,
    };
    use rand::{SeedableRng, seq::index};
    use roaring::RoaringBitmap;

    #[test]
    fn test_sanity() {
        let mut splinter = Splinter::EMPTY;

        assert!(splinter.insert(1));
        assert!(!splinter.insert(1));
        assert!(splinter.contains(1));

        let values = [1024, 123, 16384];
        for v in values {
            assert!(splinter.insert(v));
            assert!(splinter.contains(v));
            assert!(!splinter.contains(v + 1));
        }

        for i in 0..8192 + 10 {
            splinter.insert(i);
        }

        splinter.optimize();

        dbg!(&splinter);

        let expected = splinter.iter().collect_vec();
        test_partition_read(&splinter, &expected);
        test_partition_write(&mut splinter);
    }

    #[test]
    fn test_wat() {
        let mut set_gen = SetGen::new(0xDEAD_BEEF);
        let set = set_gen.random_max(64, 4096);
        let baseline_size = set.len() * 4;

        let mut splinter = Splinter::from_iter(set.iter().copied());
        splinter.optimize();

        dbg!(&splinter, splinter.encoded_size(), baseline_size, set.len());
        itertools::assert_equal(splinter.iter(), set.into_iter());
    }

    #[test]
    fn test_splinter_write() {
        let mut splinter = Splinter::from_iter(0u32..16384);
        test_partition_write(&mut splinter);
    }

    #[test]
    fn test_splinter_optimize_growth() {
        let mut splinter = Splinter::EMPTY;
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xdeadbeef);
        let set = index::sample(&mut rng, Low::MAX_LEN, 8);
        dbg!(&splinter);
        for i in set {
            splinter.insert(i as u32);
            dbg!(&splinter);
        }
    }

    #[test]
    fn test_splinter_from_range() {
        let splinter = Splinter::from(..);
        assert_eq!(splinter.cardinality(), (u32::MAX as usize) + 1);

        let mut splinter = Splinter::from(1..);
        assert_eq!(splinter.cardinality(), u32::MAX as usize);

        splinter.remove(1024);
        assert_eq!(splinter.cardinality(), (u32::MAX as usize) - 1);

        let mut count = 1;
        for i in (2048..=256000).step_by(1024) {
            splinter.remove(i);
            count += 1
        }
        assert_eq!(splinter.cardinality(), (u32::MAX as usize) - count);
    }

    proptest! {
        #[test]
        fn test_splinter_read_proptest(set in hash_set(0u32..16384, 0..1024)) {
            let expected = set.iter().copied().sorted().collect_vec();
            test_partition_read(&Splinter::from_iter(set), &expected);
        }


        #[test]
        fn test_splinter_proptest(set in vec(0u32..16384, 0..1024)) {
            let splinter = mksplinter(&set);
            if set.is_empty() {
                assert!(!splinter.contains(123));
            } else {
                let lookup = set[set.len() / 3];
                assert!(splinter.contains(lookup));
            }
        }

        #[test]
        fn test_splinter_opt_proptest(set in vec(0u32..16384, 0..1024))  {
            let mut splinter = mksplinter(&set);
            splinter.optimize();
            if set.is_empty() {
                assert!(!splinter.contains(123));
            } else {
                let lookup = set[set.len() / 3];
                assert!(splinter.contains(lookup));
            }
        }

        #[test]
        fn test_splinter_eq_proptest(set in vec(0u32..16384, 0..1024)) {
            let a = mksplinter(&set);
            assert_eq!(a, a.clone());
        }

        #[test]
        fn test_splinter_opt_eq_proptest(set in vec(0u32..16384, 0..1024)) {
            let mut a = mksplinter(&set);
            let b = mksplinter(&set);
            a.optimize();
            assert_eq!(a, b);
        }

        #[test]
        fn test_splinter_remove_range_proptest(set in hash_set(0u32..16384, 0..1024)) {
            let expected = set.iter().copied().sorted().collect_vec();
            let mut splinter = mksplinter(&expected);
            if let Some(last) = expected.last() {
                splinter.remove_range((Bound::Excluded(last), Bound::Unbounded));
                assert_equal(splinter.iter(), expected);
            }
        }
    }

    #[test]
    fn test_expected_compression() {
        fn to_roaring(set: impl Iterator<Item = u32>) -> Vec<u8> {
            let mut buf = std::io::Cursor::new(Vec::new());
            let mut bmp = RoaringBitmap::from_sorted_iter(set).unwrap();
            bmp.optimize();
            bmp.serialize_into(&mut buf).unwrap();
            buf.into_inner()
        }

        struct Report {
            name: String,
            baseline: usize,
            //        (actual, expected)
            splinter: (usize, usize),
            roaring: (usize, usize),

            splinter_lz4: usize,
            roaring_lz4: usize,
        }

        let mut reports = vec![];

        let mut run_test = |name: &str,
                            set: Vec<u32>,
                            expected_set_size: usize,
                            expected_splinter: usize,
                            expected_roaring: usize| {
            assert_eq!(set.len(), expected_set_size, "Set size mismatch");

            let mut splinter = Splinter::from_iter(set.clone());
            splinter.optimize();
            itertools::assert_equal(splinter.iter(), set.iter().copied());

            test_partition_read(&splinter, &set);

            let expected_size = splinter.encoded_size();
            let splinter = splinter.encode_to_bytes();

            assert_eq!(
                splinter.len(),
                expected_size,
                "actual encoded size does not match declared encoded size"
            );

            let roaring = to_roaring(set.iter().copied());

            let splinter_lz4 = lz4::block::compress(&splinter, None, false).unwrap();
            let roaring_lz4 = lz4::block::compress(&roaring, None, false).unwrap();

            // verify round trip
            assert_eq!(
                splinter,
                lz4::block::decompress(&splinter_lz4, Some(splinter.len() as i32)).unwrap()
            );
            assert_eq!(
                roaring,
                lz4::block::decompress(&roaring_lz4, Some(roaring.len() as i32)).unwrap()
            );

            reports.push(Report {
                name: name.to_owned(),
                baseline: set.len() * std::mem::size_of::<u32>(),
                splinter: (splinter.len(), expected_splinter),
                roaring: (roaring.len(), expected_roaring),

                splinter_lz4: splinter_lz4.len(),
                roaring_lz4: roaring_lz4.len(),
            });
        };

        let mut set_gen = SetGen::new(0xDEAD_BEEF);

        // empty splinter
        run_test("empty", vec![], 0, 13, 8);

        // 1 element in set
        let set = set_gen.distributed(1, 1, 1, 1);
        run_test("1 element", set, 1, 21, 18);

        // 1 fully dense block
        let set = set_gen.distributed(1, 1, 1, 256);
        run_test("1 dense block", set, 256, 25, 15);

        // 1 half full block
        let set = set_gen.distributed(1, 1, 1, 128);
        run_test("1 half full block", set, 128, 72, 255);

        // 1 sparse block
        let set = set_gen.distributed(1, 1, 1, 16);
        run_test("1 sparse block", set, 16, 57, 48);

        // 8 half full blocks
        let set = set_gen.distributed(1, 1, 8, 128);
        run_test("8 half full blocks", set, 1024, 338, 2003);

        // 8 sparse blocks
        let set = set_gen.distributed(1, 1, 8, 2);
        run_test("8 sparse blocks", set, 16, 67, 48);

        // 64 half full blocks
        let set = set_gen.distributed(4, 4, 4, 128);
        run_test("64 half full blocks", set, 8192, 2634, 16452);

        // 64 sparse blocks
        let set = set_gen.distributed(4, 4, 4, 2);
        run_test("64 sparse blocks", set, 128, 450, 392);

        // 256 half full blocks
        let set = set_gen.distributed(4, 8, 8, 128);
        run_test("256 half full blocks", set, 32768, 10074, 65580);

        // 256 sparse blocks
        let set = set_gen.distributed(4, 8, 8, 2);
        run_test("256 sparse blocks", set, 512, 1402, 1288);

        // 512 half full blocks
        let set = set_gen.distributed(8, 8, 8, 128);
        run_test("512 half full blocks", set, 65536, 20134, 130810);

        // 512 sparse blocks
        let set = set_gen.distributed(8, 8, 8, 2);
        run_test("512 sparse blocks", set, 1024, 2790, 2568);

        // the rest of the compression tests use 4k elements
        let elements = 4096;

        // fully dense splinter
        let set = set_gen.distributed(1, 1, 16, 256);
        run_test("fully dense", set, elements, 87, 63);

        // 128 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 32, 128);
        run_test("128/block; dense", set, elements, 1250, 8208);

        // 32 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 128, 32);
        run_test("32/block; dense", set, elements, 4802, 8208);

        // 16 element per block; dense low partitions
        let set = set_gen.distributed(1, 1, 256, 16);
        run_test("16/block; dense", set, elements, 5666, 8208);

        // 128 elements per block; sparse mid partitions
        let set = set_gen.distributed(1, 32, 1, 128);
        run_test("128/block; sparse mid", set, elements, 1529, 8282);

        // 128 elements per block; sparse high partitions
        let set = set_gen.distributed(32, 1, 1, 128);
        run_test("128/block; sparse high", set, elements, 1870, 8224);

        // 1 element per block; sparse mid partitions
        let set = set_gen.distributed(1, 256, 16, 1);
        run_test("1/block; sparse mid", set, elements, 10521, 10248);

        // 1 element per block; sparse high partitions
        let set = set_gen.distributed(256, 16, 1, 1);
        run_test("1/block; sparse high", set, elements, 15374, 40968);

        // 1/block; spread low
        let set = set_gen.dense(1, 16, 256, 1);
        run_test("1/block; spread low", set, elements, 8377, 8328);

        // each partition is dense
        let set = set_gen.dense(8, 8, 8, 8);
        run_test("dense throughout", set, elements, 2790, 2700);

        // the lowest partitions are dense
        let set = set_gen.dense(1, 1, 64, 64);
        run_test("dense low", set, elements, 291, 267);

        // the mid and low partitions are dense
        let set = set_gen.dense(1, 32, 16, 8);
        run_test("dense mid/low", set, elements, 2393, 2376);

        let random_cases = [
            // random sets drawing from the enire u32 range
            (32, High::MAX_LEN, 145, 328),
            (256, High::MAX_LEN, 1041, 2544),
            (1024, High::MAX_LEN, 4113, 10168),
            (4096, High::MAX_LEN, 15374, 40056),
            (16384, High::MAX_LEN, 52238, 148656),
            (65536, High::MAX_LEN, 199694, 461288),
            // random sets with values < 65536
            (32, 65536, 99, 80),
            (256, 65536, 547, 528),
            (1024, 65536, 2083, 2064),
            (4096, 65536, 5666, 8208),
            (65536, 65536, 25, 15),
            // small sets with values < 1024
            (8, 1024, 49, 32),
            (16, 1024, 67, 48),
            (32, 1024, 94, 80),
            (64, 1024, 126, 144),
            (128, 1024, 183, 272),
        ];

        for (count, max, expected_splinter, expected_roaring) in random_cases {
            let name = if max == High::MAX_LEN {
                format!("random/{count}")
            } else {
                format!("random/{count}/{max}")
            };
            run_test(
                &name,
                set_gen.random_max(count, max),
                count,
                expected_splinter,
                expected_roaring,
            );
        }

        let mut fail_test = false;

        println!("{}", "-".repeat(83));
        println!(
            "{:30} {:12} {:>6} {:>10} {:>10} {:>10}",
            "test", "bitmap", "size", "expected", "relative", "ok"
        );
        for report in &reports {
            println!(
                "{:30} {:12} {:6} {:10} {:>10} {:>10}",
                report.name,
                "Splinter",
                report.splinter.0,
                report.splinter.1,
                "1.00",
                if report.splinter.0 == report.splinter.1 {
                    "ok"
                } else {
                    fail_test = true;
                    "FAIL"
                }
            );

            let diff = report.roaring.0 as f64 / report.splinter.0 as f64;
            let ok_status = if report.roaring.0 != report.roaring.1 {
                fail_test = true;
                "FAIL".into()
            } else {
                ratio_to_marks(diff)
            };
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "", "Roaring", report.roaring.0, report.roaring.1, diff, ok_status
            );

            let diff = report.splinter_lz4 as f64 / report.splinter.0 as f64;
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Splinter LZ4",
                report.splinter_lz4,
                report.splinter_lz4,
                diff,
                ratio_to_marks(diff)
            );

            let diff = report.roaring_lz4 as f64 / report.splinter_lz4 as f64;
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Roaring LZ4",
                report.roaring_lz4,
                report.roaring_lz4,
                diff,
                ratio_to_marks(diff)
            );

            let diff = report.baseline as f64 / report.splinter.0 as f64;
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Baseline",
                report.baseline,
                report.baseline,
                diff,
                ratio_to_marks(diff)
            );
        }

        // calculate average compression ratio (splinter_lz4 / splinter)
        let avg_ratio = reports
            .iter()
            .map(|r| r.splinter_lz4 as f64 / r.splinter.0 as f64)
            .sum::<f64>()
            / reports.len() as f64;

        println!("average compression ratio (splinter_lz4 / splinter): {avg_ratio:.2}");

        assert!(!fail_test, "compression test failed");
    }
}
