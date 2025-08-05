use bytes::{Bytes, BytesMut};
use culprit::Culprit;
use either::Either;
use std::{
    fmt::Debug,
    ops::{Bound, RangeBounds, RangeInclusive},
};
use zerocopy::{
    ConvertError, FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned,
    little_endian::{U16, U32},
};

use crate::{
    DecodeErr, Segment, SplinterRead, SplinterWrite,
    bitmap::{BitmapExt, BitmapMutExt},
    block::{Block, BlockRef},
    partition::{Partition, PartitionRef},
    relational::Relation,
    util::{CopyToOwned, FromSuffix, SerializeContainer},
};

mod cmp;
mod cut;
mod intersection;
mod merge;
mod union;

pub const SPLINTER_MAGIC: [u8; 4] = [0xDA, 0xAE, 0x12, 0xDF];

pub const SPLINTER_MAX_VALUE: u32 = u32::MAX;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    magic: [u8; 4],
}

impl Header {
    const DEFAULT: Header = Header { magic: SPLINTER_MAGIC };

    fn serialize<B: bytes::BufMut>(&self, out: &mut B) -> usize {
        out.put_slice(self.as_bytes());
        Self::serialized_size()
    }

    const fn serialized_size() -> usize {
        size_of::<Header>()
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Footer {
    partitions: U16,
    unused: [u8; 2],
}

impl Footer {
    fn new(partitions: u16) -> Self {
        Self {
            partitions: partitions.into(),
            unused: [0; 2],
        }
    }

    fn serialize<B: bytes::BufMut>(&self, out: &mut B) -> usize {
        out.put_slice(self.as_bytes());
        Self::serialized_size()
    }

    const fn serialized_size() -> usize {
        size_of::<Footer>()
    }
}

/// An owned, compressed bitmap for u32 keys
#[derive(Clone)]
pub struct Splinter {
    partitions: Partition<U32, Partition<U32, Partition<U16, Block>>>,
}

impl Default for Splinter {
    fn default() -> Self {
        Self::EMPTY.clone()
    }
}

impl Splinter {
    pub const EMPTY: Self = Self { partitions: Partition::EMPTY };

    pub fn from_slice(data: &[u32]) -> Self {
        let mut splinter = Self::default();
        for &key in data {
            splinter.insert(key);
        }
        splinter
    }

    pub fn from_bytes<T: AsRef<[u8]>>(data: T) -> Result<Self, Culprit<DecodeErr>> {
        SplinterRef::from_bytes(data).map(Into::into)
    }

    fn insert_block(&mut self, a: u8, b: u8, c: u8, block: Block) {
        let partition = self.partitions.get_or_init(a);
        let partition = partition.get_or_init(b);
        partition.insert(c, block);
    }

    /// Computes the serialized size of this Splinter
    pub fn serialized_size(&self) -> usize {
        Header::serialized_size() + self.partitions.serialized_size() + Footer::serialized_size()
    }

    pub fn serialize<B: bytes::BufMut>(&self, out: &mut B) -> usize {
        let header_size = Header::DEFAULT.serialize(out);
        let (cardinality, partitions_size) = self.partitions.serialize(out);
        let footer_size =
            Footer::new(cardinality.try_into().expect("cardinality overflow")).serialize(out);
        header_size + partitions_size + footer_size
    }

    pub fn serialize_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.serialize(&mut buf);
        buf.freeze()
    }

    pub fn serialize_to_splinter_ref(&self) -> SplinterRef<Bytes> {
        SplinterRef::from_bytes(self.serialize_to_bytes()).expect("serialization roundtrip failed")
    }
}

impl Debug for Splinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Splinter")
            .field("num_partitions", &self.partitions.len())
            .field("cardinality", &self.cardinality())
            .finish()
    }
}

impl<K: Into<u32>> FromIterator<K> for Splinter {
    fn from_iter<T: IntoIterator<Item = K>>(iter: T) -> Self {
        let mut splinter = Self::default();
        for key in iter {
            splinter.insert(key.into());
        }
        splinter
    }
}

impl SplinterRead for Splinter {
    #[inline]
    fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    fn contains(&self, key: u32) -> bool {
        let [a, b, c, d] = segments(key);

        if let Some(partition) = self.partitions.get(a) {
            if let Some(partition) = partition.get(b) {
                if let Some(block) = partition.get(c) {
                    return block.contains(d);
                }
            }
        }

        false
    }

    fn cardinality(&self) -> usize {
        self.partitions
            .iter()
            .flat_map(|(_, p)| p.iter())
            .flat_map(|(_, p)| p.iter())
            .map(|(_, b)| b.cardinality())
            .sum()
    }

    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.partitions
            .iter()
            .flat_map(|(a, p)| p.iter().map(move |(b, p)| (a, b, p)))
            .flat_map(|(a, b, p)| p.iter().map(move |(c, p)| (a, b, c, p)))
            .flat_map(|(a, b, c, p)| p.segments().map(move |d| combine_segments(a, b, c, d)))
    }

    fn range<'a, R>(&'a self, range: R) -> impl Iterator<Item = u32> + 'a
    where
        R: RangeBounds<u32> + 'a,
    {
        // compute the high, mid, low, and block ranges
        let Some([ra, rb, rc, rd]) = segment_ranges(range) else {
            return Either::Left(std::iter::empty());
        };
        Either::Right(
            self.partitions
                .range(ra.into())
                .flat_map(move |(a, p)| {
                    p.range(inner_range(a, ra, rb)).map(move |(b, p)| (a, b, p))
                })
                .flat_map(move |(a, b, p)| {
                    p.range(inner_range(b, rb, rc))
                        .map(move |(c, p)| (a, b, c, p))
                })
                .flat_map(move |(a, b, c, p)| {
                    p.range(inner_range(c, rc, rd))
                        .map(move |d| combine_segments(a, b, c, d))
                }),
        )
    }

    fn last(&self) -> Option<u32> {
        let (a, p) = self.partitions.last()?;
        let (b, p) = p.last()?;
        let (c, p) = p.last()?;
        let d = p.last()?;
        Some(combine_segments(a, b, c, d))
    }
}

impl SplinterWrite for Splinter {
    fn insert(&mut self, key: u32) -> bool {
        let [a, b, c, d] = segments(key);
        let partition = self.partitions.get_or_init(a);
        let partition = partition.get_or_init(b);
        let block = partition.get_or_init(c);
        block.insert(d)
    }
}

/// A compressed bitmap for u32 keys operating directly on a slice of bytes
#[derive(Clone)]
pub struct SplinterRef<T> {
    data: T,
    partitions: usize,
}

impl<T> SplinterRef<T> {
    pub fn inner(&self) -> &T {
        &self.data
    }

    pub fn into_inner(self) -> T {
        self.data
    }
}

impl<T> SplinterRef<T>
where
    T: AsRef<[u8]>,
{
    pub fn from_bytes(data: T) -> Result<Self, Culprit<DecodeErr>> {
        use DecodeErr::*;

        let (header, _) = Ref::<_, Header>::from_prefix(data.as_ref()).map_err(|err| {
            debug_assert!(matches!(err, ConvertError::Size(_)));
            InvalidHeader
        })?;
        if header.magic != SPLINTER_MAGIC {
            return Err(InvalidMagic.into());
        }

        let (_, footer) = Ref::<_, Footer>::from_suffix(data.as_ref()).map_err(|err| {
            debug_assert!(matches!(err, ConvertError::Size(_)));
            InvalidFooter
        })?;
        let partitions = footer.partitions.get() as usize;

        Ok(SplinterRef { data, partitions })
    }

    /// Returns the size of this `SplinterRef`'s serialized bytes
    pub fn size(&self) -> usize {
        self.data.as_ref().len()
    }

    pub(crate) fn load_partitions(
        &self,
    ) -> PartitionRef<'_, U32, PartitionRef<'_, U32, PartitionRef<'_, U16, BlockRef<'_>>>> {
        let data = self.data.as_ref();
        let slice = &data[..data.len() - size_of::<Footer>()];
        PartitionRef::from_suffix(slice, self.partitions)
    }
}

impl<T: AsRef<[u8]>> Debug for SplinterRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplinterRef")
            .field("num_partitions", &self.partitions)
            .field("cardinality", &self.cardinality())
            .finish()
    }
}

impl<T: AsRef<[u8]>> From<SplinterRef<T>> for Splinter {
    fn from(value: SplinterRef<T>) -> Self {
        value.copy_to_owned()
    }
}

impl<T: AsRef<[u8]>> CopyToOwned for SplinterRef<T> {
    type Owned = Splinter;

    fn copy_to_owned(&self) -> Self::Owned {
        let partitions = self.load_partitions().copy_to_owned();
        Splinter { partitions }
    }
}

impl<T: AsRef<[u8]>> SplinterRead for SplinterRef<T> {
    /// Returns `true` if the splinter is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default().serialize_to_splinter_ref();
    /// assert!(splinter.is_empty());
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(1);
    /// let splinter = splinter.serialize_to_splinter_ref();
    /// assert!(!splinter.is_empty());
    /// ```
    #[inline]
    fn is_empty(&self) -> bool {
        self.load_partitions().is_empty()
    }

    /// Returns `true` if the splinter contains the given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// let splinter = splinter.serialize_to_splinter_ref();
    ///
    /// assert!(splinter.contains(1));
    /// assert!(!splinter.contains(2));
    /// assert!(splinter.contains(3));
    /// ```
    fn contains(&self, key: u32) -> bool {
        let [a, b, c, d] = segments(key);

        if let Some(partition) = self.load_partitions().get(a) {
            if let Some(partition) = partition.get(b) {
                if let Some(block) = partition.get(c) {
                    return block.contains(d);
                }
            }
        }

        false
    }

    /// Calculates the total number of values stored in the set.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// let splinter = splinter.serialize_to_splinter_ref();
    ///
    /// assert_eq!(3, splinter.cardinality());
    /// ```
    fn cardinality(&self) -> usize {
        let mut sum = 0;
        for (_, partition) in self.load_partitions().iter() {
            for (_, partition) in partition.iter() {
                sum += partition.cardinality();
            }
        }
        sum
    }

    /// Returns an sorted [`Iterator`] over all keys.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// let splinter = splinter.serialize_to_splinter_ref();
    ///
    /// assert_eq!(&[1, 3, 6], &*splinter.iter().collect::<Vec<_>>());
    /// ```
    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.load_partitions()
            .into_iter()
            .flat_map(|(a, p)| p.into_iter().map(move |(b, p)| (a, b, p)))
            .flat_map(|(a, b, p)| p.into_iter().map(move |(c, p)| (a, b, c, p)))
            .flat_map(|(a, b, c, p)| p.into_segments().map(move |d| combine_segments(a, b, c, d)))
    }

    /// Returns an sorted [`Iterator`] over all keys contained by the provided range.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// splinter.insert(5);
    /// splinter.insert(9);
    /// let splinter = splinter.serialize_to_splinter_ref();
    ///
    /// assert_eq!(&[3, 5, 6], &*splinter.range(3..=6).collect::<Vec<_>>());
    /// ```
    fn range<'a, R>(&'a self, range: R) -> impl Iterator<Item = u32> + 'a
    where
        R: RangeBounds<u32> + 'a,
    {
        // compute the high, mid, low, and block ranges
        let Some([ra, rb, rc, rd]) = segment_ranges(range) else {
            return Either::Left(std::iter::empty());
        };
        Either::Right(
            self.load_partitions()
                .into_range(ra.into())
                .flat_map(move |(a, p)| {
                    p.into_range(inner_range(a, ra, rb))
                        .map(move |(b, p)| (a, b, p))
                })
                .flat_map(move |(a, b, p)| {
                    p.into_range(inner_range(b, rb, rc))
                        .map(move |(c, p)| (a, b, c, p))
                })
                .flat_map(move |(a, b, c, p)| {
                    p.into_range(inner_range(c, rc, rd))
                        .map(move |d| combine_segments(a, b, c, d))
                }),
        )
    }

    /// Returns the last key in the set
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    ///
    /// let splinter = splinter.serialize_to_splinter_ref();
    /// assert_eq!(Some(6), splinter.last());
    /// ```
    fn last(&self) -> Option<u32> {
        let (a, p) = self.load_partitions().last()?;
        let (b, p) = p.last()?;
        let (c, p) = p.last()?;
        let d = p.last()?;
        Some(combine_segments(a, b, c, d))
    }
}

/// split the key into 4 8-bit segments
#[inline]
fn segments(key: u32) -> [Segment; 4] {
    key.to_be_bytes()
}

#[inline]
fn combine_segments(a: Segment, b: Segment, c: Segment, d: Segment) -> u32 {
    u32::from_be_bytes([a, b, c, d])
}

#[derive(Debug, Clone, Copy)]
struct SegmentRange {
    start: Segment,
    end: Segment,
}

impl From<SegmentRange> for RangeInclusive<Segment> {
    fn from(val: SegmentRange) -> Self {
        val.start..=val.end
    }
}

/// Split a range of keys into 4 inclusive ranges corresponding to the high,
/// mid, low, and block segments.
///
/// Returns None if the input range is empty.
#[inline]
fn segment_ranges<R: RangeBounds<u32>>(range: R) -> Option<[SegmentRange; 4]> {
    use Bound::*;
    let (start_bound, end_bound) = (range.start_bound().cloned(), range.end_bound().cloned());
    let is_empty = match (start_bound, end_bound) {
        (_, Excluded(u32::MIN)) | (Excluded(u32::MAX), _) => true,
        (Included(start), Excluded(end))
        | (Excluded(start), Included(end))
        | (Excluded(start), Excluded(end)) => start >= end,
        (Included(start), Included(end)) => start > end,
        _ => false,
    };
    if is_empty {
        return None;
    }

    let start = match start_bound {
        Unbounded => [0; 4],
        Included(segment) => segments(segment),
        Excluded(segment) => segments(segment.saturating_add(1)),
    };
    let end = match end_bound {
        Unbounded => [u8::MAX; 4],
        Included(segment) => segments(segment),
        Excluded(segment) => segments(segment.checked_sub(1).expect("end segment underflow")),
    };
    // zip the two arrays together
    Some(std::array::from_fn(|i| SegmentRange {
        start: start[i],
        end: end[i],
    }))
}

#[inline]
fn inner_range(
    key: Segment,
    key_range: SegmentRange,
    inner_range: SegmentRange,
) -> RangeInclusive<Segment> {
    let SegmentRange { start: s, end: e } = key_range;
    if key == s && key == e {
        inner_range.into()
    } else if key == s {
        inner_range.start..=u8::MAX
    } else if key == e {
        0..=inner_range.end
    } else {
        0..=u8::MAX
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::testutil::{SetGen, analyze_compression_patterns, mksplinter, mksplinter_ref};

    use super::*;
    use roaring::RoaringBitmap;

    #[test]
    fn test_splinter_sanity() {
        // fill up the first partition and sparse fill up the second partition
        let values = (0..65535)
            .chain((65536..85222).step_by(7))
            .collect::<Vec<_>>();

        // build a splinter from the values
        let splinter = mksplinter(values.iter().copied());

        // check that all expected keys are present
        for &i in &values {
            if !splinter.contains(i) {
                splinter.contains(i); // break here for debugging
                panic!("missing key: {i}");
            }
        }

        // check that some keys are not present
        assert!(!splinter.contains(65535), "unexpected key: 65535");
        assert!(!splinter.contains(90999), "unexpected key: 90999");
    }

    #[test]
    fn test_roundtrip_sanity() {
        let assert_round_trip = |splinter: Splinter| {
            let estimated_size = splinter.serialized_size();
            let splinter_ref = SplinterRef::from_bytes(splinter.serialize_to_bytes()).unwrap();
            assert_eq!(
                splinter_ref.size(),
                estimated_size,
                "serialized size matches estimated size"
            );
            assert_eq!(
                splinter.cardinality(),
                splinter_ref.cardinality(),
                "cardinality equal"
            );
            assert_eq!(splinter, splinter_ref, "Splinter == SplinterRef");
            assert_eq!(
                splinter,
                splinter_ref.copy_to_owned(),
                "Splinter == Splinter"
            );
            assert_eq!(
                splinter_ref.copy_to_owned().serialize_to_bytes(),
                splinter.serialize_to_bytes(),
                "deterministic serialization"
            );
        };

        assert_round_trip(mksplinter(0..0));
        assert_round_trip(mksplinter(0..1));
        assert_round_trip(mksplinter(u32::MAX - 10..u32::MAX));
        assert_round_trip(mksplinter(0..10));
        assert_round_trip(mksplinter(0..=255));
        assert_round_trip(mksplinter(0..=4096));
        assert_round_trip(mksplinter(0..=16384));
        assert_round_trip(mksplinter(1512..=3258));
        assert_round_trip(mksplinter((0..=16384).step_by(7)));
    }

    #[test]
    fn test_splinter_ref_sanity() {
        // fill up the first partition and sparse fill up the second partition
        let values = (0..65535)
            .chain((65536..85222).step_by(7))
            .collect::<Vec<_>>();

        // build a splinter from the values
        let splinter = mksplinter_ref(values.iter().copied());

        // check that all expected keys are present
        for &i in &values {
            if !splinter.contains(i) {
                splinter.contains(i); // break here for debugging
                panic!("missing key: {i}");
            }
        }

        // check that the splinter can enumerate all keys
        assert!(itertools::equal(values, splinter.iter()));

        // check that some keys are not present
        assert!(!splinter.contains(65535), "unexpected key: 65535");
        assert!(!splinter.contains(90999), "unexpected key: 90999");
    }

    /// verify `Splinter::range` and `SplinterRef::range`
    #[test]
    pub fn test_range() {
        #[track_caller]
        fn case<I1, R, I2>(name: &str, set: I1, range: R, expected: I2)
        where
            I1: IntoIterator<Item = u32> + Clone,
            R: RangeBounds<u32> + Clone,
            I2: IntoIterator<Item = u32> + Clone,
        {
            let expected = expected.into_iter().collect::<Vec<_>>();

            let output = mksplinter(set.clone())
                .range(range.clone())
                .collect::<Vec<_>>();
            assert!(
                output == expected,
                "Splinter::range failed for case: {name}; output: {:?}; expected: {:?}",
                (output.first(), output.last(), output.len()),
                (expected.first(), expected.last(), expected.len()),
            );

            let output = mksplinter_ref(set).range(range).collect::<Vec<_>>();
            assert!(
                output == expected,
                "SplinterRef::range failed for case: {name}; output: {:?}; expected: {:?}",
                (output.first(), output.last(), output.len()),
                (expected.first(), expected.last(), expected.len()),
            );
        }

        case("empty", [], .., []);
        case("one element", [156106], .., [156106]);
        case(
            "one element, inclusive",
            [156106],
            156105..=156106,
            [156106],
        );
        case("one element, exclusive", [156106], 156105..156107, [156106]);

        case("zero", [0], .., [0]);
        case("zero, inclusive end", [0], ..=0, [0]);
        case("zero, inclusive start", [0], 0.., [0]);
        case("zero, exclusive end", [0], ..0, []);
        case("zero, exclusive start", [0], 1.., []);

        case("max element", [u32::MAX], .., [u32::MAX]);
        case(
            "max element, inclusive end",
            [u32::MAX],
            ..=u32::MAX,
            [u32::MAX],
        );
        case(
            "max element, inclusive start",
            [u32::MAX],
            u32::MAX..,
            [u32::MAX],
        );
        case("max element, exclusive end", [u32::MAX], ..u32::MAX, []);
        case(
            "max element, exclusive start",
            [u32::MAX],
            u32::MAX - 1..,
            [u32::MAX],
        );

        case(
            "simple set",
            [12, 16, 19, 1000002, 1000016, 1000046],
            ..,
            [12, 16, 19, 1000002, 1000016, 1000046],
        );
        case(
            "simple set, inclusive",
            [12, 16, 19, 1000002, 1000016, 1000046],
            19..=1000016,
            [19, 1000002, 1000016],
        );
        case(
            "simple set, exclusive",
            [12, 16, 19, 1000002, 1000016, 1000046],
            19..1000016,
            [19, 1000002],
        );

        let mut set_gen = SetGen::new(0xDEAD_BEEF);

        let set = set_gen.distributed(4, 8, 8, 128);
        let expected = set[1024..16384].to_vec();
        let range = expected[0]..=expected[expected.len() - 1];
        case("256 half full blocks", set.clone(), range, expected);

        let expected = set[1024..].to_vec();
        let range = expected[0]..;
        case(
            "256 half full blocks, unbounded right",
            set.clone(),
            range,
            expected,
        );

        let expected = set[..16384].to_vec();
        let range = ..=expected[expected.len() - 1];
        case(
            "256 half full blocks, unbounded left",
            set.clone(),
            range,
            expected,
        );
    }

    #[test]
    fn test_expected_compression() {
        let to_roaring = |set: Vec<u32>| {
            let mut buf = io::Cursor::new(Vec::new());
            let mut bmp = RoaringBitmap::from_sorted_iter(set).unwrap();
            bmp.optimize();
            bmp.serialize_into(&mut buf).unwrap();
            buf.into_inner()
        };

        struct Report {
            name: &'static str,
            baseline: usize,
            //        (actual, expected)
            splinter: (usize, usize),
            roaring: (usize, usize),

            splinter_lz4: usize,
            roaring_lz4: usize,
        }

        let mut reports = vec![];

        let mut run_test = |name: &'static str,
                            set: Vec<u32>,
                            expected_set_size: usize,
                            expected_splinter: usize,
                            expected_roaring: usize| {
            println!("-------------------------------------");
            println!("running test: {name}");

            assert_eq!(set.len(), expected_set_size, "Set size mismatch");

            let splinter = mksplinter(set.clone()).serialize_to_bytes();
            let roaring = to_roaring(set.clone());

            analyze_compression_patterns(&splinter);

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
                name,
                baseline: set.len() * std::mem::size_of::<u32>(),
                splinter: (splinter.len(), expected_splinter),
                roaring: (roaring.len(), expected_roaring),

                splinter_lz4: splinter_lz4.len(),
                roaring_lz4: roaring_lz4.len(),
            });
        };

        let mut set_gen = SetGen::new(0xDEAD_BEEF);

        // empty splinter
        run_test("empty", vec![], 0, 8, 8);

        // 1 element in set
        let set = set_gen.distributed(1, 1, 1, 1);
        run_test("1 element", set, 1, 25, 18);

        // 1 fully dense block
        let set = set_gen.distributed(1, 1, 1, 256);
        run_test("1 dense block", set, 256, 24, 15);

        // 1 half full block
        let set = set_gen.distributed(1, 1, 1, 128);
        run_test("1 half full block", set, 128, 56, 247);

        // 1 sparse block
        let set = set_gen.distributed(1, 1, 1, 16);
        run_test("1 sparse block", set, 16, 40, 48);

        // 8 half full blocks
        let set = set_gen.distributed(1, 1, 8, 128);
        run_test("8 half full blocks", set, 1024, 308, 2064);

        // 8 sparse blocks
        let set = set_gen.distributed(1, 1, 8, 2);
        run_test("8 sparse blocks", set, 16, 68, 48);

        // 64 half full blocks
        let set = set_gen.distributed(4, 4, 4, 128);
        run_test("64 half full blocks", set, 8192, 2432, 16486);

        // 64 sparse blocks
        let set = set_gen.distributed(4, 4, 4, 2);
        run_test("64 sparse blocks", set, 128, 512, 392);

        // 256 half full blocks
        let set = set_gen.distributed(4, 8, 8, 128);
        run_test("256 half full blocks", set, 32768, 9440, 65520);

        // 256 sparse blocks
        let set = set_gen.distributed(4, 8, 8, 2);
        run_test("256 sparse blocks", set, 512, 1760, 1288);

        // 512 half full blocks
        let set = set_gen.distributed(8, 8, 8, 128);
        run_test("512 half full blocks", set, 65536, 18872, 130742);

        // 512 sparse blocks
        let set = set_gen.distributed(8, 8, 8, 2);
        run_test("512 sparse blocks", set, 1024, 3512, 2568);

        // the rest of the compression tests use 4k elements
        let elements = 4096;

        // fully dense splinter
        let set = set_gen.distributed(1, 1, 16, 256);
        run_test("fully dense", set, elements, 84, 75);

        // 128 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 32, 128);
        run_test("128/block; dense", set, elements, 1172, 8195);

        // 32 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 128, 32);
        run_test("32/block; dense", set, elements, 4532, 8208);

        // 16 element per block; dense low partitions
        let set = set_gen.distributed(1, 1, 256, 16);
        run_test("16/block; dense", set, elements, 4884, 8208);

        // 128 elements per block; sparse mid partitions
        let set = set_gen.distributed(1, 32, 1, 128);
        run_test("128/block; sparse mid", set, elements, 1358, 8300);

        // 128 elements per block; sparse high partitions
        let set = set_gen.distributed(32, 1, 1, 128);
        run_test("128/block; sparse high", set, elements, 1544, 8290);

        // 1 element per block; sparse mid partitions
        let set = set_gen.distributed(1, 256, 16, 1);
        run_test("1/block; sparse mid", set, elements, 21774, 10248);

        // 1 element per block; sparse high partitions
        let set = set_gen.distributed(256, 16, 1, 1);
        run_test("1/block; sparse high", set, elements, 46344, 40968);

        // 1/block; spread low
        let set = set_gen.dense(1, 16, 256, 1);
        run_test("1/block; spread low", set, elements, 16494, 8328);

        // each partition is dense
        let set = set_gen.dense(8, 8, 8, 8);
        run_test("dense throughout", set, elements, 6584, 2700);

        // the lowest partitions are dense
        let set = set_gen.dense(1, 1, 64, 64);
        run_test("dense low", set, elements, 2292, 267);

        // the mid and low partitions are dense
        let set = set_gen.dense(1, 32, 16, 8);
        run_test("dense mid/low", set, elements, 6350, 2376);

        // fully random sets of varying sizes
        run_test("random/32", set_gen.random(32), 32, 546, 328);
        run_test("random/256", set_gen.random(256), 256, 3655, 2560);
        run_test("random/1024", set_gen.random(1024), 1024, 12499, 10168);
        run_test("random/4096", set_gen.random(4096), 4096, 45582, 39952);
        run_test("random/16384", set_gen.random(16384), 16384, 163758, 148600);
        run_test("random/65535", set_gen.random(65535), 65535, 543584, 462190);

        let mut fail_test = false;

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
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Roaring",
                report.roaring.0,
                report.roaring.1,
                diff,
                if report.roaring.0 != report.roaring.1 {
                    fail_test = true;
                    "FAIL"
                } else if diff < 1.0 {
                    "<"
                } else {
                    "ok"
                }
            );
            let diff = report.splinter_lz4 as f64 / report.splinter.0 as f64;
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Splinter LZ4",
                report.splinter_lz4,
                report.splinter_lz4,
                diff,
                if report.splinter.0 <= report.splinter_lz4 {
                    ">"
                } else {
                    "<"
                }
            );
            let diff = report.roaring_lz4 as f64 / report.splinter_lz4 as f64;
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Roaring LZ4",
                report.roaring_lz4,
                report.roaring_lz4,
                diff,
                if report.splinter_lz4 <= report.roaring_lz4 {
                    "ok"
                } else {
                    "<"
                }
            );
            let diff = report.baseline as f64 / report.splinter.0 as f64;
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Baseline",
                report.baseline,
                report.baseline,
                diff,
                if report.splinter.0 <= report.baseline {
                    "ok"
                } else {
                    // we don't fail the test, just report for informational purposes;
                    "<"
                }
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
