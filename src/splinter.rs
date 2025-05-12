use bytes::{Bytes, BytesMut};
use culprit::Culprit;
use std::fmt::Debug;
use zerocopy::{
    ConvertError, FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned,
    little_endian::{U16, U32},
};

use crate::{
    DecodeErr, Segment,
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
        size_of::<Header>()
    }
}

/// An owned, compressed bitmap for u32 keys
#[derive(Default, Clone)]
pub struct Splinter {
    partitions: Partition<U32, Partition<U32, Partition<U16, Block>>>,
}

impl Splinter {
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

    /// Returns `true` if the splinter is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::Splinter;
    ///
    /// let mut splinter = Splinter::default();
    /// assert!(splinter.is_empty());
    /// splinter.insert(1);
    /// assert!(!splinter.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Returns `true` if the splinter contains the given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::Splinter;
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(1);
    /// splinter.insert(3);
    ///
    /// assert!(splinter.contains(1));
    /// assert!(!splinter.contains(2));
    /// assert!(splinter.contains(3));
    /// ```
    pub fn contains(&self, key: u32) -> bool {
        let (a, b, c, d) = segments(key);

        if let Some(partition) = self.partitions.get(a) {
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
    /// # use splinter_rs::Splinter;
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    ///
    /// assert_eq!(3, splinter.cardinality());
    /// ```
    pub fn cardinality(&self) -> usize {
        self.partitions
            .sorted_iter()
            .flat_map(|(_, p)| p.sorted_iter())
            .flat_map(|(_, p)| p.sorted_iter())
            .map(|(_, b)| b.cardinality())
            .sum()
    }

    /// Returns an sorted [`Iterator`] over all keys.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::Splinter;
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    ///
    /// assert_eq!(&[1, 3, 6], &*splinter.iter().collect::<Vec<_>>());
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.partitions
            .sorted_iter()
            .flat_map(|(a, p)| p.sorted_iter().map(move |(b, p)| (a, b, p)))
            .flat_map(|(a, b, p)| p.sorted_iter().map(move |(c, p)| (a, b, c, p)))
            .flat_map(|(a, b, c, p)| p.segments().map(move |d| combine_segments(a, b, c, d)))
    }

    /// Attempts to insert a key into the Splinter, returning true if a key was inserted
    pub fn insert(&mut self, key: u32) -> bool {
        let (a, b, c, d) = segments(key);
        let partition = self.partitions.get_or_init(a);
        let partition = partition.get_or_init(b);
        let block = partition.get_or_init(c);
        block.insert(d)
    }

    fn insert_block(&mut self, a: u8, b: u8, c: u8, block: Block) {
        let partition = self.partitions.get_or_init(a);
        let partition = partition.get_or_init(b);
        partition.insert(c, block);
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

    /// Returns the last key in the set
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::Splinter;
    ///
    /// let mut splinter = Splinter::default();
    ///
    /// assert_eq!(None, splinter.last());
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// assert_eq!(Some(6), splinter.last());
    /// ```
    pub fn last(&self) -> Option<u32> {
        let (a, p) = self.partitions.last()?;
        let (b, p) = p.last()?;
        let (c, p) = p.last()?;
        let d = p.last()?;
        Some(combine_segments(a, b, c, d))
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

/// A compressed bitmap for u32 keys operating directly on a slice of bytes
#[derive(Clone)]
pub struct SplinterRef<T> {
    data: T,
    partitions: usize,
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

    pub fn size(&self) -> usize {
        self.data.as_ref().len()
    }

    pub fn inner(&self) -> &T {
        &self.data
    }

    pub fn into_inner(self) -> T {
        self.data
    }

    pub(crate) fn load_partitions(
        &self,
    ) -> PartitionRef<'_, U32, PartitionRef<'_, U32, PartitionRef<'_, U16, BlockRef<'_>>>> {
        let data = self.data.as_ref();
        let slice = &data[..data.len() - size_of::<Footer>()];
        PartitionRef::from_suffix(slice, self.partitions)
    }

    /// Returns `true` if the splinter is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::Splinter;
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
    pub fn is_empty(&self) -> bool {
        self.load_partitions().is_empty()
    }

    /// Returns `true` if the splinter contains the given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::Splinter;
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
    pub fn contains(&self, key: u32) -> bool {
        let (a, b, c, d) = segments(key);

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
    /// # use splinter_rs::Splinter;
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// let splinter = splinter.serialize_to_splinter_ref();
    ///
    /// assert_eq!(3, splinter.cardinality());
    /// ```
    pub fn cardinality(&self) -> usize {
        let mut sum = 0;
        for (_, partition) in self.load_partitions().sorted_iter() {
            for (_, partition) in partition.sorted_iter() {
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
    /// # use splinter_rs::Splinter;
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// let splinter = splinter.serialize_to_splinter_ref();
    ///
    /// assert_eq!(&[1, 3, 6], &*splinter.iter().collect::<Vec<_>>());
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.load_partitions()
            .into_iter()
            .flat_map(|(a, p)| p.into_iter().map(move |(b, p)| (a, b, p)))
            .flat_map(|(a, b, p)| p.into_iter().map(move |(c, p)| (a, b, c, p)))
            .flat_map(|(a, b, c, p)| p.into_segments().map(move |d| combine_segments(a, b, c, d)))
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

/// split the key into 4 8-bit segments
#[inline]
fn segments(key: u32) -> (Segment, Segment, Segment, Segment) {
    let [a, b, c, d] = key.to_be_bytes();
    (a, b, c, d)
}

#[inline]
fn combine_segments(a: Segment, b: Segment, c: Segment, d: Segment) -> u32 {
    u32::from_be_bytes([a, b, c, d])
}

#[cfg(test)]
mod tests {

    use std::io;

    use crate::testutil::{mksplinter, mksplinter_ref};

    use super::*;
    use itertools::Itertools;
    use rand::{SeedableRng, seq::index};
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
            let splinter_ref = SplinterRef::from_bytes(splinter.serialize_to_bytes()).unwrap();
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

    /// Heuristic analyzer: prints patterns found in the data which could be
    /// exploited by lz4 to improve compression
    pub fn analyze_compression_patterns(data: &[u8]) {
        use std::collections::HashMap;

        let len = data.len();
        if len == 0 {
            println!("empty slice");
            return;
        }
        println!("length: {len} bytes");

        // --- zeros ---
        let (mut zeros, mut longest_run, mut run) = (0usize, 0usize, 0usize);
        for &b in data {
            if b == 0 {
                zeros += 1;
                run += 1;
                longest_run = longest_run.max(run);
            } else {
                run = 0;
            }
        }
        println!(
            "zeros: {zeros} ({:.2}%), longest run: {longest_run}",
            zeros as f64 * 100.0 / len as f64
        );

        // --- histogram / entropy ---
        let mut freq = [0u32; 256];
        for &b in data {
            freq[b as usize] += 1;
        }
        let entropy: f64 = freq
            .iter()
            .filter(|&&c| c != 0)
            .map(|&c| {
                let p = c as f64 / len as f64;
                -p * p.log2()
            })
            .sum();
        println!("shannon entropy ≈ {entropy:.3} bits/byte (max 8)");

        // --- repeated 8-byte blocks ---
        const BLOCK: usize = 8;
        if len >= BLOCK {
            let mut map: HashMap<&[u8], u32> = HashMap::new();
            for chunk in data.chunks_exact(BLOCK) {
                *map.entry(chunk).or_default() += 1;
            }

            let mut duplicate_bytes = 0u32;
            let mut top: Option<(&[u8], u32)> = None;

            for (&k, &v) in map.iter() {
                if v > 1 {
                    duplicate_bytes += (v - 1) * BLOCK as u32;
                    if top.map_or(true, |(_, max)| v > max) {
                        top = Some((k, v));
                    }
                }
            }

            if let Some((bytes, count)) = top {
                println!(
                    "repeated 8-byte blocks: {} duplicate bytes; most common occurs {count}× (bytes {:02X?})",
                    duplicate_bytes, bytes
                );
            } else {
                println!("no duplicated 8-byte blocks");
            }
        }

        println!("analysis complete");
    }

    #[test]
    fn test_expected_compression() {
        let to_roaring = |set: Vec<u32>| {
            let mut buf = io::Cursor::new(Vec::new());
            RoaringBitmap::from_sorted_iter(set)
                .unwrap()
                .serialize_into(&mut buf)
                .unwrap();
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
                            expected_splinter: usize,
                            expected_roaring: usize| {
            println!("-------------------------------------");
            println!("running test: {name}");

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

        struct SetGen {
            rng: rand::rngs::StdRng,
        }

        impl SetGen {
            #[track_caller]
            fn distributed(
                &mut self,
                high: usize,
                mid: usize,
                low: usize,
                block: usize,
                expected_len: usize,
            ) -> Vec<u32> {
                let mut out = Vec::with_capacity(expected_len);
                for high in index::sample(&mut self.rng, 256, high) {
                    for mid in index::sample(&mut self.rng, 256, mid) {
                        for low in index::sample(&mut self.rng, 256, low) {
                            for blk in index::sample(&mut self.rng, 256, block) {
                                out.push(u32::from_be_bytes([
                                    high as u8, mid as u8, low as u8, blk as u8,
                                ]));
                            }
                        }
                    }
                }
                out.sort();
                assert_eq!(out.len(), expected_len);
                out
            }

            #[track_caller]
            fn dense(
                &mut self,
                high: usize,
                mid: usize,
                low: usize,
                block: usize,
                expected_len: usize,
            ) -> Vec<u32> {
                let out: Vec<u32> = itertools::iproduct!(0..high, 0..mid, 0..low, 0..block)
                    .map(|(a, b, c, d)| u32::from_be_bytes([a as u8, b as u8, c as u8, d as u8]))
                    .collect();
                assert_eq!(out.len(), expected_len);
                out
            }

            fn random(&mut self, len: usize) -> Vec<u32> {
                index::sample(&mut self.rng, u32::MAX as usize, len)
                    .into_iter()
                    .map(|i| i as u32)
                    .sorted()
                    .collect()
            }
        }

        let mut set_gen = SetGen {
            rng: rand::rngs::StdRng::seed_from_u64(0xDEAD_BEEF),
        };

        // empty splinter
        run_test("empty", vec![], 8, 8);

        // 1 element in set
        let set = set_gen.distributed(1, 1, 1, 1, 1);
        run_test("1 element", set, 25, 18);

        // 1 fully dense block
        let set = set_gen.distributed(1, 1, 1, 256, 256);
        run_test("1 dense block", set, 24, 528);

        // 1 half full block
        let set = set_gen.distributed(1, 1, 1, 128, 128);
        run_test("1 half full block", set, 56, 272);

        // 1 sparse block
        let set = set_gen.distributed(1, 1, 1, 16, 16);
        run_test("1 sparse block", set, 40, 48);

        // 8 half full blocks
        let set = set_gen.distributed(1, 1, 8, 128, 1024);
        run_test("8 half full blocks", set, 308, 2064);

        // 8 sparse blocks
        let set = set_gen.distributed(1, 1, 8, 2, 16);
        run_test("8 sparse blocks", set, 68, 48);

        // 64 half full blocks
        let set = set_gen.distributed(4, 4, 4, 128, 8192);
        run_test("64 half full blocks", set, 2432, 16520);

        // 64 sparse blocks
        let set = set_gen.distributed(4, 4, 4, 2, 128);
        run_test("64 sparse blocks", set, 512, 392);

        // 256 half full blocks
        let set = set_gen.distributed(4, 8, 8, 128, 32768);
        run_test("256 half full blocks", set, 9440, 65800);

        // 256 sparse blocks
        let set = set_gen.distributed(4, 8, 8, 2, 512);
        run_test("256 sparse blocks", set, 1760, 1288);

        // 512 half full blocks
        let set = set_gen.distributed(8, 8, 8, 128, 65536);
        run_test("512 half full blocks", set, 18872, 131592);

        // 512 sparse blocks
        let set = set_gen.distributed(8, 8, 8, 2, 1024);
        run_test("512 sparse blocks", set, 3512, 2568);

        // the rest of the compression tests use 4k elements
        let elements = 4096;

        // fully dense splinter
        let set = set_gen.distributed(1, 1, 16, 256, elements);
        run_test("fully dense", set, 84, 8208);

        // 128 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 32, 128, elements);
        run_test("128/block; dense", set, 1172, 8208);

        // 32 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 128, 32, elements);
        run_test("32/block; dense", set, 4532, 8208);

        // 16 element per block; dense low partitions
        let set = set_gen.distributed(1, 1, 256, 16, elements);
        run_test("16/block; dense", set, 4884, 8208);

        // 128 elements per block; sparse mid partitions
        let set = set_gen.distributed(1, 32, 1, 128, elements);
        run_test("128/block; sparse mid", set, 1358, 8456);

        // 128 elements per block; sparse high partitions
        let set = set_gen.distributed(32, 1, 1, 128, elements);
        run_test("128/block; sparse high", set, 1544, 8456);

        // 1 element per block; sparse mid partitions
        let set = set_gen.distributed(1, 256, 16, 1, elements);
        run_test("1/block; sparse mid", set, 21774, 10248);

        // 1 element per block; sparse high partitions
        let set = set_gen.distributed(256, 16, 1, 1, elements);
        run_test("1/block; sparse high", set, 46344, 40968);

        // each partition is dense
        let set = set_gen.dense(8, 8, 8, 8, elements);
        run_test("dense throughout", set, 6584, 8712);

        // the lowest partitions are dense
        let set = set_gen.dense(1, 1, 64, 64, elements);
        run_test("dense low", set, 2292, 8208);

        // the mid and low partitions are dense
        let set = set_gen.dense(1, 32, 16, 8, elements);
        run_test("dense mid/low", set, 6350, 8456);

        // fully random sets of varying sizes
        run_test("random/32", set_gen.random(32), 546, 328);
        run_test("random/256", set_gen.random(256), 3655, 2560);
        run_test("random/1024", set_gen.random(1024), 12499, 10168);
        run_test("random/4096", set_gen.random(4096), 45582, 39952);
        run_test("random/16384", set_gen.random(16384), 163758, 148600);
        run_test("random/65535", set_gen.random(65535), 543584, 462190);

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
