use core::fmt;
use std::{collections::BTreeMap, fmt::Debug, usize};

use bitvec::{bitbox, boxed::BitBox, order::Lsb0};
use num::cast::AsPrimitive;
use zerocopy::{LE, U16, U32};

use crate::u24::u24;

/// Tree sparsity ratio limit
const SPARSE_THRESHOLD: f64 = 0.5;

pub type Splinter = Partition<High>;

static_assertions::const_assert_eq!(std::mem::size_of::<Splinter>(), 40);

pub type Segment = u8;

pub trait SplitSegment {
    type Rest;
    fn segment(self) -> Segment;
    fn split(self) -> (Segment, Self::Rest);
    fn unsplit(segment: Segment, rest: Self::Rest) -> Self;
}

macro_rules! impl_split {
    ($(($ty:ty,  $rest:ty)),*) => {
        $(
            impl SplitSegment for $ty {
                type Rest = $rest;

                #[inline]
                fn segment(self) -> Segment {
                    (self >> (<$rest>::BITS as usize)).as_()
                }

                #[inline]
                fn split(self) -> (Segment, Self::Rest) {
                    (self.segment(), self.as_())
                }

                fn unsplit(segment: Segment, rest: Self::Rest) -> Self {
                    let segment: $ty = segment.as_();
                    let rest: $ty = rest.as_();
                    segment << (<$rest>::BITS as usize) | rest
                }
            }
        )*
    };
}

impl_split!((u32, u24), (u24, u16), (u16, u8));

impl SplitSegment for u8 {
    type Rest = u8;
    fn segment(self) -> Segment {
        unreachable!()
    }
    fn split(self) -> (Segment, Self::Rest) {
        unreachable!()
    }
    fn unsplit(_segment: Segment, _rest: Self::Rest) -> Self {
        unreachable!()
    }
}

pub trait TruncateFrom<T> {
    fn truncate_from(other: T) -> Self;
}

macro_rules! impl_truncate_from {
    ($($ty:ty),*) => {
        $(
            impl TruncateFrom<usize> for $ty {
                fn truncate_from(other: usize) -> Self {
                    other.as_()
                }
            }
        )*
    };
}

impl_truncate_from!(u32, u24, u16, u8);

pub trait Level {
    const DEBUG_NAME: &'static str;

    type Offset;
    type LevelDown: Level;
    type Down: PartitionRead<Self::LevelDown> + PartitionWrite<Self::LevelDown> + Default + Debug;
    type Value: num::PrimInt
        + AsPrimitive<usize>
        + SplitSegment<Rest = <Self::LevelDown as Level>::Value>
        + TruncateFrom<usize>
        + Debug;
    const BITS: usize;
    const MAX_LEN: usize = 1 << Self::BITS;
    const VEC_LIMIT: usize = (Self::MAX_LEN) / Self::BITS;
    const TREE_MIN: usize = 32;
    const PREFER_TREE: bool = Self::BITS > 8;
}

#[derive(Debug, Default)]
pub struct High;
impl Level for High {
    const DEBUG_NAME: &'static str = "High";

    type Offset = U32<LE>;
    type LevelDown = Mid;
    type Down = Partition<Self::LevelDown>;
    type Value = u32;
    const BITS: usize = 32;
}

#[derive(Debug, Default)]
pub struct Mid;
impl Level for Mid {
    const DEBUG_NAME: &'static str = "Mid";

    type Offset = U32<LE>;
    type LevelDown = Low;
    type Down = Partition<Self::LevelDown>;
    type Value = u24;
    const BITS: usize = 24;
}

#[derive(Debug, Default)]
pub struct Low;
impl Level for Low {
    const DEBUG_NAME: &'static str = "Low";

    type Offset = U16<LE>;
    type LevelDown = ();
    type Down = Partition<()>;
    type Value = u16;
    const BITS: usize = 16;
}

impl Level for () {
    const DEBUG_NAME: &'static str = "Block";

    type Offset = ();
    type LevelDown = ();
    type Down = ();
    type Value = u8;
    const BITS: usize = 8;
}

pub trait PartitionRead<L: Level> {
    /// the total number of values accessible via this partition.
    fn cardinality(&self) -> usize;

    /// returns true if this partition is empty
    fn is_empty(&self) -> bool;

    /// returns true if this partition contains the given value
    fn contains(&self, value: L::Value) -> bool;

    /// returns an iterator over all values in this partition
    fn iter(&self) -> impl Iterator<Item = L::Value>;

    /// returns the serialized size in bytes of this Partition
    fn serialized_size(&self) -> usize;
}

impl<L: Level> PartitionRead<L> for () {
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

    fn serialized_size(&self) -> usize {
        unreachable!("invalid splinter")
    }
}

pub trait PartitionWrite<L: Level> {
    /// Inserts the value into the partition unless it already exists.
    /// Returns `true` if the insertion occurred, otherwise `false`.
    fn insert(&mut self, value: L::Value) -> bool;
}

impl<L: Level> PartitionWrite<L> for () {
    fn insert(&mut self, _value: L::Value) -> bool {
        unreachable!("invalid splinter")
    }
}

pub enum Partition<L: Level> {
    Vec(VecPartition<L>),
    Tree(TreePartition<L>),
    Bitmap(BitmapPartition<L>),
    Full,
}

impl<L: Level> Default for Partition<L> {
    fn default() -> Self {
        Partition::Vec(VecPartition::default())
    }
}

impl<L: Level> Debug for Partition<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Partition::Tree(partition) => partition.fmt(f),
            Partition::Vec(partition) => partition.fmt(f),
            Partition::Bitmap(partition) => partition.fmt(f),
            Partition::Full => write!(f, "Full"),
        }
    }
}

impl<L: Level> PartitionRead<L> for Partition<L> {
    fn cardinality(&self) -> usize {
        match self {
            Partition::Tree(partition) => partition.cardinality(),
            Partition::Vec(partition) => partition.cardinality(),
            Partition::Bitmap(partition) => partition.cardinality(),
            Partition::Full => L::MAX_LEN,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Partition::Tree(partition) => partition.is_empty(),
            Partition::Vec(partition) => partition.is_empty(),
            Partition::Bitmap(partition) => partition.is_empty(),
            Partition::Full => false,
        }
    }

    fn contains(&self, value: L::Value) -> bool {
        debug_assert!(value.as_() < L::MAX_LEN, "value out of range");

        match self {
            Partition::Tree(partition) => partition.contains(value),
            Partition::Vec(partition) => partition.contains(value),
            Partition::Bitmap(partition) => partition.contains(value),
            Partition::Full => true,
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        use itertools::Either::*;
        match self {
            Partition::Tree(partition) => Left(Left(partition.iter())),
            Partition::Vec(partition) => Left(Right(partition.iter())),
            Partition::Bitmap(partition) => Right(Left(partition.iter())),
            Partition::Full => Right(Right((0..L::MAX_LEN).map(L::Value::truncate_from))),
        }
    }

    fn serialized_size(&self) -> usize {
        match self {
            Partition::Tree(partition) => partition.serialized_size(),
            Partition::Vec(partition) => partition.serialized_size(),
            Partition::Bitmap(partition) => partition.serialized_size(),
            Partition::Full => 1,
        }
    }
}

impl<L: Level> PartitionWrite<L> for Partition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let inserted = match self {
            Partition::Tree(partition) => partition.insert(value),
            Partition::Vec(partition) => partition.insert(value),
            Partition::Bitmap(partition) => partition.insert(value),
            Partition::Full => false,
        };

        if inserted {
            let new_partition = match self {
                Partition::Tree(p) => p.maybe_change_storage(),
                Partition::Vec(p) => p.maybe_change_storage(),
                Partition::Bitmap(p) => p.maybe_change_storage(),
                _ => None,
            };

            if let Some(new_partition) = new_partition {
                *self = new_partition;
            }
        }

        inserted
    }
}

impl<L: Level> FromIterator<L::Value> for Partition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let partition: VecPartition<L> = iter.into_iter().collect();
        if let Some(p) = partition.maybe_change_storage() {
            p
        } else {
            Partition::Vec(partition)
        }
    }
}

pub struct TreePartition<L: Level> {
    children: BTreeMap<Segment, L::Down>,
    cardinality: usize,
    _marker: std::marker::PhantomData<L>,
}

impl<L: Level> TreePartition<L> {
    fn maybe_change_storage(&self) -> Option<Partition<L>> {
        if self.cardinality == L::MAX_LEN {
            return Some(Partition::Full);
        } else if self.cardinality == 0 {
            return None;
        }

        let sparsity_ratio = self.children.len() as f64 / self.cardinality as f64;
        if self.cardinality <= L::VEC_LIMIT && sparsity_ratio > SPARSE_THRESHOLD {
            return Some(Partition::Vec(self.iter().collect()));
        }

        if self.cardinality > L::VEC_LIMIT {
            return Some(Partition::Bitmap(self.iter().collect()));
        }

        None
    }
}

impl<L: Level> Debug for TreePartition<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TreePartition<{}>", L::DEBUG_NAME)?;
        self.children.fmt(f)
    }
}

impl<L: Level> Default for TreePartition<L> {
    fn default() -> Self {
        Self {
            children: BTreeMap::new(),
            cardinality: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<L: Level> FromIterator<L::Value> for TreePartition<L> {
    fn from_iter<T: IntoIterator<Item = L::Value>>(iter: T) -> Self {
        let mut partition = TreePartition::default();
        for value in iter {
            partition.insert(value);
        }
        partition
    }
}

impl<L: Level> PartitionRead<L> for TreePartition<L> {
    fn cardinality(&self) -> usize {
        self.cardinality
    }

    fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    fn contains(&self, value: L::Value) -> bool {
        let (segment, value) = value.split();
        self.children
            .get(&segment)
            .map_or(false, |child| child.contains(value))
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.children.iter().flat_map(|(&segment, child)| {
            child
                .iter()
                .map(move |value| L::Value::unsplit(segment, value))
        })
    }

    fn serialized_size(&self) -> usize {
        let index = self.children.len().min(L::VEC_LIMIT);
        let offsets = self.children.len() * std::mem::size_of::<L::Offset>();
        let values: usize = self.children.values().map(|c| c.serialized_size()).sum();
        index + offsets + values
    }
}

impl<L: Level> PartitionWrite<L> for TreePartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let (segment, value) = value.split();
        if self.children.entry(segment).or_default().insert(value) {
            self.cardinality += 1;
            true
        } else {
            false
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct VecPartition<L: Level> {
    values: Vec<L::Value>,
}

impl<L: Level> Default for VecPartition<L> {
    fn default() -> Self {
        VecPartition { values: Vec::new() }
    }
}

impl<L: Level> Debug for VecPartition<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VecPartition<{}>({})", L::DEBUG_NAME, self.cardinality())
    }
}

impl<L: Level> VecPartition<L> {
    /// Construct an VecPartition from a sorted vector of values
    #[inline]
    pub fn from_sorted(values: Vec<L::Value>) -> Self {
        VecPartition { values }
    }

    fn maybe_change_storage(&self) -> Option<Partition<L>> {
        if self.cardinality() == L::MAX_LEN {
            return Some(Partition::Full);
        } else if self.cardinality() > L::VEC_LIMIT {
            Some(Partition::Bitmap(self.iter().collect()))
        } else if self.cardinality() > L::TREE_MIN && L::PREFER_TREE {
            let unique_segments = count_unique_sorted(self.values.iter().map(|v| v.segment()));
            let sparsity_ratio = unique_segments as f64 / self.cardinality() as f64;

            if sparsity_ratio < SPARSE_THRESHOLD {
                Some(Partition::Tree(self.iter().collect()))
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<L: Level> FromIterator<L::Value> for VecPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let mut values: Vec<L::Value> = iter.into_iter().collect();
        values.sort();
        VecPartition::from_sorted(values)
    }
}

impl<L: Level> PartitionRead<L> for VecPartition<L> {
    fn cardinality(&self) -> usize {
        self.values.len()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn contains(&self, value: L::Value) -> bool {
        self.values.binary_search(&value).is_ok()
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.values.iter().copied()
    }

    fn serialized_size(&self) -> usize {
        self.values.len() * (L::BITS / 8)
    }
}

impl<L: Level> PartitionWrite<L> for VecPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        assert!(value.as_() < L::MAX_LEN, "value out of range");
        match self.values.binary_search(&value) {
            // value already exists
            Ok(_) => false,
            // value doesn't exist, insert it
            Err(index) => {
                self.values.insert(index, value);
                true
            }
        }
    }
}

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

impl<L: Level> BitmapPartition<L> {
    fn maybe_change_storage(&self) -> Option<Partition<L>> {
        (self.cardinality() == L::MAX_LEN).then_some(Partition::Full)
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

    fn serialized_size(&self) -> usize {
        L::MAX_LEN / 8
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

fn count_unique_sorted<I, T>(iter: I) -> usize
where
    I: IntoIterator<Item = T>,
    T: PartialEq,
{
    let mut iter = iter.into_iter();
    let mut count = 1;
    let mut prev = match iter.next() {
        Some(val) => val,
        None => return 0,
    };

    for curr in iter {
        if curr != prev {
            count += 1;
            prev = curr;
        }
    }

    count
}

#[cfg(test)]
mod tests {

    use roaring::RoaringBitmap;

    use crate::testutil::SetGen;

    use super::*;

    #[test]
    fn test_sanity() {
        let mut splinter = Splinter::default();

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

        dbg!(splinter);
    }

    #[test]
    fn test_wat() {
        let mut set_gen = SetGen::new(0xDEADBEEF);
        let set = set_gen.distributed(1, 256, 1, 17);
        let baseline_size = set.len() * 4;
        let mut splinter = Splinter::default();
        for v in set {
            assert!(splinter.insert(v));
        }
        dbg!(&splinter, splinter.serialized_size(), baseline_size);
    }

    #[test]
    fn test_expected_compression_v2() {
        fn to_roaring(set: impl Iterator<Item = u32>) -> Vec<u8> {
            let mut buf = std::io::Cursor::new(Vec::new());
            let mut bmp = RoaringBitmap::from_sorted_iter(set).unwrap();
            bmp.optimize();
            bmp.serialize_into(&mut buf).unwrap();
            buf.into_inner()
        }

        struct Report {
            name: &'static str,
            baseline: usize,
            //        (actual, expected)
            splinter: (usize, usize),
            roaring: (usize, usize),
        }

        let mut reports = vec![];

        let mut run_test = |name: &'static str,
                            set: Vec<u32>,
                            expected_set_size: usize,
                            expected_splinter: usize,
                            expected_roaring: usize| {
            println!("running test: {name}");

            assert_eq!(set.len(), expected_set_size, "Set size mismatch");

            let splinter = Splinter::from_iter(set.iter().copied());
            let roaring = to_roaring(set.iter().copied());

            const SPLINTER_HEADER_SIZE: usize = 8;

            reports.push(Report {
                name,
                baseline: set.len() * std::mem::size_of::<u32>(),
                splinter: (
                    splinter.serialized_size() + SPLINTER_HEADER_SIZE,
                    expected_splinter,
                ),
                roaring: (roaring.len(), expected_roaring),
            });
        };

        let mut set_gen = SetGen::new(0xDEAD_BEEF);

        // empty splinter
        run_test("empty", vec![], 0, 8, 8);

        // 1 element in set
        let set = set_gen.distributed(1, 1, 1, 1);
        run_test("1 element", set, 1, 12, 18);

        // 1 fully dense block
        let set = set_gen.distributed(1, 1, 1, 256);
        run_test("1 dense block", set, 256, 22, 15);

        // 1 half full block
        let set = set_gen.distributed(1, 1, 1, 128);
        run_test("1 half full block", set, 128, 53, 247);

        // 1 sparse block
        let set = set_gen.distributed(1, 1, 1, 16);
        run_test("1 sparse block", set, 16, 72, 48);

        // 8 half full blocks
        let set = set_gen.distributed(1, 1, 8, 128);
        run_test("8 half full blocks", set, 1024, 298, 2064);

        // 8 sparse blocks
        let set = set_gen.distributed(1, 1, 8, 2);
        run_test("8 sparse blocks", set, 16, 72, 48);

        // 64 half full blocks
        let set = set_gen.distributed(4, 4, 4, 128);
        run_test("64 half full blocks", set, 8192, 2348, 16486);

        // 64 sparse blocks
        let set = set_gen.distributed(4, 4, 4, 2);
        run_test("64 sparse blocks", set, 128, 412, 392);

        // 256 half full blocks
        let set = set_gen.distributed(4, 8, 8, 128);
        run_test("256 half full blocks", set, 32768, 9148, 65520);

        // 256 sparse blocks
        let set = set_gen.distributed(4, 8, 8, 2);
        run_test("256 sparse blocks", set, 512, 1476, 1288);

        // 512 half full blocks
        let set = set_gen.distributed(8, 8, 8, 128);
        run_test("512 half full blocks", set, 65536, 18288, 130742);

        // 512 sparse blocks
        let set = set_gen.distributed(8, 8, 8, 2);
        run_test("512 sparse blocks", set, 1024, 3120, 2568);

        // the rest of the compression tests use 4k elements
        let elements = 4096;

        // fully dense splinter
        let set = set_gen.distributed(1, 1, 16, 256);
        run_test("fully dense", set, elements, 82, 75);

        // 128 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 32, 128);
        run_test("128/block; dense", set, elements, 1138, 8195);

        // 32 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 128, 32);
        run_test("32/block; dense", set, elements, 4498, 8208);

        // 16 element per block; dense low partitions
        let set = set_gen.distributed(1, 1, 256, 16);
        run_test("16/block; dense", set, elements, 4882, 8208);

        // 128 elements per block; sparse mid partitions
        let set = set_gen.distributed(1, 32, 1, 128);
        run_test("128/block; sparse mid", set, elements, 1293, 8300);

        // 128 elements per block; sparse high partitions
        let set = set_gen.distributed(32, 1, 1, 128);
        run_test("128/block; sparse high", set, elements, 1448, 8290);

        // 1 element per block; sparse mid partitions
        let set = set_gen.distributed(1, 256, 16, 1);
        run_test("1/block; sparse mid", set, elements, 12301, 10248);

        // 1 element per block; sparse high partitions
        let set = set_gen.distributed(256, 16, 1, 1);
        run_test("1/block; sparse high", set, elements, 13576, 40968);

        // 1/block; spread low
        let set = set_gen.dense(1, 16, 256, 1);
        run_test("1/block; spread low", set, elements, 8285, 8328);

        // each partition is dense
        let set = set_gen.dense(8, 8, 8, 8);
        run_test("dense throughout", set, elements, 6000, 2700);

        // the lowest partitions are dense
        let set = set_gen.dense(1, 1, 64, 64);
        run_test("dense low", set, elements, 2258, 267);

        // the mid and low partitions are dense
        let set = set_gen.dense(1, 32, 16, 8);
        run_test("dense mid/low", set, elements, 5805, 2376);

        // fully random sets of varying sizes
        run_test("random/32", set_gen.random(32), 32, 136, 328);
        run_test("random/256", set_gen.random(256), 256, 1032, 2560);
        run_test("random/1024", set_gen.random(1024), 1024, 4335, 10168);
        run_test("random/4096", set_gen.random(4096), 4096, 13576, 39952);
        run_test("random/16384", set_gen.random(16384), 16384, 50440, 148600);
        run_test("random/65535", set_gen.random(65535), 65535, 197893, 462190);

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

        assert!(!fail_test, "compression test failed");
    }
}
