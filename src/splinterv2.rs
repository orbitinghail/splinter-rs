pub mod codec;
pub mod count;
pub mod level;
pub mod never;
pub mod ops;
pub mod partition;
pub mod segment;
pub mod traits;

use std::fmt::Debug;
use std::ops::Deref;

use bytes::Bytes;
use zerocopy::FromBytes;

use crate::splinterv2::{
    codec::{DecodeErr, encoder::Encoder, footer::Footer, partition_ref::PartitionRef},
    level::High,
    partition::Partition,
};

pub use crate::splinterv2::codec::Encodable;
pub use crate::splinterv2::traits::{Optimizable, PartitionRead, PartitionWrite};

#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct SplinterV2(Partition<High>);

static_assertions::const_assert_eq!(std::mem::size_of::<SplinterV2>(), 40);

impl SplinterV2 {
    pub fn encode_to_splinter_ref(&self) -> SplinterRefV2<Bytes> {
        SplinterRefV2 { data: self.encode_to_bytes() }
    }
}

impl FromIterator<u32> for SplinterV2 {
    fn from_iter<I: IntoIterator<Item = u32>>(iter: I) -> Self {
        Self(Partition::<High>::from_iter(iter))
    }
}

impl PartitionRead<High> for SplinterV2 {
    fn cardinality(&self) -> usize {
        self.0.cardinality()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn contains(&self, value: u32) -> bool {
        self.0.contains(value)
    }

    fn rank(&self, value: u32) -> usize {
        self.0.rank(value)
    }

    fn select(&self, idx: usize) -> Option<u32> {
        self.0.select(idx)
    }

    fn last(&self) -> Option<u32> {
        self.0.last()
    }

    fn iter(&self) -> impl Iterator<Item = u32> {
        self.0.iter()
    }
}

impl PartitionWrite<High> for SplinterV2 {
    fn insert(&mut self, value: u32) -> bool {
        self.0.insert(value)
    }

    fn remove(&mut self, value: u32) -> bool {
        self.0.remove(value)
    }
}

impl Encodable for SplinterV2 {
    fn encoded_size(&self) -> usize {
        self.0.encoded_size() + std::mem::size_of::<Footer>()
    }

    fn encode<B: bytes::BufMut>(&self, encoder: &mut Encoder<B>) {
        self.0.encode(encoder);
        encoder.write_footer();
    }
}

impl Optimizable for SplinterV2 {
    fn optimize(&mut self) {
        self.0.optimize();
    }
}

pub struct SplinterRefV2<B> {
    data: B,
}

impl<B: Deref<Target = [u8]>> Debug for SplinterRefV2<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SplinterRefV2")
            .field(&self.load_unchecked())
            .finish()
    }
}

impl<B: Deref<Target = [u8]>> SplinterRefV2<B> {
    pub fn from_bytes(data: B) -> Result<Self, DecodeErr> {
        if data.len() < Footer::SIZE {
            return Err(DecodeErr::Length);
        }
        let (partitions, footer) = data.split_at(data.len() - Footer::SIZE);
        Footer::ref_from_bytes(footer)?.validate(partitions)?;
        PartitionRef::<High>::from_suffix(partitions)?;
        Ok(Self { data })
    }

    pub fn inner(&self) -> &[u8] {
        &self.data
    }

    pub fn into_inner(self) -> B {
        self.data
    }

    fn load_unchecked(&self) -> PartitionRef<'_, High> {
        let without_footer = &self.data[..(self.data.len() - Footer::SIZE)];
        PartitionRef::from_suffix(without_footer).unwrap()
    }
}

impl<B: Deref<Target = [u8]>> PartitionRead<High> for SplinterRefV2<B> {
    fn cardinality(&self) -> usize {
        self.load_unchecked().cardinality()
    }

    fn is_empty(&self) -> bool {
        self.load_unchecked().is_empty()
    }

    fn contains(&self, value: u32) -> bool {
        self.load_unchecked().contains(value)
    }

    fn rank(&self, value: u32) -> usize {
        self.load_unchecked().rank(value)
    }

    fn select(&self, idx: usize) -> Option<u32> {
        self.load_unchecked().select(idx)
    }

    fn last(&self) -> Option<u32> {
        self.load_unchecked().last()
    }

    fn iter(&self) -> impl Iterator<Item = u32> {
        self.load_unchecked().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        splinterv2::{codec::Encodable, traits::Optimizable},
        testutil::{SetGen, analyze_compression_patterns, ratio_to_marks},
    };
    use roaring::RoaringBitmap;

    #[test]
    fn test_sanity() {
        let mut splinter = SplinterV2::default();

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

        dbg!(splinter);
    }

    #[test]
    fn test_wat() {
        let mut set_gen = SetGen::new(0xDEADBEEF);
        let set = set_gen.distributed(1, 1, 16, 256);
        let baseline_size = set.len() * 4;

        let mut splinter = SplinterV2::from_iter(set.iter().copied());
        splinter.optimize();

        dbg!(&splinter, splinter.encoded_size(), baseline_size);
        itertools::assert_equal(splinter.iter(), set.into_iter());
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

            let mut splinter = SplinterV2::from_iter(set.clone());
            splinter.optimize();
            itertools::assert_equal(splinter.iter(), set.iter().copied());

            let splinter = splinter.encode_to_bytes();
            let roaring = to_roaring(set.iter().copied());

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
        run_test("empty", vec![], 0, 13, 8);

        // 1 element in set
        let set = set_gen.distributed(1, 1, 1, 1);
        run_test("1 element", set, 1, 21, 18);

        // 1 fully dense block
        let set = set_gen.distributed(1, 1, 1, 256);
        run_test("1 dense block", set, 256, 31, 15);

        // 1 half full block
        let set = set_gen.distributed(1, 1, 1, 128);
        run_test("1 half full block", set, 128, 63, 247);

        // 1 sparse block
        let set = set_gen.distributed(1, 1, 1, 16);
        run_test("1 sparse block", set, 16, 81, 48);

        // 8 half full blocks
        let set = set_gen.distributed(1, 1, 8, 128);
        run_test("8 half full blocks", set, 1024, 315, 2064);

        // 8 sparse blocks
        let set = set_gen.distributed(1, 1, 8, 2);
        run_test("8 sparse blocks", set, 16, 81, 48);

        // 64 half full blocks
        let set = set_gen.distributed(4, 4, 4, 128);
        run_test("64 half full blocks", set, 8192, 2442, 16486);

        // 64 sparse blocks
        let set = set_gen.distributed(4, 4, 4, 2);
        run_test("64 sparse blocks", set, 128, 434, 392);

        // 256 half full blocks
        let set = set_gen.distributed(4, 8, 8, 128);
        run_test("256 half full blocks", set, 32768, 9450, 65520);

        // 256 sparse blocks
        let set = set_gen.distributed(4, 8, 8, 2);
        run_test("256 sparse blocks", set, 512, 1290, 1288);

        // 512 half full blocks
        let set = set_gen.distributed(8, 8, 8, 128);
        run_test("512 half full blocks", set, 65536, 18886, 130742);

        // 512 sparse blocks
        let set = set_gen.distributed(8, 8, 8, 2);
        run_test("512 sparse blocks", set, 1024, 2566, 2568);

        // the rest of the compression tests use 4k elements
        let elements = 4096;

        // fully dense splinter
        let set = set_gen.distributed(1, 1, 16, 256);
        run_test("fully dense", set, elements, 91, 75);

        // 128 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 32, 128);
        run_test("128/block; dense", set, elements, 1179, 8195);

        // 32 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 128, 32);
        run_test("32/block; dense", set, elements, 4539, 8208);

        // 16 element per block; dense low partitions
        let set = set_gen.distributed(1, 1, 256, 16);
        run_test("16/block; dense", set, elements, 5147, 8208);

        // 128 elements per block; sparse mid partitions
        let set = set_gen.distributed(1, 32, 1, 128);
        run_test("128/block; sparse mid", set, elements, 1365, 8300);

        // 128 elements per block; sparse high partitions
        let set = set_gen.distributed(32, 1, 1, 128);
        run_test("128/block; sparse high", set, elements, 1582, 8290);

        // 1 element per block; sparse mid partitions
        let set = set_gen.distributed(1, 256, 16, 1);
        run_test("1/block; sparse mid", set, elements, 9749, 10248);

        // 1 element per block; sparse high partitions
        let set = set_gen.distributed(256, 16, 1, 1);
        run_test("1/block; sparse high", set, elements, 14350, 40968);

        // 1/block; spread low
        let set = set_gen.dense(1, 16, 256, 1);
        run_test("1/block; spread low", set, elements, 8325, 8328);

        // each partition is dense
        let set = set_gen.dense(8, 8, 8, 8);
        run_test("dense throughout", set, elements, 4038, 2700);

        // the lowest partitions are dense
        let set = set_gen.dense(1, 1, 64, 64);
        run_test("dense low", set, elements, 443, 267);

        // the mid and low partitions are dense
        let set = set_gen.dense(1, 32, 16, 8);
        run_test("dense mid/low", set, elements, 3797, 2376);

        // fully random sets of varying sizes
        run_test("random/32", set_gen.random(32), 32, 145, 328);
        run_test("random/256", set_gen.random(256), 256, 1041, 2560);
        run_test("random/1024", set_gen.random(1024), 1024, 5126, 10168);
        run_test("random/4096", set_gen.random(4096), 4096, 14350, 39952);
        run_test("random/16384", set_gen.random(16384), 16384, 51214, 148600);
        run_test("random/65535", set_gen.random(65535), 65535, 198667, 462190);

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
