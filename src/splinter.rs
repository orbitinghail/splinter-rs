use std::{fmt::Debug, ops::Deref};

use bytes::Bytes;

use crate::{
    Cut, Encodable, Merge, Optimizable, SplinterRef,
    codec::{encoder::Encoder, footer::Footer},
    level::High,
    partition::Partition,
    traits::{PartitionRead, PartitionWrite},
};

#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct Splinter(Partition<High>);

static_assertions::const_assert_eq!(std::mem::size_of::<Splinter>(), 40);

impl Splinter {
    pub fn encode_to_splinter_ref(&self) -> SplinterRef<Bytes> {
        SplinterRef { data: self.encode_to_bytes() }
    }
}

impl FromIterator<u32> for Splinter {
    fn from_iter<I: IntoIterator<Item = u32>>(iter: I) -> Self {
        Self(Partition::<High>::from_iter(iter))
    }
}

impl PartitionRead<High> for Splinter {
    #[inline]
    fn cardinality(&self) -> usize {
        self.0.cardinality()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    fn contains(&self, value: u32) -> bool {
        self.0.contains(value)
    }

    #[inline]
    fn rank(&self, value: u32) -> usize {
        self.0.rank(value)
    }

    #[inline]
    fn select(&self, idx: usize) -> Option<u32> {
        self.0.select(idx)
    }

    #[inline]
    fn last(&self) -> Option<u32> {
        self.0.last()
    }

    #[inline]
    fn iter(&self) -> impl Iterator<Item = u32> {
        self.0.iter()
    }
}

impl PartitionWrite<High> for Splinter {
    #[inline]
    fn insert(&mut self, value: u32) -> bool {
        self.0.insert(value)
    }

    #[inline]
    fn remove(&mut self, value: u32) -> bool {
        self.0.remove(value)
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

impl Merge for Splinter {
    fn merge(&mut self, rhs: &Self) {
        self.0.merge(&rhs.0)
    }
}

impl<B: Deref<Target = [u8]>> Merge<SplinterRef<B>> for Splinter {
    fn merge(&mut self, rhs: &SplinterRef<B>) {
        self.0.merge(&rhs.load_unchecked())
    }
}

impl Cut for Splinter {
    type Out = Self;

    fn cut(&mut self, rhs: &Self) -> Self::Out {
        Self(self.0.cut(&rhs.0))
    }
}

impl<B: Deref<Target = [u8]>> Cut<SplinterRef<B>> for Splinter {
    type Out = Self;

    fn cut(&mut self, rhs: &SplinterRef<B>) -> Self::Out {
        Self(self.0.cut(&rhs.load_unchecked()))
    }
}

impl<B: Deref<Target = [u8]>> PartialEq<SplinterRef<B>> for Splinter {
    #[inline]
    fn eq(&self, other: &SplinterRef<B>) -> bool {
        self.0 == other.load_unchecked()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        codec::Encodable,
        testutil::{SetGen, analyze_compression_patterns, mksplinter, ratio_to_marks},
        traits::Optimizable,
    };
    use quickcheck_macros::quickcheck;
    use roaring::RoaringBitmap;

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

        splinter.optimize();

        dbg!(splinter);
    }

    #[test]
    fn test_wat() {
        let mut set_gen = SetGen::new(0xDEAD_BEEF);
        let set = set_gen.random(1024);
        let baseline_size = set.len() * 4;

        let mut splinter = Splinter::from_iter(set.iter().copied());
        splinter.optimize();

        dbg!(&splinter, splinter.encoded_size(), baseline_size);
        itertools::assert_equal(splinter.iter(), set.into_iter());
    }

    /// This is a regression test for a bug in the SplinterRef encoding. The bug
    /// was that we used LittleEndian encoded values to store unaligned values,
    /// which sort in reverse order from what we expect.
    #[test]
    fn test_contains_bug() {
        let mut set_gen = SetGen::new(0xDEAD_BEEF);
        let set = set_gen.random(1024);
        let lookup = set[(set.len() / 3) as usize];
        let splinter = mksplinter(&set).encode_to_splinter_ref();
        assert!(splinter.contains(lookup))
    }

    #[quickcheck]
    fn test_splinter_quickcheck(set: Vec<u32>) -> bool {
        let splinter = mksplinter(&set);
        if set.is_empty() {
            !splinter.contains(123)
        } else {
            let lookup = set[set.len() / 3];
            splinter.contains(lookup)
        }
    }

    #[quickcheck]
    fn test_splinter_opt_quickcheck(set: Vec<u32>) -> bool {
        let mut splinter = mksplinter(&set);
        splinter.optimize();
        if set.is_empty() {
            !splinter.contains(123)
        } else {
            let lookup = set[set.len() / 3];
            splinter.contains(lookup)
        }
    }

    #[quickcheck]
    fn test_splinter_ref_quickcheck(set: Vec<u32>) -> bool {
        let splinter = mksplinter(&set).encode_to_splinter_ref();
        if set.is_empty() {
            !splinter.contains(123)
        } else {
            let lookup = set[set.len() / 3];
            splinter.contains(lookup)
        }
    }

    #[quickcheck]
    fn test_splinter_opt_ref_quickcheck(set: Vec<u32>) -> bool {
        let mut splinter = mksplinter(&set);
        splinter.optimize();
        let splinter = splinter.encode_to_splinter_ref();
        if set.is_empty() {
            !splinter.contains(123)
        } else {
            let lookup = set[set.len() / 3];
            splinter.contains(lookup)
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

            let mut splinter = Splinter::from_iter(set.clone());
            splinter.optimize();
            itertools::assert_equal(splinter.iter(), set.iter().copied());

            let expected_size = splinter.encoded_size();
            let splinter = splinter.encode_to_bytes();

            assert_eq!(
                splinter.len(),
                expected_size,
                "actual encoded size does not match declared encoded size"
            );

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
        run_test("1 dense block", set, 256, 25, 15);

        // 1 half full block
        let set = set_gen.distributed(1, 1, 1, 128);
        run_test("1 half full block", set, 128, 63, 255);

        // 1 sparse block
        let set = set_gen.distributed(1, 1, 1, 16);
        run_test("1 sparse block", set, 16, 81, 48);

        // 8 half full blocks
        let set = set_gen.distributed(1, 1, 8, 128);
        run_test("8 half full blocks", set, 1024, 315, 2003);

        // 8 sparse blocks
        let set = set_gen.distributed(1, 1, 8, 2);
        run_test("8 sparse blocks", set, 16, 81, 48);

        // 64 half full blocks
        let set = set_gen.distributed(4, 4, 4, 128);
        run_test("64 half full blocks", set, 8192, 2442, 16452);

        // 64 sparse blocks
        let set = set_gen.distributed(4, 4, 4, 2);
        run_test("64 sparse blocks", set, 128, 434, 392);

        // 256 half full blocks
        let set = set_gen.distributed(4, 8, 8, 128);
        run_test("256 half full blocks", set, 32768, 9450, 65580);

        // 256 sparse blocks
        let set = set_gen.distributed(4, 8, 8, 2);
        run_test("256 sparse blocks", set, 512, 1290, 1288);

        // 512 half full blocks
        let set = set_gen.distributed(8, 8, 8, 128);
        run_test("512 half full blocks", set, 65536, 18886, 130810);

        // 512 sparse blocks
        let set = set_gen.distributed(8, 8, 8, 2);
        run_test("512 sparse blocks", set, 1024, 2566, 2568);

        // the rest of the compression tests use 4k elements
        let elements = 4096;

        // fully dense splinter
        let set = set_gen.distributed(1, 1, 16, 256);
        run_test("fully dense", set, elements, 80, 63);

        // 128 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 32, 128);
        run_test("128/block; dense", set, elements, 1179, 8208);

        // 32 elements per block; dense partitions
        let set = set_gen.distributed(1, 1, 128, 32);
        run_test("32/block; dense", set, elements, 4539, 8208);

        // 16 element per block; dense low partitions
        let set = set_gen.distributed(1, 1, 256, 16);
        run_test("16/block; dense", set, elements, 5147, 8208);

        // 128 elements per block; sparse mid partitions
        let set = set_gen.distributed(1, 32, 1, 128);
        run_test("128/block; sparse mid", set, elements, 1365, 8282);

        // 128 elements per block; sparse high partitions
        let set = set_gen.distributed(32, 1, 1, 128);
        run_test("128/block; sparse high", set, elements, 1582, 8224);

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
        run_test("dense throughout", set, elements, 4113, 2700);

        // the lowest partitions are dense
        let set = set_gen.dense(1, 1, 64, 64);
        run_test("dense low", set, elements, 529, 267);

        // the mid and low partitions are dense
        let set = set_gen.dense(1, 32, 16, 8);
        run_test("dense mid/low", set, elements, 4113, 2376);

        // fully random sets of varying sizes
        run_test("random/32", set_gen.random(32), 32, 145, 328);
        run_test("random/256", set_gen.random(256), 256, 1041, 2552);
        run_test("random/1024", set_gen.random(1024), 1024, 4113, 10152);
        run_test("random/4096", set_gen.random(4096), 4096, 14350, 39792);
        run_test("random/16384", set_gen.random(16384), 16384, 51214, 148832);
        run_test("random/65535", set_gen.random(65535), 65535, 198667, 462838);

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
