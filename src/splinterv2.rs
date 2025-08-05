pub mod count;
pub mod encode;
pub mod level;
pub mod never;
pub mod partition;
pub mod segment;
pub mod traits;

pub use crate::splinterv2::encode::Encodable;
pub use crate::splinterv2::partition::Partition;
pub use crate::splinterv2::traits::{PartitionRead, PartitionWrite};

pub type SplinterV2 = Partition<level::High>;

static_assertions::const_assert_eq!(std::mem::size_of::<SplinterV2>(), 40);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        splinterv2::{encode::Encodable, traits::Optimizable},
        testutil::SetGen,
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
        }

        let mut reports = vec![];

        let mut run_test = |name: &'static str,
                            set: Vec<u32>,
                            expected_set_size: usize,
                            expected_splinter: usize,
                            expected_roaring: usize| {
            println!("running test: {name}");

            assert_eq!(set.len(), expected_set_size, "Set size mismatch");

            let mut splinter = SplinterV2::from_iter(set.iter().copied());
            splinter.optimize();
            itertools::assert_equal(splinter.iter(), set.iter().copied());

            let roaring = to_roaring(set.iter().copied());

            const SPLINTER_HEADER_SIZE: usize = 8;

            reports.push(Report {
                name,
                baseline: set.len() * std::mem::size_of::<u32>(),
                splinter: (
                    splinter.encoded_size() + SPLINTER_HEADER_SIZE,
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
        run_test("256 sparse blocks", set, 512, 1212, 1288);

        // 512 half full blocks
        let set = set_gen.distributed(8, 8, 8, 128);
        run_test("512 half full blocks", set, 65536, 18288, 130742);

        // 512 sparse blocks
        let set = set_gen.distributed(8, 8, 8, 2);
        run_test("512 sparse blocks", set, 1024, 2416, 2568);

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
        run_test("32/block; dense", set, elements, 4402, 8208);

        // 16 element per block; dense low partitions
        let set = set_gen.distributed(1, 1, 256, 16);
        run_test("16/block; dense", set, elements, 4658, 8208);

        // 128 elements per block; sparse mid partitions
        let set = set_gen.distributed(1, 32, 1, 128);
        run_test("128/block; sparse mid", set, elements, 1293, 8300);

        // 128 elements per block; sparse high partitions
        let set = set_gen.distributed(32, 1, 1, 128);
        run_test("128/block; sparse high", set, elements, 1448, 8290);

        // 1 element per block; sparse mid partitions
        let set = set_gen.distributed(1, 256, 16, 1);
        run_test("1/block; sparse mid", set, elements, 9261, 10248);

        // 1 element per block; sparse high partitions
        let set = set_gen.distributed(256, 16, 1, 1);
        run_test("1/block; sparse high", set, elements, 13352, 40968);

        // 1/block; spread low
        let set = set_gen.dense(1, 16, 256, 1);
        run_test("1/block; spread low", set, elements, 8285, 8328);

        // each partition is dense
        let set = set_gen.dense(8, 8, 8, 8);
        run_test("dense throughout", set, elements, 2928, 2700);

        // the lowest partitions are dense
        let set = set_gen.dense(1, 1, 64, 64);
        run_test("dense low", set, elements, 306, 267);

        // the mid and low partitions are dense
        let set = set_gen.dense(1, 32, 16, 8);
        run_test("dense mid/low", set, elements, 2733, 2376);

        // fully random sets of varying sizes
        run_test("random/32", set_gen.random(32), 32, 136, 328);
        run_test("random/256", set_gen.random(256), 256, 1032, 2560);
        run_test("random/1024", set_gen.random(1024), 1024, 4116, 10168);
        run_test("random/4096", set_gen.random(4096), 4096, 13352, 39952);
        run_test("random/16384", set_gen.random(16384), 16384, 50216, 148600);
        run_test("random/65535", set_gen.random(65535), 65535, 197669, 462190);

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
