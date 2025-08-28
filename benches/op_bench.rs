use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use roaring::RoaringBitmap;
use std::hint::black_box;

use splinter_rs::{
    Optimizable, PartitionRead, PartitionWrite, Splinter, SplinterRef, testutil::SetGen,
};

fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter {
    Splinter::from_iter(values)
}

fn mksplinter_ref(values: impl IntoIterator<Item = u32>) -> SplinterRef<Bytes> {
    mksplinter(values).encode_to_splinter_ref()
}

fn benchmark_contains(c: &mut Criterion) {
    let cardinalities = [4u32, 16, 64, 256, 1024, 4096, 16384];

    let mut group = c.benchmark_group("contains");
    let mut set_gen = SetGen::new(0xDEAD_BEEF);

    for &cardinality in &cardinalities {
        let set = set_gen.random(cardinality as usize);
        // we want to lookup the cardinality/3th element
        let lookup = set[(set.len() / 3) as usize];

        group.bench_function(BenchmarkId::new("splinter", cardinality), |b| {
            let splinter = mksplinter(set.clone());
            assert!(splinter.contains(black_box(lookup)), "lookup {}", lookup);
            b.iter(|| splinter.contains(black_box(lookup)))
        });

        group.bench_function(BenchmarkId::new("splinter optimized", cardinality), |b| {
            let mut splinter = mksplinter(set.clone());
            splinter.optimize();
            assert!(splinter.contains(black_box(lookup)), "lookup {}", lookup);
            b.iter(|| splinter.contains(black_box(lookup)))
        });

        group.bench_function(BenchmarkId::new("splinter ref", cardinality), |b| {
            let splinter = mksplinter_ref(set.clone());
            assert!(splinter.contains(black_box(lookup)), "lookup {}", lookup);
            b.iter(|| splinter.contains(black_box(lookup)))
        });

        group.bench_function(
            BenchmarkId::new("splinter ref optimized", cardinality),
            |b| {
                let mut splinter = mksplinter(set.clone());
                splinter.optimize();
                let splinter = splinter.encode_to_splinter_ref();
                assert!(splinter.contains(black_box(lookup)), "lookup {}", lookup);
                b.iter(|| splinter.contains(black_box(lookup)))
            },
        );

        group.bench_function(BenchmarkId::new("roaring", cardinality), |b| {
            let bitmap = RoaringBitmap::from_sorted_iter(set.clone()).unwrap();
            assert!(bitmap.contains(black_box(lookup)), "lookup {}", lookup);
            b.iter(|| bitmap.contains(black_box(lookup)))
        });

        group.bench_function(BenchmarkId::new("roaring optimized", cardinality), |b| {
            let mut bitmap = RoaringBitmap::from_sorted_iter(set.clone()).unwrap();
            bitmap.optimize();
            assert!(bitmap.contains(black_box(lookup)), "lookup {}", lookup);
            b.iter(|| bitmap.contains(black_box(lookup)))
        });
    }

    group.finish();
}

fn benchmark_insert(c: &mut Criterion) {
    const MAGIC: u32 = 513;

    let mut group = c.benchmark_group("insert");

    group.bench_function("roaring/warm", |b| {
        let mut roaring_bitmap = RoaringBitmap::default();
        b.iter(|| roaring_bitmap.insert(black_box(MAGIC)))
    });

    group.bench_function("splinter/warm", |b| {
        let mut splinter = Splinter::EMPTY;
        b.iter(|| splinter.insert(black_box(MAGIC)))
    });

    group.bench_function("roaring/cold", |b| {
        b.iter(|| RoaringBitmap::default().insert(black_box(MAGIC)))
    });

    group.bench_function("splinter/cold", |b| {
        b.iter(|| Splinter::default().insert(black_box(MAGIC)))
    });

    group.finish();
}

criterion_group!(benches, benchmark_contains, benchmark_insert);
criterion_main!(benches);
