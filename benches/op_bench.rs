use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::seq::index::{self, IndexVec};
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
    const SET_LEN: usize = 1024;

    fn makeset() -> IndexVec {
        index::sample(&mut rand::rng(), 16384, SET_LEN)
    }

    let mut group = c.benchmark_group("insert_many");

    group.bench_function("roaring/warm", |b| {
        let mut bitmap = RoaringBitmap::default();
        b.iter_batched(
            makeset,
            |set| {
                for i in set {
                    bitmap.insert(black_box(i as u32));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("splinter/warm", |b| {
        let mut bitmap = Splinter::EMPTY;
        b.iter_batched(
            makeset,
            |set| {
                for i in set {
                    bitmap.insert(black_box(i as u32));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("roaring/cold", |b| {
        b.iter_batched(
            makeset,
            |set| {
                let mut bitmap = RoaringBitmap::default();
                for i in set {
                    bitmap.insert(black_box(i as u32));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("splinter/cold", |b| {
        b.iter_batched(
            makeset,
            |set| {
                let mut bitmap = Splinter::default();
                for i in set {
                    bitmap.insert(black_box(i as u32));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn benchmark_position(c: &mut Criterion) {
    let cardinalities = [4u32, 16, 64, 256, 1024, 4096, 16384];

    let mut group = c.benchmark_group("position");
    let mut set_gen = SetGen::new(0xDEAD_BEEF);

    for &cardinality in &cardinalities {
        let set = set_gen.random(cardinality as usize);
        let lookup = set[(set.len() / 3) as usize];

        group.bench_function(BenchmarkId::new("splinter", cardinality), |b| {
            let splinter = mksplinter(set.clone());
            assert!(splinter.position(black_box(lookup)).is_some());
            b.iter(|| splinter.position(black_box(lookup)))
        });

        group.bench_function(BenchmarkId::new("splinter optimized", cardinality), |b| {
            let mut splinter = mksplinter(set.clone());
            splinter.optimize();
            assert!(splinter.position(black_box(lookup)).is_some());
            b.iter(|| splinter.position(black_box(lookup)))
        });

        group.bench_function(BenchmarkId::new("splinter ref", cardinality), |b| {
            let splinter = mksplinter_ref(set.clone());
            assert!(splinter.position(black_box(lookup)).is_some());
            b.iter(|| splinter.position(black_box(lookup)))
        });

        group.bench_function(
            BenchmarkId::new("splinter ref optimized", cardinality),
            |b| {
                let mut splinter = mksplinter(set.clone());
                splinter.optimize();
                let splinter = splinter.encode_to_splinter_ref();
                assert!(splinter.position(black_box(lookup)).is_some());
                b.iter(|| splinter.position(black_box(lookup)))
            },
        );
    }

    group.finish();
}

fn benchmark_contains_vs_position(c: &mut Criterion) {
    const CARDINALITY: usize = 16384;

    let mut set_gen = SetGen::new(0xDEAD_BEEF);
    let set = set_gen.random(CARDINALITY);
    let needle_hit = set[set.len() / 3];
    let needle_miss = loop {
        let candidate = rand::random::<u32>();
        if !set.contains(&candidate) {
            break candidate;
        }
    };

    let mut group = c.benchmark_group("contains_vs_position");

    // Splinter
    let splinter = mksplinter(set.clone());
    assert!(splinter.contains(needle_hit));
    assert!(!splinter.contains(needle_miss));

    group.bench_function("splinter/contains/hit", |b| {
        b.iter(|| splinter.contains(black_box(needle_hit)))
    });
    group.bench_function("splinter/contains/miss", |b| {
        b.iter(|| splinter.contains(black_box(needle_miss)))
    });
    group.bench_function("splinter/position/hit", |b| {
        b.iter(|| splinter.position(black_box(needle_hit)))
    });
    group.bench_function("splinter/position/miss", |b| {
        b.iter(|| splinter.position(black_box(needle_miss)))
    });

    // SplinterRef
    let splinter_ref = mksplinter_ref(set.clone());
    assert!(splinter_ref.contains(needle_hit));
    assert!(!splinter_ref.contains(needle_miss));

    group.bench_function("splinter_ref/contains/hit", |b| {
        b.iter(|| splinter_ref.contains(black_box(needle_hit)))
    });
    group.bench_function("splinter_ref/contains/miss", |b| {
        b.iter(|| splinter_ref.contains(black_box(needle_miss)))
    });
    group.bench_function("splinter_ref/position/hit", |b| {
        b.iter(|| splinter_ref.position(black_box(needle_hit)))
    });
    group.bench_function("splinter_ref/position/miss", |b| {
        b.iter(|| splinter_ref.position(black_box(needle_miss)))
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_contains,
    benchmark_insert,
    benchmark_position,
    benchmark_contains_vs_position
);
criterion_main!(benches);
