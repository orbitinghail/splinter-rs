use std::hint::black_box;
use std::ops::{BitAnd, BitOr, BitXor, Sub};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use roaring::RoaringBitmap;

use splinter_rs::{Cut, Splinter, SplinterRef, testutil::SetGen};

const SEED: u64 = 0xDEAD_BEEF;

fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter {
    Splinter::from_iter(values)
}

fn mksplinter_ref(values: impl IntoIterator<Item = u32>) -> SplinterRef<Bytes> {
    mksplinter(values).encode_to_splinter_ref()
}

fn mkroaring(values: impl IntoIterator<Item = u32>) -> RoaringBitmap {
    values.into_iter().collect()
}

macro_rules! bench_bitop {
    ($name:ident, $group:literal, $op:path) => {
        fn $name(c: &mut Criterion) {
            let cardinalities_left = [64, 4096, 16384];
            let cardinalities_right = [64, 4096, 16384];

            let mut group = c.benchmark_group($group);
            let mut set_gen = SetGen::new(SEED);

            for &cardinality_left in &cardinalities_left {
                for &cardinality_right in &cardinalities_right {
                    let set_a = set_gen.random(cardinality_left as usize);
                    let set_b = set_gen.random(cardinality_right as usize);

                    group.bench_function(
                        BenchmarkId::new(
                            "splinter",
                            format!("{cardinality_left}/{cardinality_right}"),
                        ),
                        |b| {
                            b.iter_batched(
                                || (mksplinter(set_a.clone()), mksplinter(set_b.clone())),
                                |(a, b)| black_box($op(a, b)),
                                criterion::BatchSize::SmallInput,
                            );
                        },
                    );

                    group.bench_function(
                        BenchmarkId::new(
                            "splinter_ref",
                            format!("{cardinality_left}/{cardinality_right}"),
                        ),
                        |b| {
                            let b_ref = mksplinter_ref(set_b.clone());
                            b.iter_batched(
                                || mksplinter(set_a.clone()),
                                |a| black_box($op(a, &b_ref)),
                                criterion::BatchSize::SmallInput,
                            );
                        },
                    );

                    group.bench_function(
                        BenchmarkId::new(
                            "roaring",
                            format!("{cardinality_left}/{cardinality_right}"),
                        ),
                        |b| {
                            b.iter_batched(
                                || (mkroaring(set_a.clone()), mkroaring(set_b.clone())),
                                |(a, b)| black_box($op(a, b)),
                                criterion::BatchSize::SmallInput,
                            );
                        },
                    );
                }
            }

            group.finish();
        }
    };
}

bench_bitop!(benchmark_bitor, "bitor", BitOr::bitor);
bench_bitop!(benchmark_bitand, "bitand", BitAnd::bitand);
bench_bitop!(benchmark_bitxor, "bitxor", BitXor::bitxor);
bench_bitop!(benchmark_sub, "sub", Sub::sub);

fn benchmark_cut(c: &mut Criterion) {
    let cardinalities_left = [64, 4096, 16384];
    let cardinalities_right = [64, 4096, 16384];

    let mut group = c.benchmark_group("cut");
    let mut set_gen = SetGen::new(SEED);

    for &cardinality_left in &cardinalities_left {
        for &cardinality_right in &cardinalities_right {
            let set_a = set_gen.random(cardinality_left as usize);
            let set_b = set_gen.random(cardinality_right as usize);

            group.bench_function(
                BenchmarkId::new(
                    "splinter",
                    format!("{cardinality_left}/{cardinality_right}"),
                ),
                |b| {
                    let other = mksplinter(set_b.clone());
                    b.iter_batched(
                        || mksplinter(set_a.clone()),
                        |mut a| {
                            let intersection = a.cut(&other);
                            black_box((a, intersection))
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            group.bench_function(
                BenchmarkId::new(
                    "splinter_ref",
                    format!("{cardinality_left}/{cardinality_right}"),
                ),
                |b| {
                    let other_ref = mksplinter_ref(set_b.clone());
                    b.iter_batched(
                        || mksplinter(set_a.clone()),
                        |mut a| {
                            let intersection = a.cut(&other_ref);
                            black_box((a, intersection))
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            // Equivalent roaring operation: intersection + difference
            group.bench_function(
                BenchmarkId::new("roaring", format!("{cardinality_left}/{cardinality_right}")),
                |b| {
                    let other = mkroaring(set_b.clone());
                    b.iter_batched(
                        || mkroaring(set_a.clone()),
                        |mut a| {
                            let intersection = &a & &other;
                            a -= &other;
                            black_box((a, intersection))
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_bitor,
    benchmark_bitand,
    benchmark_bitxor,
    benchmark_sub,
    benchmark_cut
);
criterion_main!(benches);
