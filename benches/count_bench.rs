use bitvec::{bitbox, boxed::BitBox, order::Lsb0};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use itertools::Itertools;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use splinter_rs::splinterv2::count::{count_bitmap_runs, count_runs_sorted, count_unique_sorted};
use std::hint::black_box;

fn run_sequence(rng: &mut impl Rng, size: usize, stride: usize) -> impl Iterator<Item = usize> {
    num::range_step(0, size, stride).flat_map(move |i| {
        let run_len = rng.random_range(0..stride);
        i..size.min(i + run_len)
    })
}

fn dup_sequence(rng: &mut impl Rng, size: usize, stride: usize) -> impl Iterator<Item = usize> {
    num::range_step(0, size, stride).flat_map(move |i| {
        let count = rng.random_range(0..=stride);
        std::iter::repeat_n(i, count)
    })
}

/// create a bitmap with an interesting distribution of runs
fn create_bitmap(rng: &mut impl Rng, size: usize) -> BitBox<u64, Lsb0> {
    let mut bitmap = bitbox![u64, Lsb0; 0; size];
    for i in run_sequence(rng, size, 7) {
        bitmap.set(i, true);
    }
    bitmap
}

fn benchmark_count_runs(c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(42);
    let mut group = c.benchmark_group("count_runs");

    for size in [256, 4096, 65536, 262144] {
        let bitmap = create_bitmap(&mut rng, size);
        group.bench_function(BenchmarkId::new("count_bitmap_runs", size), |b| {
            b.iter(|| black_box(count_bitmap_runs(&bitmap)))
        });

        let seq = run_sequence(&mut rng, size, 7).collect_vec();
        group.bench_function(BenchmarkId::new("count_runs_sorted", size), |b| {
            b.iter(|| black_box(count_runs_sorted(seq.iter().copied())))
        });
    }

    group.finish();
}

fn benchmark_count_unique(c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(42);
    let mut group = c.benchmark_group("count_unique_sorted");

    for size in [256, 4096, 65536, 262144] {
        let seq = dup_sequence(&mut rng, size, 7).collect_vec();
        group.bench_function(BenchmarkId::new("count_unique_sorted", size), |b| {
            b.iter(|| black_box(count_unique_sorted(seq.iter().copied())))
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_count_runs, benchmark_count_unique);
criterion_main!(benches);
