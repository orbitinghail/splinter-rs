use std::{fmt::Debug, marker::PhantomData, ops::Bound};

use bytes::Bytes;
use itertools::{Itertools, assert_equal};
use num::{CheckedAdd, Saturating, traits::ConstOne};
use rand::{Rng, SeedableRng, seq::index};

use crate::{
    Splinter, SplinterRead, SplinterRef, SplinterWrite,
    splinterv2::{
        Partition, PartitionRead, level::Level, partition::PartitionKind, traits::TruncateFrom,
    },
    util::CopyToOwned,
};

pub fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter {
    let mut splinter = Splinter::default();
    for i in values {
        splinter.insert(i);
    }
    splinter
}

pub fn mksplinter_ref(values: impl IntoIterator<Item = u32>) -> SplinterRef<Bytes> {
    SplinterRef::from_bytes(mksplinter(values).serialize_to_bytes()).unwrap()
}

/// Create a pair of `Splinter` and `SplinterRef` from the same values.
pub fn mksplinters(values: impl IntoIterator<Item = u32> + Clone) -> [TestSplinter; 2] {
    let splinter = mksplinter(values.clone());
    let splinter_ref = mksplinter_ref(values);
    [
        TestSplinter::Splinter(splinter),
        TestSplinter::SplinterRef(splinter_ref),
    ]
}

pub fn check_combinations<L, R, E, F>(left: L, right: R, expected: E, test: F)
where
    L: IntoIterator<Item = u32> + Clone,
    R: IntoIterator<Item = u32> + Clone,
    E: IntoIterator<Item = u32> + Clone,
    F: Fn(TestSplinter, TestSplinter) -> Splinter,
{
    let left = mksplinters(left);
    let right = mksplinters(right);
    let expected = mksplinter(expected);
    for (lhs, rhs) in left.into_iter().cartesian_product(right) {
        let label = format!("lhs: {lhs:?}, rhs: {rhs:?}");
        let out = test(lhs, rhs);
        assert_eq!(out, expected, "{label}");
    }
}

#[derive(Clone)]
pub enum TestSplinter {
    Splinter(Splinter),
    SplinterRef(SplinterRef<Bytes>),
}

impl Debug for TestSplinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Splinter(splinter) => {
                let prefix: Vec<_> = splinter.iter().take(10).collect();
                f.debug_struct("Splinter")
                    .field("meta", splinter)
                    .field("prefix", &prefix)
                    .finish()
            }
            Self::SplinterRef(splinter) => {
                let prefix: Vec<_> = splinter.copy_to_owned().iter().take(10).collect();
                f.debug_struct("SplinterRef")
                    .field("meta", splinter)
                    .field("prefix", &prefix)
                    .finish()
            }
        }
    }
}

pub struct SetGen {
    rng: rand::rngs::StdRng,
}

impl SetGen {
    pub fn new(seed: u64) -> Self {
        let rng = rand::rngs::StdRng::seed_from_u64(seed);
        Self { rng }
    }

    pub fn distributed(&mut self, high: usize, mid: usize, low: usize, block: usize) -> Vec<u32> {
        let mut out = Vec::default();
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
        out
    }

    pub fn dense(&mut self, high: usize, mid: usize, low: usize, block: usize) -> Vec<u32> {
        let out: Vec<u32> = itertools::iproduct!(0..high, 0..mid, 0..low, 0..block)
            .map(|(a, b, c, d)| u32::from_be_bytes([a as u8, b as u8, c as u8, d as u8]))
            .collect();
        out
    }

    pub fn random(&mut self, len: usize) -> Vec<u32> {
        index::sample(&mut self.rng, u32::MAX as usize, len)
            .into_iter()
            .map(|i| i as u32)
            .sorted()
            .collect()
    }
}

/// Validate that `splinter` correctly implements [`SplinterRead`] given the
/// expected set of values.
pub fn harness_read<S>(splinter: &S, expected: &[u32])
where
    S: SplinterRead + Debug,
{
    assert_eq!(splinter.is_empty(), expected.is_empty(), "is_empty");
    assert_eq!(splinter.cardinality(), expected.len(), "cardinality");
    assert_eq!(splinter.last(), expected.last().copied(), "last");

    for key in [0u32, 1, 33, 255, 256, 1024, u32::MAX] {
        assert_eq!(
            splinter.contains(key),
            expected.contains(&key),
            "contains({key})"
        );
    }

    assert!(splinter.iter().eq(expected.iter().copied()), "iter");
    assert!(splinter.range(..).eq(expected.iter().copied()), "range(..)");

    if let (Some(&start), Some(&end)) = (expected.first(), expected.last()) {
        assert!(
            splinter
                .range(start..)
                .eq(expected.iter().copied().filter(|&v| v >= start)),
            "range(start..)"
        );
        assert!(
            splinter
                .range(..=end)
                .eq(expected.iter().copied().filter(|&v| v <= end)),
            "range(..=end)"
        );
        assert!(
            splinter
                .range(start..=end)
                .eq(expected.iter().copied().filter(|&v| v >= start && v <= end)),
            "range(start..=end)"
        );
        if start < end {
            assert!(
                splinter
                    .range(start..end)
                    .eq(expected.iter().copied().filter(|&v| v >= start && v < end)),
                "range(start..end)"
            );
        }
    }
}

/// Validate that type `W` correctly implements [`SplinterWrite`] by inserting
/// `values` and comparing the result against a baseline [`Splinter`].
pub fn harness_write<W>(values: &[u32])
where
    W: SplinterWrite + SplinterRead + Default + Debug,
{
    let mut splinter = W::default();
    assert!(splinter.is_empty(), "new splinter not empty");

    for &key in values {
        assert!(splinter.insert(key), "first insert {key}");
        assert!(!splinter.insert(key), "duplicate insert {key}");
    }

    harness_read(&splinter, values);

    let result: Vec<u32> = splinter.iter().collect();
    assert_eq!(result, values, "write result");
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
                if top.is_none_or(|(_, max)| v > max) {
                    top = Some((k, v));
                }
            }
        }

        if let Some((bytes, count)) = top {
            println!(
                "repeated 8-byte blocks: {duplicate_bytes} duplicate bytes; most common occurs {count}× (bytes {bytes:02X?})"
            );
        } else {
            println!("no duplicated 8-byte blocks");
        }
    }

    println!("analysis complete");
}

pub fn ratio_to_marks(ratio: f64) -> String {
    let magnitude = if ratio >= 1.0 { ratio } else { 1.0 / ratio };
    let marks = if magnitude >= 4.0 {
        4
    } else if magnitude >= 2.5 {
        3
    } else if magnitude >= 1.6 {
        2
    } else if magnitude >= 1.1 {
        1
    } else {
        0
    };
    if marks == 0 {
        "ok".into()
    } else {
        let mark = if ratio > 1.0 { "+" } else { "-" };
        mark.repeat(marks)
    }
}

pub struct SetGenV2<L> {
    rng: rand::rngs::StdRng,
    _phantom: PhantomData<L>,
}

impl<L: Level> SetGenV2<L> {
    pub fn new(seed: u64) -> Self {
        let rng = rand::rngs::StdRng::seed_from_u64(seed);
        Self { rng, _phantom: PhantomData }
    }

    pub fn random(&mut self, len: usize) -> Vec<L::Value> {
        index::sample(&mut self.rng, L::MAX_LEN - 1, len)
            .into_iter()
            .map(L::Value::truncate_from)
            .sorted()
            .collect()
    }

    /// Generate a random set of values such that the probability any two values
    /// are sequential is `stickyness`.
    pub fn runs(&mut self, len: usize, stickyness: f64) -> Vec<L::Value> {
        let s = stickyness.clamp(0.0, 1.0);
        let mut out = Vec::with_capacity(len);
        if len == 0 {
            return out;
        }
        // Allow worst-case growth of ~2 per step to avoid overflow.
        let max_start =
            (L::MAX_LEN - 1).saturating_sub(2usize.saturating_mul(len.saturating_sub(1)));
        let mut cur = self.rng.random_range(0..=max_start);
        out.push(L::Value::truncate_from(cur));

        for _ in 1..len {
            if self.rng.random_bool(s) {
                cur = cur.saturating_add(1);
            } else {
                // Non-sequential: jump by >=2. Use a geometric(0.5) tail for gaps.
                let mut k = 0;
                while !self.rng.random_bool(0.5) {
                    k += 1;
                }
                let gap = 2 + k; // 2,3,4,... with decreasing probability
                cur = cur.saturating_add(gap);
            }
            out.push(L::Value::truncate_from(cur));
        }
        out
    }
}

pub fn mkpartition<L: Level>(kind: PartitionKind, values: &[L::Value]) -> Partition<L> {
    let mut p = kind.build();
    for &v in values {
        p.raw_insert(v);
    }
    p
}

/// Validate that a type correctly implements [`PartitionRead`] given the
/// expected set of values. expected must be sorted.
pub fn test_partition_read<L, S>(splinter: &S, expected: &[L::Value])
where
    L: Level,
    S: PartitionRead<L> + Debug,
{
    assert_eq!(splinter.is_empty(), expected.is_empty(), "is_empty");
    assert_eq!(splinter.cardinality(), expected.len(), "cardinality");
    assert_eq!(splinter.last(), expected.last().copied(), "last");

    for &exp in expected {
        assert!(splinter.contains(exp), "contains({exp})");
    }

    if let Some(not_exp) = expected
        .last()
        .copied()
        .and_then(|v| v.checked_add(&L::Value::ONE))
    {
        assert!(!splinter.contains(not_exp), "not contains({not_exp})");
    }

    assert_equal(splinter.iter(), expected.iter().copied());

    if splinter.is_empty() {
        assert_eq!(splinter.rank(L::Value::ONE), 0);
        assert_eq!(splinter.select(0), None);
    } else {
        for idx in 0..10.min(splinter.cardinality()) {
            let selected = splinter.select(idx).unwrap();
            let rank = splinter.rank(selected);
            assert_eq!(rank - 1, idx);
        }
        assert_eq!(splinter.select(splinter.cardinality() + 1), None);
        assert_eq!(
            splinter.rank(splinter.last().unwrap()),
            splinter.cardinality()
        );
    }

    if let (Some(&start), Some(&end)) = (expected.first(), expected.last()) {
        let mid = start.saturating_add(end) / L::Value::truncate_from(2);

        let starts = [
            Bound::Unbounded,
            Bound::Included(start),
            Bound::Excluded(start),
            Bound::Included(mid),
            Bound::Excluded(mid),
        ];
        let ends = [
            Bound::Unbounded,
            Bound::Included(end),
            Bound::Excluded(end),
            Bound::Included(mid),
            Bound::Excluded(mid),
        ];

        for start in starts {
            for end in ends {
                let expected_range = expected.iter().copied().filter(|&v| {
                    (match start {
                        Bound::Included(start) => start <= v,
                        Bound::Excluded(start) => start < v,
                        Bound::Unbounded => true,
                    }) && (match end {
                        Bound::Included(end) => v <= end,
                        Bound::Excluded(end) => v < end,
                        Bound::Unbounded => true,
                    })
                });
                let splinter_range = splinter.range((start, end));
                assert_equal(splinter_range, expected_range);
            }
        }
    }
}
