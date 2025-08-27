use std::{fmt::Debug, marker::PhantomData, ops::Bound};

use bytes::{BufMut, Bytes, BytesMut};
use itertools::{Itertools, assert_equal};
use num::{CheckedAdd, Saturating, traits::ConstOne};
use rand::{Rng, SeedableRng, rngs::StdRng, seq::index};
use zerocopy::IntoBytes;

use crate::{
    PartitionRead,
    codec::{Encodable, footer::Footer},
    level::{High, Level},
    partition::{Partition, PartitionKind},
    splinter::Splinter,
    traits::TruncateFrom,
};

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

pub type SetGen = LevelSetGen<High>;

pub struct LevelSetGen<L> {
    seed: u64,
    _phantom: PhantomData<L>,
}

impl SetGen {
    pub fn distributed(&mut self, high: usize, mid: usize, low: usize, block: usize) -> Vec<u32> {
        let mut out = Vec::default();
        let mut rng = self.rng();
        for high in index::sample(&mut rng, 256, high) {
            for mid in index::sample(&mut rng, 256, mid) {
                for low in index::sample(&mut rng, 256, low) {
                    for blk in index::sample(&mut rng, 256, block) {
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
}

impl<L: Level> LevelSetGen<L> {
    pub fn new(seed: u64) -> Self {
        Self { seed, _phantom: PhantomData }
    }

    fn rng(&self) -> StdRng {
        rand::rngs::StdRng::seed_from_u64(self.seed)
    }

    pub fn linear(&mut self, count: usize) -> Vec<L::Value> {
        assert!(count <= L::MAX_LEN, "count must be less than L::MAX_LEN");
        (0..count).map(|i| L::Value::truncate_from(i)).collect()
    }

    pub fn random(&mut self, len: usize) -> Vec<L::Value> {
        index::sample(&mut self.rng(), L::MAX_LEN - 1, len)
            .into_iter()
            .map(L::Value::truncate_from)
            .sorted()
            .collect()
    }

    /// Generate a random set of values such that the probability any two values
    /// are sequential is `stickiness`.
    pub fn runs(&mut self, len: usize, stickiness: f64) -> Vec<L::Value> {
        let mut rng = self.rng();
        let s = stickiness.clamp(0.0, 1.0);
        let mut out = Vec::with_capacity(len);
        if len == 0 {
            return out;
        }
        // Allow worst-case growth of ~2 per step to avoid overflow.
        let max_start =
            (L::MAX_LEN - 1).saturating_sub(2usize.saturating_mul(len.saturating_sub(1)));
        let mut cur = rng.random_range(0..=max_start);
        out.push(L::Value::truncate_from(cur));

        for _ in 1..len {
            if rng.random_bool(s) {
                cur = cur.saturating_add(1);
            } else {
                // Non-sequential: jump by >=2. Use a geometric(0.5) tail for gaps.
                let mut k = 0;
                while !rng.random_bool(0.5) {
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

pub fn mkchecksum(data: &[u8]) -> u64 {
    let mut c = crc64fast_nvme::Digest::new();
    c.write(&data);
    c.sum64()
}

/// appends a valid Splinter Footer to data and returns it as Bytes
pub fn mksplinter_manual(data: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(data.len() + Footer::SIZE);
    buf.put_slice(data);
    buf.put_slice(Footer::from_checksum(mkchecksum(data)).as_bytes());
    buf.freeze()
}

pub fn mkpartition<L: Level>(kind: PartitionKind, values: &[L::Value]) -> Partition<L> {
    let mut p = kind.build();
    for &v in values {
        p.raw_insert(v);
    }
    p
}

pub fn mkpartition_buf<L: Level>(kind: PartitionKind, values: &[L::Value]) -> BytesMut {
    mkpartition::<L>(kind, values)
        .encode_to_bytes()
        .try_into_mut()
        .unwrap()
}

pub fn mksplinter(values: &[u32]) -> Splinter {
    Splinter::from_iter(values.iter().copied())
}

pub fn mksplinter_buf(values: &[u32]) -> BytesMut {
    mksplinter(values).encode_to_bytes().try_into_mut().unwrap()
}
