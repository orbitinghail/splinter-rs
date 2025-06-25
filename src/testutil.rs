use std::fmt::Debug;

use bytes::Bytes;
use itertools::Itertools;
use rand::{SeedableRng, seq::index};

use crate::{Splinter, SplinterRef, util::CopyToOwned};

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

    #[track_caller]
    pub fn distributed(
        &mut self,
        high: usize,
        mid: usize,
        low: usize,
        block: usize,
        expected_len: usize,
    ) -> Vec<u32> {
        let mut out = Vec::with_capacity(expected_len);
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
        assert_eq!(out.len(), expected_len);
        out
    }

    #[track_caller]
    pub fn dense(
        &mut self,
        high: usize,
        mid: usize,
        low: usize,
        block: usize,
        expected_len: usize,
    ) -> Vec<u32> {
        let out: Vec<u32> = itertools::iproduct!(0..high, 0..mid, 0..low, 0..block)
            .map(|(a, b, c, d)| u32::from_be_bytes([a as u8, b as u8, c as u8, d as u8]))
            .collect();
        assert_eq!(out.len(), expected_len);
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
