use std::fmt::Display;

use bitvec::{boxed::BitBox, order::BitOrder, slice::BitSlice, store::BitStore};
use num::{PrimInt, traits::ConstOne};

/// Counts the number of unique values present in `iter`. Requires that the
/// iterator is sorted.
#[track_caller]
pub fn count_unique_sorted<I, T>(iter: I) -> usize
where
    I: IntoIterator<Item = T>,
    T: PrimInt,
{
    let mut iter = iter.into_iter().peekable();
    let mut count = 0;
    let mut max = T::zero();
    while let Some(curr) = iter.next() {
        debug_assert!(curr >= max, "values must be sorted");
        max = curr;

        count += 1;
        while iter.peek() == Some(&curr) {
            iter.next();
        }
    }
    count
}

/// Counts the number of runs present in `iter`. Requires that the
/// iterator is sorted and unique
pub fn count_runs_sorted<I, T>(iter: I) -> usize
where
    I: IntoIterator<Item = T>,
    T: PrimInt + ConstOne + Display,
{
    let mut iter = iter.into_iter().peekable();
    let mut count = 0;
    let mut last = None;

    while let Some(mut curr) = iter.next() {
        debug_assert!(
            Some(curr) > last.replace(curr),
            "values must be sorted and unique"
        );

        count += 1;
        curr = curr + T::ONE;
        while iter.peek() == Some(&curr) {
            curr = curr + T::one();
            iter.next();
        }
    }

    count
}

/// Counts the number of runs present in `bitmap`.
pub fn count_bitmap_runs<T, O>(bitmap: &BitSlice<T, O>) -> usize
where
    T: BitStore,
    O: BitOrder,
{
    // this implements ((bitmap<<1) & !bitmap) + bitmap[0]
    let shifted = &bitmap[1..];
    let transitions = !BitBox::from_bitslice(bitmap) & shifted;
    transitions.count_ones() + (bitmap[0] as usize)
}

#[cfg(test)]
mod tests {
    use bitvec::{order::Msb0, view::BitView};
    use itertools::enumerate;

    use super::*;

    #[test]
    fn test_count_unique_sorted() {
        assert_eq!(count_unique_sorted(Vec::<u32>::new()), 0);
        assert_eq!(count_unique_sorted(vec![1]), 1);
        assert_eq!(count_unique_sorted(vec![1, 1, 1]), 1);
        assert_eq!(count_unique_sorted(vec![1, 2, 3]), 3);
        assert_eq!(count_unique_sorted(vec![1, 1, 2, 2, 3, 3]), 3);
        assert_eq!(count_unique_sorted(vec![1, 2, 2, 2, 3, 4, 4]), 4);
    }

    #[test]
    #[should_panic]
    fn test_count_unique_sorted_panic() {
        count_unique_sorted(vec![1, 2, 1]);
    }

    #[test]
    fn test_count_runs_sorted() {
        let cases = [
            (vec![], 0),
            (vec![1], 1),
            (vec![1, 2], 1),
            (vec![1, 2, 4], 2),
            (vec![1, 2, 5, 7], 3),
            (vec![2, 3, 4, 5], 1),
        ];

        for (input, expected) in cases {
            assert_eq!(count_runs_sorted(input.clone()), expected, "{input:?}");
        }
    }

    #[test]
    #[should_panic]
    fn test_count_runs_sorted_panic() {
        count_runs_sorted([1, 2, 1]);
    }

    #[test]
    fn test_count_bitmap_runs() {
        let cases = [
            (vec![0b00000000_00000000], 0),
            (vec![0b00000000_00000001], 1),
            (vec![0b00000000_00000010], 1),
            (vec![0b00000000_00000011], 1),
            (vec![0b10000000_00000000], 1),
            (vec![0b10000000_00000000], 1),
            (vec![0b01000000_00000000], 1),
            (vec![0b00000001_00000001], 2),
            (vec![0b10000000_10000000], 2),
            (vec![0b10000001_10000000], 2),
            (vec![0b00000001_10000000], 1),
            (vec![0b00000001_10000001], 2),
            (vec![0b11111111_11111111], 1),
            (vec![0b11110000_11111111], 2),
            (vec![0b11110111_11111111], 2),
            (vec![0b10101010_10101010], 8),
            (vec![0b01010101_01010101], 8),
            (vec![0b11011011_01101101], 6),
            (vec![0b11101110_11101110], 4),
            ([0b10101010_10101010].repeat(128), 128 * 8),
            ([0b01010101_01010101].repeat(128), 128 * 8),
            ([0b10000000_00000001].repeat(128), 128 + 1),
            ([0b10000000_10000001].repeat(128), 128 + 1 + 128),
            ([0b11111111_11111111].repeat(128), 1),
            ([0b00000000_00000000].repeat(128), 0),
        ];
        for (i, (bits, expected)) in enumerate(cases) {
            let bitmap: &BitSlice<u16, Msb0> = bits.view_bits::<Msb0>();
            println!("testing case {i}: {bitmap:b}");
            assert_eq!(
                count_bitmap_runs(bitmap),
                expected,
                "case {i} failed: {bitmap:b}"
            );
        }
    }
}
