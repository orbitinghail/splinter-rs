use std::{
    iter::{FusedIterator, Peekable},
    ops::{Bound, RangeBounds, RangeInclusive},
};

use num::{
    PrimInt,
    traits::{ConstOne, ConstZero},
};
use range_set_blaze::Integer;

#[doc(hidden)]
#[macro_export]
macro_rules! MultiIter {
    ($type:ident, $($name:ident),+) => {
        #[must_use]
        pub(crate) enum $type<$($name),+> {
            $($name($name)),+
        }

        impl<T, $($name: Iterator<Item=T>),+> Iterator
        for $type<$($name),+>
        {
            type Item = T;

            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    $(Self::$name(iter) => iter.next(),)+
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                match self {
                    $(Self::$name(iter) => iter.size_hint(),)+
                }
            }
        }

        impl<T, $($name: Iterator<Item=T> + std::iter::ExactSizeIterator),+> std::iter::ExactSizeIterator
        for $type<$($name),+> { }

        impl<T, $($name: Iterator<Item=T> + std::iter::FusedIterator),+> std::iter::FusedIterator
        for $type<$($name),+> { }
    };
}

pub fn find_next_sorted<I, T>(iter: &mut Peekable<I>, needle: &T) -> Option<T>
where
    I: Iterator<Item = T>,
    T: PartialOrd + PartialEq,
{
    // advance the iterator until either:
    // 1. we find the needle
    // 2. we find a value larger than the needle
    //
    while let Some(next) = iter.next_if(|v| v <= needle) {
        if &next == needle {
            return Some(next);
        }
    }
    None
}

pub trait RangeExt<T> {
    fn is_empty(&self) -> bool;
    fn try_into_inclusive(self) -> Option<RangeInclusive<T>>;
}

impl<R: RangeBounds<T>, T: PrimInt + ConstOne + ConstZero> RangeExt<T> for R {
    fn is_empty(&self) -> bool {
        use Bound::*;
        match (self.start_bound(), self.end_bound()) {
            (Unbounded, Excluded(&end)) if end == T::ZERO => true,

            (Excluded(&start), Unbounded) if start == T::max_value() => true,

            (Unbounded, _) | (_, Unbounded) => false,

            (Included(start), Excluded(end)) | (Excluded(start), Included(end)) => start >= end,

            // Integer-exclusive bounds have an extra gap: (n, n + 1) is empty.
            (Excluded(start), Excluded(end)) => (*start).saturating_add(T::ONE) >= *end,

            (Included(start), Included(end)) => start > end,
        }
    }

    /// Converts self into an inclusive range if the range is not empty
    fn try_into_inclusive(self) -> Option<RangeInclusive<T>> {
        if RangeExt::is_empty(&self) {
            None
        } else {
            let start = match self.start_bound() {
                Bound::Included(v) => *v,
                Bound::Excluded(v) => v.saturating_add(T::ONE),
                Bound::Unbounded => T::ZERO,
            };
            let end = match self.end_bound() {
                Bound::Included(v) => *v,
                Bound::Excluded(v) => v.saturating_sub(T::ONE),
                Bound::Unbounded => T::max_value(),
            };
            Some(start..=end)
        }
    }
}

pub trait IteratorExt: Iterator + Sized {
    #[inline]
    fn with_size_hint(self, hint: usize) -> SizeHintIter<Self> {
        SizeHintIter::new(hint, self)
    }
}

impl<I: Iterator> IteratorExt for I {}

/// A `SizeHintIter` wraps an iter with a lower bound.
#[must_use]
pub struct SizeHintIter<I> {
    remaining: usize,
    iter: I,
}

impl<T, I: Iterator<Item = T>> SizeHintIter<I> {
    pub fn new(size: usize, iter: I) -> Self {
        Self { remaining: size, iter }
    }
}

impl<T, I: Iterator<Item = T>> Iterator for SizeHintIter<I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.remaining = self.remaining.saturating_sub(1);
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.iter.size_hint();
        (self.remaining.max(lower), upper)
    }
}

/// Iterator over a range of integers using `range_set_blaze::Integer` trait
pub struct RangeIter<T: Integer> {
    range: Option<RangeInclusive<T>>,
}

impl<T: Integer> RangeIter<T> {
    pub fn new(range: RangeInclusive<T>) -> Self {
        Self { range: Some(range) }
    }
}

impl<T: Integer> Iterator for RangeIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut range) = self.range {
            T::range_next(range)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(ref range) = self.range {
            let len = T::safe_len(range);
            let len = T::safe_len_to_f64_lossy(len) as usize;
            (len, Some(len))
        } else {
            (0, Some(0))
        }
    }
}

impl<T: Integer> DoubleEndedIterator for RangeIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(ref mut range) = self.range {
            T::range_next_back(range)
        } else {
            None
        }
    }
}

impl<T: Integer> ExactSizeIterator for RangeIter<T> {}
impl<T: Integer> FusedIterator for RangeIter<T> {}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use crate::{PartitionRead, Splinter, util::RangeExt};

    #[test]
    fn test_full_contains_any_exclusive_adjacent_range_is_false() {
        // (5, 6) is empty over integers and should never intersect, even with FULL.
        let range = (Bound::Excluded(5), Bound::Excluded(6));
        assert!(!Splinter::FULL.contains_any(range));
    }

    #[test]
    fn test_range_ext_is_empty_table() {
        type Case = (&'static str, (Bound<u8>, Bound<u8>), bool);
        let cases: &[Case] = &[
            (
                "unbounded..unbounded",
                (Bound::Unbounded, Bound::Unbounded),
                false,
            ),
            (
                "unbounded..=included",
                (Bound::Unbounded, Bound::Included(3)),
                false,
            ),
            (
                "unbounded..excluded_zero",
                (Bound::Unbounded, Bound::Excluded(0)),
                true,
            ),
            (
                "unbounded..excluded_nonzero",
                (Bound::Unbounded, Bound::Excluded(1)),
                false,
            ),
            (
                "included..unbounded",
                (Bound::Included(3), Bound::Unbounded),
                false,
            ),
            (
                "excluded_max..unbounded",
                (Bound::Excluded(u8::MAX), Bound::Unbounded),
                true,
            ),
            (
                "excluded_not_max..unbounded",
                (Bound::Excluded(u8::MAX - 1), Bound::Unbounded),
                false,
            ),
            (
                "included..included_non_empty",
                (Bound::Included(3), Bound::Included(3)),
                false,
            ),
            (
                "included..included_empty",
                (Bound::Included(4), Bound::Included(3)),
                true,
            ),
            (
                "included..excluded_non_empty",
                (Bound::Included(3), Bound::Excluded(4)),
                false,
            ),
            (
                "included..excluded_empty",
                (Bound::Included(3), Bound::Excluded(3)),
                true,
            ),
            (
                "excluded..included_non_empty",
                (Bound::Excluded(2), Bound::Included(3)),
                false,
            ),
            (
                "excluded..included_empty",
                (Bound::Excluded(3), Bound::Included(3)),
                true,
            ),
            (
                "excluded..excluded_non_empty",
                (Bound::Excluded(3), Bound::Excluded(5)),
                false,
            ),
            (
                "excluded..excluded_adjacent_empty",
                (Bound::Excluded(3), Bound::Excluded(4)),
                true,
            ),
        ];

        for (name, range, expected_empty) in cases {
            assert_eq!(RangeExt::is_empty(range), *expected_empty, "case: {name}");
        }
    }
}
