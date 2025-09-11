use std::{
    iter::Peekable,
    ops::{Bound, RangeBounds, RangeInclusive},
};

use num::{
    PrimInt,
    traits::{ConstOne, ConstZero},
};

#[doc(hidden)]
#[macro_export]
macro_rules! MultiIter {
    ($type:ident, $($name:ident),+) => {
        #[must_use]
        pub(crate) enum $type<$($name),+> {
            $($name($name)),+
        }

        impl<
            T, $($name: Iterator<Item=T>),+
        > Iterator for $type<$($name),+>
        {
            type Item = T;

            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    $(Self::$name(iter) => iter.next(),)+
                }
            }
        }
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

            (Included(start), Excluded(end))
            | (Excluded(start), Included(end))
            | (Excluded(start), Excluded(end)) => start >= end,

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
