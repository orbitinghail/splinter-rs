use std::iter::FusedIterator;

use num::traits::AsPrimitive;
use u24::u24;

pub type Segment = u8;

pub trait SplitSegment {
    type Rest;

    /// returns the first byte (segment) of `Self`
    fn segment(self) -> Segment;

    /// returns the remaining bytes
    fn rest(self) -> Self::Rest;

    /// splits the first byte (segment) off of `Self`
    fn split(self) -> (Segment, Self::Rest);

    /// performs the inverse of `Self::split`
    fn unsplit(segment: Segment, rest: Self::Rest) -> Self;

    /// returns the first value in the current segment
    /// equivalent to masking all of the trailing bytes to 0
    fn segment_start(self) -> Self;

    /// returns the last value in the current segment
    /// equivalent to masking all of the trailing bytes to 1
    fn segment_end(self) -> Self;
}

macro_rules! impl_split {
    ($(($ty:ty, $rest:ty, $segment_mask:expr)),*) => {
        $(
            impl SplitSegment for $ty {
                type Rest = $rest;

                #[inline(always)]
                fn segment(self) -> Segment {
                    (self >> (<$rest>::BITS as usize)).as_()
                }

                #[inline(always)]
                fn rest(self) -> Self::Rest {
                    self.as_()
                }

                #[inline(always)]
                fn split(self) -> (Segment, Self::Rest) {
                    (self.segment(), self.rest())
                }

                #[inline(always)]
                fn unsplit(segment: Segment, rest: Self::Rest) -> Self {
                    let segment: $ty = segment.as_();
                    let rest: $ty = rest.as_();
                    segment << (<$rest>::BITS as usize) | rest
                }

                #[inline(always)]
                fn segment_start(self) -> Self {
                    self & $segment_mask
                }

                #[inline(always)]
                fn segment_end(self) -> Self {
                    self & Self::MAX
                }
            }
        )*
    };
}

impl_split!(
    (u32, u24, 0xFF000000),
    (u24, u16, u24!(0xFF0000)),
    (u16, u8, 0xFF00)
);

impl SplitSegment for u8 {
    type Rest = u8;

    fn segment(self) -> Segment {
        unreachable!()
    }
    fn rest(self) -> Self::Rest {
        unreachable!()
    }
    fn split(self) -> (Segment, Self::Rest) {
        unreachable!()
    }
    fn unsplit(_segment: Segment, _rest: Self::Rest) -> Self {
        unreachable!()
    }
    fn segment_start(self) -> Self {
        unreachable!()
    }
    fn segment_end(self) -> Self {
        unreachable!()
    }
}

/// An iterator of values that can be segmented into `(Segment, Rest)` pairs via
/// the `SplitSegment` trait.
#[must_use]
pub struct IterSegmented<I> {
    inner: I,
}

impl<I> IterSegmented<I> {
    pub fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<T: SplitSegment, I: Iterator<Item = T>> Iterator for IterSegmented<I> {
    type Item = (Segment, T::Rest);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|v| v.split())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: SplitSegment, I: DoubleEndedIterator<Item = T>> DoubleEndedIterator for IterSegmented<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|v| v.split())
    }
}

impl<T: SplitSegment, I: FusedIterator<Item = T>> FusedIterator for IterSegmented<I> {}
impl<T: SplitSegment, I: ExactSizeIterator<Item = T>> ExactSizeIterator for IterSegmented<I> {}
