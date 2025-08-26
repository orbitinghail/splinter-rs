use num::traits::AsPrimitive;
use u24::u24;

pub type Segment = u8;

pub trait SplitSegment {
    type Rest;

    fn segment(self) -> Segment;
    fn split(self) -> (Segment, Self::Rest);
    fn unsplit(segment: Segment, rest: Self::Rest) -> Self;
}

macro_rules! impl_split {
    ($(($ty:ty, $rest:ty)),*) => {
        $(
            impl SplitSegment for $ty {
                type Rest = $rest;

                #[inline(always)]
                fn segment(self) -> Segment {
                    (self >> (<$rest>::BITS as usize)).as_()
                }

                #[inline(always)]
                fn split(self) -> (Segment, Self::Rest) {
                    (self.segment(), self.as_())
                }

                #[inline(always)]
                fn unsplit(segment: Segment, rest: Self::Rest) -> Self {
                    let segment: $ty = segment.as_();
                    let rest: $ty = rest.as_();
                    segment << (<$rest>::BITS as usize) | rest
                }
            }
        )*
    };
}

impl_split!((u32, u24), (u24, u16), (u16, u8));

impl SplitSegment for u8 {
    type Rest = u8;

    fn segment(self) -> Segment {
        unreachable!()
    }
    fn split(self) -> (Segment, Self::Rest) {
        unreachable!()
    }
    fn unsplit(_segment: Segment, _rest: Self::Rest) -> Self {
        unreachable!()
    }
}
