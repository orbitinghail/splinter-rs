use std::ops::Deref;

use crate::{Splinter, SplinterRef};

impl<B> PartialEq<Splinter> for SplinterRef<B>
where
    B: Deref<Target = [u8]>,
{
    #[inline]
    fn eq(&self, other: &Splinter) -> bool {
        other == self
    }
}

impl<B, B2> PartialEq<SplinterRef<B2>> for SplinterRef<B>
where
    B: Deref<Target = [u8]>,
    B2: Deref<Target = [u8]>,
{
    fn eq(&self, other: &SplinterRef<B2>) -> bool {
        self.load_unchecked() == other.load_unchecked()
    }
}

impl<B: Deref<Target = [u8]>> Eq for SplinterRef<B> {}
