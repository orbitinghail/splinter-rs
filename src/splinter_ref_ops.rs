use std::ops::Deref;

use crate::{Splinter, SplinterRef};

impl<B: Deref<Target = [u8]>> PartialEq<Splinter> for SplinterRef<B> {
    #[inline]
    fn eq(&self, other: &Splinter) -> bool {
        other == self
    }
}

impl<B: Deref<Target = [u8]>, B2: Deref<Target = [u8]>> PartialEq<SplinterRef<B2>>
    for SplinterRef<B>
{
    fn eq(&self, other: &SplinterRef<B2>) -> bool {
        self.load_unchecked() == other.load_unchecked()
    }
}

impl<B: Deref<Target = [u8]>> Eq for SplinterRef<B> {}
