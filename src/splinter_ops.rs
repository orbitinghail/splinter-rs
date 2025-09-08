use std::ops::Deref;

use crate::{CowSplinter, Cut, Merge, Splinter, SplinterRef};

impl<B: Deref<Target = [u8]>> PartialEq<SplinterRef<B>> for Splinter {
    #[inline]
    fn eq(&self, other: &SplinterRef<B>) -> bool {
        self.inner() == &other.load_unchecked()
    }
}

impl<B: Deref<Target = [u8]>> PartialEq<CowSplinter<B>> for Splinter {
    fn eq(&self, other: &CowSplinter<B>) -> bool {
        match other {
            CowSplinter::Ref(splinter_ref) => self.eq(splinter_ref),
            CowSplinter::Owned(splinter) => self.eq(splinter),
        }
    }
}

impl Cut for Splinter {
    type Out = Self;

    fn cut(&mut self, rhs: &Self) -> Self::Out {
        Self::new(self.inner_mut().cut(rhs.inner()))
    }
}

impl<B: Deref<Target = [u8]>> Cut<SplinterRef<B>> for Splinter {
    type Out = Self;

    fn cut(&mut self, rhs: &SplinterRef<B>) -> Self::Out {
        Self::new(self.inner_mut().cut(&rhs.load_unchecked()))
    }
}

impl<B: Deref<Target = [u8]>> Cut<CowSplinter<B>> for Splinter {
    type Out = Self;

    fn cut(&mut self, rhs: &CowSplinter<B>) -> Self::Out {
        match rhs {
            CowSplinter::Ref(splinter_ref) => self.cut(splinter_ref),
            CowSplinter::Owned(splinter) => self.cut(splinter),
        }
    }
}

impl Merge for Splinter {
    fn merge(&mut self, rhs: &Self) {
        self.inner_mut().merge(rhs.inner())
    }
}

impl<B: Deref<Target = [u8]>> Merge<SplinterRef<B>> for Splinter {
    fn merge(&mut self, rhs: &SplinterRef<B>) {
        self.inner_mut().merge(&rhs.load_unchecked())
    }
}

impl<B: Deref<Target = [u8]>> Merge<CowSplinter<B>> for Splinter {
    fn merge(&mut self, rhs: &CowSplinter<B>) {
        match rhs {
            CowSplinter::Ref(splinter_ref) => self.merge(splinter_ref),
            CowSplinter::Owned(splinter) => self.merge(splinter),
        }
    }
}
