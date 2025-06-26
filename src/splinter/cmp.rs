use crate::cow::CowSplinter;

use super::{Splinter, SplinterRef};

// Splinter == Splinter
impl PartialEq for Splinter {
    fn eq(&self, other: &Self) -> bool {
        self.partitions == other.partitions
    }
}

// Splinter == SplinterRef
impl<T: AsRef<[u8]>> PartialEq<SplinterRef<T>> for Splinter {
    fn eq(&self, other: &SplinterRef<T>) -> bool {
        self.partitions == other.load_partitions()
    }
}

// Splinter == CowSplinter
impl<T: AsRef<[u8]>> PartialEq<CowSplinter<T>> for Splinter {
    fn eq(&self, other: &CowSplinter<T>) -> bool {
        other == self
    }
}

// SplinterRef == SplinterRef
impl<T1: AsRef<[u8]>, T2: AsRef<[u8]>> PartialEq<SplinterRef<T2>> for SplinterRef<T1> {
    fn eq(&self, other: &SplinterRef<T2>) -> bool {
        self.load_partitions() == other.load_partitions()
    }
}

// SplinterRef == Splinter
impl<T: AsRef<[u8]>> PartialEq<Splinter> for SplinterRef<T> {
    fn eq(&self, other: &Splinter) -> bool {
        other == self
    }
}

// SplinterRef == CowSplinter
impl<T1: AsRef<[u8]>, T2: AsRef<[u8]>> PartialEq<CowSplinter<T2>> for SplinterRef<T1> {
    fn eq(&self, other: &CowSplinter<T2>) -> bool {
        other == self
    }
}

// CowSplinter == CowSplinter
impl<T: AsRef<[u8]>> PartialEq for CowSplinter<T> {
    fn eq(&self, other: &Self) -> bool {
        use CowSplinter::*;
        match (self, other) {
            (Ref(left), Ref(right)) => left == right,
            (Ref(left), Owned(right)) => right == left,
            (Owned(left), Ref(right)) => left == right,
            (Owned(left), Owned(right)) => left == right,
        }
    }
}

// CowSplinter == Splinter
impl<T: AsRef<[u8]>> PartialEq<Splinter> for CowSplinter<T> {
    fn eq(&self, right: &Splinter) -> bool {
        use CowSplinter::*;
        match self {
            Ref(left) => right == left,
            Owned(left) => left == right,
        }
    }
}

// CowSplinter == SplinterRef
impl<T1: AsRef<[u8]>, T2: AsRef<[u8]>> PartialEq<SplinterRef<T2>> for CowSplinter<T1> {
    fn eq(&self, other: &SplinterRef<T2>) -> bool {
        use CowSplinter::*;
        match self {
            Ref(left) => left == other,
            Owned(left) => left == other,
        }
    }
}
