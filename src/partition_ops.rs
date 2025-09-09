use std::ops::BitOrAssign;

use crate::{
    PartitionRead, PartitionWrite,
    codec::partition_ref::{NonRecursivePartitionRef, PartitionRef},
    level::Level,
    partition::Partition,
    traits::Cut,
};

impl<L: Level> PartialEq for Partition<L> {
    fn eq(&self, other: &Partition<L>) -> bool {
        use Partition::*;

        match (self, other) {
            // use fast physical ops if both partitions share storage
            (Full, Full) => true,
            (Bitmap(a), Bitmap(b)) => a == b,
            (Vec(a), Vec(b)) => a == b,
            (Run(a), Run(b)) => a == b,
            (Tree(a), Tree(b)) => a == b,

            // otherwise fall back to logical ops
            (a, b) => {
                debug_assert_ne!(a.kind(), b.kind(), "should have different storage classes");
                itertools::equal(a.iter(), b.iter())
            }
        }
    }
}

impl<L: Level> PartialEq<PartitionRef<'_, L>> for Partition<L> {
    fn eq(&self, other: &PartitionRef<'_, L>) -> bool {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        match (self, other) {
            // use fast physical ops if both partitions share storage
            (Partition::Full, NonRecursive(Full)) => true,
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a == bitmap,
            (Partition::Vec(a), NonRecursive(Vec { values })) => a == values,
            (Partition::Run(a), NonRecursive(Run { runs })) => a == runs,
            (Partition::Tree(a), Tree(b)) => a == b,

            // otherwise fall back to logical ops
            (a, b) => itertools::equal(a.iter(), b.iter()),
        }
    }
}

impl<L: Level> BitOrAssign<&Partition<L>> for Partition<L> {
    fn bitor_assign(&mut self, rhs: &Partition<L>) {
        use Partition::*;

        match (&mut *self, rhs) {
            // special case full
            (Full, _) => (),
            (a, Full) => *a = Full,

            // use fast physical ops if both partitions share storage
            (Bitmap(a), Bitmap(b)) => a.bitor_assign(b),
            (Vec(a), Vec(b)) => a.bitor_assign(b),
            (Run(a), Run(b)) => a.bitor_assign(b),
            (Tree(a), Tree(b)) => a.bitor_assign(b),

            // otherwise fall back to logical ops
            (a, b) => {
                for el in b.iter() {
                    a.raw_insert(el);
                }
            }
        }

        self.optimize_fast();
    }
}

impl<L: Level> BitOrAssign<&PartitionRef<'_, L>> for Partition<L> {
    fn bitor_assign(&mut self, rhs: &PartitionRef<'_, L>) {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        match (&mut *self, rhs) {
            // special cases for full and empty
            (Partition::Full, _) => (),
            (_, NonRecursive(Empty)) => (),
            (a, NonRecursive(Full)) => *a = Partition::Full,

            // use fast physical ops if both partitions share storage
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a.bitor_assign(*bitmap),
            (Partition::Vec(a), NonRecursive(Vec { values })) => a.bitor_assign(*values),
            (Partition::Run(a), NonRecursive(Run { runs })) => a.bitor_assign(runs),
            (Partition::Tree(a), Tree(tree)) => a.bitor_assign(tree),

            // otherwise fall back to logical ops
            (a, b) => {
                for el in b.iter() {
                    a.raw_insert(el);
                }
            }
        }

        self.optimize_fast();
    }
}

impl<L: Level> Cut for Partition<L> {
    type Out = Self;

    fn cut(&mut self, rhs: &Self) -> Self::Out {
        use Partition::*;

        let mut intersection = match (&mut *self, rhs) {
            // use fast physical ops if both partitions share storage
            (a @ Full, Full) => std::mem::take(a),
            (Bitmap(a), Bitmap(b)) => a.cut(b),
            (Run(a), Run(b)) => a.cut(b),
            (Tree(a), Tree(b)) => a.cut(b),

            // fallback to general optimized logical ops
            (Vec(a), b) => a.cut(b),

            // otherwise fall back to logical ops
            (a, b) => {
                let mut intersection = Partition::default();
                for val in b.iter() {
                    if a.remove(val) {
                        intersection.raw_insert(val);
                    }
                }
                intersection
            }
        };

        self.optimize_fast();
        intersection.optimize_fast();
        intersection
    }
}

impl<L: Level> Cut<PartitionRef<'_, L>> for Partition<L> {
    type Out = Self;

    fn cut(&mut self, rhs: &PartitionRef<'_, L>) -> Self::Out {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        let mut intersection = match (&mut *self, rhs) {
            // special case empty
            (_, NonRecursive(Empty)) => Partition::default(),

            // use fast physical ops if both partitions share storage
            (a @ Partition::Full, NonRecursive(Full)) => std::mem::take(a),
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a.cut(bitmap),
            (Partition::Run(a), NonRecursive(Run { runs })) => a.cut(runs),
            (Partition::Tree(a), Tree(b)) => a.cut(b),

            // fallback to general optimized logical ops
            (Partition::Vec(a), b) => a.cut(b),

            // otherwise fall back to logical ops
            (a, b) => {
                let mut intersection = Partition::default();
                for val in b.iter() {
                    if a.remove(val) {
                        intersection.raw_insert(val);
                    }
                }
                intersection
            }
        };

        self.optimize_fast();
        intersection.optimize_fast();
        intersection
    }
}
