use std::ops::{BitAndAssign, BitOrAssign, BitXorAssign, SubAssign};

use itertools::{EitherOrBoth, Itertools};

use crate::{
    PartitionRead, PartitionWrite,
    codec::partition_ref::{NonRecursivePartitionRef, PartitionRef},
    level::Level,
    partition::Partition,
    traits::{Complement, Cut},
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

impl<L: Level> BitAndAssign<&Partition<L>> for Partition<L> {
    fn bitand_assign(&mut self, rhs: &Partition<L>) {
        use Partition::*;

        match (&mut *self, rhs) {
            // special case full
            (a @ Full, b) => *a = b.clone(),
            (_, Full) => (),

            // use fast physical ops if both partitions share storage
            (Bitmap(a), Bitmap(b)) => a.bitand_assign(b),
            (Vec(a), Vec(b)) => a.bitand_assign(b),
            (Run(a), Run(b)) => a.bitand_assign(b),
            (Tree(a), Tree(b)) => a.bitand_assign(b),

            // otherwise fall back to logical ops
            (a, b) => {
                *a = std::mem::take(a)
                    .iter()
                    .merge_join_by(b.iter(), L::Value::cmp)
                    .flat_map(|x| match x {
                        EitherOrBoth::Left(_) => None,
                        EitherOrBoth::Right(_) => None,
                        EitherOrBoth::Both(l, _) => Some(l),
                    })
                    .collect();
            }
        }

        self.optimize_fast();
    }
}

impl<L: Level> BitAndAssign<&PartitionRef<'_, L>> for Partition<L> {
    fn bitand_assign(&mut self, rhs: &PartitionRef<'_, L>) {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        match (&mut *self, rhs) {
            // special cases for full and empty
            (a @ Partition::Full, b) => *a = b.into(),
            (a, NonRecursive(Empty)) => *a = Partition::default(),
            (_, NonRecursive(Full)) => (),

            // use fast physical ops if both partitions share storage
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a.bitand_assign(*bitmap),
            (Partition::Vec(a), NonRecursive(Vec { values })) => a.bitand_assign(*values),
            (Partition::Run(a), NonRecursive(Run { runs })) => a.bitand_assign(runs),
            (Partition::Tree(a), Tree(tree)) => a.bitand_assign(tree),

            // otherwise fall back to logical ops
            (a, b) => {
                *a = std::mem::take(a)
                    .iter()
                    .merge_join_by(b.iter(), L::Value::cmp)
                    .flat_map(|x| match x {
                        EitherOrBoth::Left(_) => None,
                        EitherOrBoth::Right(_) => None,
                        EitherOrBoth::Both(l, _) => Some(l),
                    })
                    .collect();
            }
        }

        self.optimize_fast();
    }
}

impl<L: Level> BitXorAssign<&Partition<L>> for Partition<L> {
    fn bitxor_assign(&mut self, rhs: &Partition<L>) {
        use Partition::*;

        match (&mut *self, rhs) {
            // special case full
            (a @ Full, b) => {
                *a = b.clone();
                a.complement();
            }
            (a, Full) => a.complement(),

            // use fast physical ops if both partitions share storage
            (Bitmap(a), Bitmap(b)) => a.bitxor_assign(b),
            (Vec(a), Vec(b)) => a.bitxor_assign(b),
            (Run(a), Run(b)) => a.bitxor_assign(b),
            (Tree(a), Tree(b)) => a.bitxor_assign(b),

            // otherwise fall back to logical ops
            (a, b) => {
                *a = std::mem::take(a)
                    .iter()
                    .merge_join_by(b.iter(), L::Value::cmp)
                    .flat_map(|x| match x {
                        EitherOrBoth::Left(l) => Some(l),
                        EitherOrBoth::Right(r) => Some(r),
                        EitherOrBoth::Both(_, _) => None,
                    })
                    .collect();
            }
        }

        self.optimize_fast();
    }
}

impl<L: Level> BitXorAssign<&PartitionRef<'_, L>> for Partition<L> {
    fn bitxor_assign(&mut self, rhs: &PartitionRef<'_, L>) {
        todo!()
    }
}

impl<L: Level> SubAssign<&Partition<L>> for Partition<L> {
    fn sub_assign(&mut self, rhs: &Partition<L>) {
        todo!()
    }
}

impl<L: Level> SubAssign<&PartitionRef<'_, L>> for Partition<L> {
    fn sub_assign(&mut self, rhs: &PartitionRef<'_, L>) {
        todo!()
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

impl<L: Level> Complement for Partition<L> {
    fn complement(&mut self) {
        use Partition::*;

        match &mut *self {
            p @ Full => {
                *p = Partition::EMPTY;
            }
            Bitmap(p) => p.complement(),
            Vec(p) => p.complement(),
            Run(p) => p.complement(),
            Tree(p) => p.complement(),
        }

        self.optimize_fast();
    }
}

impl<L: Level> From<&PartitionRef<'_, L>> for Partition<L> {
    fn from(value: &PartitionRef<'_, L>) -> Self {
        value.into()
    }
}
