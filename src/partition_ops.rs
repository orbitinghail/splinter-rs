use std::ops::{BitAndAssign, BitOrAssign, BitXorAssign, SubAssign};

use itertools::{EitherOrBoth, Itertools};

use crate::{
    PartitionRead, PartitionWrite,
    codec::partition_ref::{NonRecursivePartitionRef, PartitionRef},
    level::Level,
    partition::Partition,
    partition_kind::PartitionKind,
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
            (a, b) => itertools::equal(a.iter(), b.iter()),
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

            // special case empty
            (_, b) if b.is_empty() => (),
            (a, b) if a.is_empty() => *a = b.clone(),

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
            // special case full
            (Partition::Full, _) => (),
            (a, NonRecursive(Full)) => *a = Partition::Full,

            // special case empty
            (_, NonRecursive(Empty)) => (),
            (a, b) if a.is_empty() => *a = b.into(),

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

            // special case empty
            (a, b) if a.is_empty() || b.is_empty() => *a = Partition::EMPTY,

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
                    .filter_map(|x| match x {
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
            // special case full
            (a @ Partition::Full, b) => *a = b.into(),
            (_, NonRecursive(Full)) => (),

            // special case empty
            (a, NonRecursive(Empty)) => *a = Partition::EMPTY,
            (a, _) if a.is_empty() => (),

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
                    .filter_map(|x| match x {
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

            // special case empty
            (_, b) if b.is_empty() => (),
            (a, b) if a.is_empty() => *a = b.clone(),

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
                    .filter_map(|x| match x {
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
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        match (&mut *self, rhs) {
            // special case full
            (a @ Partition::Full, b) => {
                *a = b.into();
                a.complement()
            }
            (a, NonRecursive(Full)) => a.complement(),

            // special case empty
            (_, NonRecursive(Empty)) => (),
            (a, b) if a.is_empty() => *a = b.into(),

            // use fast physical ops if both partitions share storage
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a.bitxor_assign(*bitmap),
            (Partition::Vec(a), NonRecursive(Vec { values })) => a.bitxor_assign(*values),
            (Partition::Run(a), NonRecursive(Run { runs })) => a.bitxor_assign(runs),
            (Partition::Tree(a), Tree(tree)) => a.bitxor_assign(tree),

            // otherwise fall back to logical ops
            (a, b) => {
                *a = std::mem::take(a)
                    .iter()
                    .merge_join_by(b.iter(), L::Value::cmp)
                    .filter_map(|x| match x {
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

impl<L: Level> SubAssign<&Partition<L>> for Partition<L> {
    fn sub_assign(&mut self, rhs: &Partition<L>) {
        use Partition::*;

        match (&mut *self, rhs) {
            // special case full
            (a @ Full, b) => {
                *a = b.clone();
                a.complement();
            }
            (a, Full) => *a = Partition::EMPTY,

            // special case empty
            (a, b) if a.is_empty() || b.is_empty() => (),

            // use fast physical ops if both partitions share storage
            (Bitmap(a), Bitmap(b)) => a.sub_assign(b),
            (Vec(a), Vec(b)) => a.sub_assign(b),
            (Run(a), Run(b)) => a.sub_assign(b),
            (Tree(a), Tree(b)) => a.sub_assign(b),

            // otherwise fall back to logical ops
            (a, b) => {
                *a = std::mem::take(a)
                    .iter()
                    .filter(|a| !b.contains(*a))
                    .collect();
            }
        }

        self.optimize_fast();
    }
}

impl<L: Level> SubAssign<&PartitionRef<'_, L>> for Partition<L> {
    fn sub_assign(&mut self, rhs: &PartitionRef<'_, L>) {
        use NonRecursivePartitionRef::*;
        use PartitionRef::*;

        match (&mut *self, rhs) {
            // special case full
            (a @ Partition::Full, b) => {
                *a = b.into();
                a.complement()
            }
            (a, NonRecursive(Full)) => *a = Partition::EMPTY,

            // special case empty
            (a, b) if a.is_empty() || b.is_empty() => (),

            // use fast physical ops if both partitions share storage
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a.sub_assign(*bitmap),
            (Partition::Vec(a), NonRecursive(Vec { values })) => a.sub_assign(*values),
            (Partition::Run(a), NonRecursive(Run { runs })) => a.sub_assign(runs),
            (Partition::Tree(a), Tree(tree)) => a.sub_assign(tree),

            // otherwise fall back to logical ops
            (a, b) => {
                *a = std::mem::take(a)
                    .iter()
                    .filter(|a| !b.contains(*a))
                    .collect();
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
            // special case empty
            (_, b) if b.is_empty() => Partition::default(),

            // special case full
            (a, Full) => std::mem::take(a),

            // use fast physical ops if both partitions share storage
            (Bitmap(a), Bitmap(b)) => a.cut(b),
            (Run(a), Run(b)) => a.cut(b),
            (Tree(a), Tree(b)) => a.cut(b),

            // fallback to general optimized logical ops
            (Vec(a), b) => a.cut(b),

            // otherwise fall back to logical ops
            (a, b) => {
                let mut intersection = Partition::default();
                let mut remaining = a.cardinality();
                for val in b.iter() {
                    if a.remove(val) {
                        remaining -= 1;
                        intersection.raw_insert(val);
                    }
                    if remaining == 0 {
                        debug_assert!(a.is_empty(), "BUG: cardinality out of sync");
                        break;
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

            // special case empty
            (a, NonRecursive(Full)) => std::mem::take(a),

            // use fast physical ops if both partitions share storage
            (Partition::Bitmap(a), NonRecursive(Bitmap { bitmap })) => a.cut(bitmap),
            (Partition::Run(a), NonRecursive(Run { runs })) => a.cut(runs),
            (Partition::Tree(a), Tree(b)) => a.cut(b),

            // fallback to general optimized logical ops
            (Partition::Vec(a), b) => a.cut(b),

            // otherwise fall back to logical ops
            (a, b) => {
                let mut intersection = Partition::default();
                let mut remaining = a.cardinality();
                for val in b.iter() {
                    if a.remove(val) {
                        remaining -= 1;
                        intersection.raw_insert(val);
                    }
                    if remaining == 0 {
                        debug_assert!(a.is_empty(), "BUG: cardinality out of sync");
                        break;
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
            p if p.is_empty() => {
                *p = Partition::Full;
            }
            Bitmap(p) => p.complement(),
            Vec(p) => {
                let complement_cardinality = L::MAX_LEN.saturating_sub(p.cardinality());
                if complement_cardinality > L::MAX_LEN / 2 {
                    // if the complement is more than half the universe, switch
                    // to a run partition before complementing
                    self.switch_kind(PartitionKind::Run);
                    self.complement();
                } else {
                    p.complement();
                }
            }
            Run(p) => p.complement(),
            Tree(p) => p.complement(),
        }

        self.optimize_fast();
    }
}

impl<L: Level> From<&PartitionRef<'_, L>> for Partition<L> {
    fn from(value: &PartitionRef<'_, L>) -> Self {
        use PartitionRef::*;
        match value {
            NonRecursive(p) => p.into(),
            Tree(t) => Partition::Tree(t.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use crate::{
        Cut, PartitionRead,
        level::{High, Level, Low},
        partition::{Partition, vec::VecPartition},
        traits::Complement,
    };

    #[test]
    fn test_cut_with_full_rhs_takes_lhs() {
        let set = 0u16..=1024u16;
        let mut lhs = Partition::<Low>::from_iter(set.clone());
        let rhs = Partition::<Low>::Full;

        let cut = lhs.cut(&rhs);

        assert!(lhs.is_empty());
        itertools::equal(cut.iter(), set);
    }

    #[test]
    fn test_complement_small_vec() {
        let mut partition = Partition::<High>::Vec(VecPartition::from_iter([1u32]));
        partition.complement();
        assert_matches!(partition, Partition::Run(_));
        assert_eq!(partition.cardinality(), High::MAX_LEN - 1);
    }

    #[test]
    fn test_complement_empty_full() {
        let mut partition = Partition::<High>::Vec(VecPartition::default());
        partition.complement();
        assert_eq!(partition, Partition::Full);
        partition.complement();
        assert_eq!(partition, Partition::EMPTY);
    }
}
