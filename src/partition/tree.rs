use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt::{self, Debug},
    marker::PhantomData,
    ops::{BitAndAssign, BitOrAssign, BitXorAssign, RangeBounds, SubAssign},
};

use bytes::BufMut;
use itertools::{EitherOrBoth, FoldWhile, Itertools};

use crate::{
    codec::{
        Encodable,
        encoder::Encoder,
        tree_ref::{TreeIndexBuilder, TreeRef},
    },
    count::count_runs_sorted,
    level::Level,
    partition::Partition,
    segment::{IterSegmented, Segment, SplitSegment},
    traits::{Complement, Cut, DefaultFull, Optimizable, PartitionRead, PartitionWrite},
    util::RangeExt,
};

#[derive(Clone, Eq)]
pub struct TreePartition<L: Level> {
    children: BTreeMap<Segment, L::Down>,
    cardinality: usize,
    _marker: std::marker::PhantomData<L>,
}

impl<L: Level> TreePartition<L> {
    pub fn sparsity_ratio(&self) -> f64 {
        self.children.len() as f64 / self.cardinality as f64
    }

    #[inline]
    pub fn count_runs(&self) -> usize {
        count_runs_sorted(self.iter())
    }

    pub fn optimize_children(&mut self) {
        for child in self.children.values_mut() {
            child.optimize();
        }
    }

    fn refresh_cardinality(&mut self) {
        self.cardinality = self.children.values().map(|c| c.cardinality()).sum();
    }
}

impl<L: Level> Encodable for TreePartition<L> {
    fn encoded_size(&self) -> usize {
        let index_size = TreeIndexBuilder::<L>::encoded_size(self.children.len());
        let values: usize = self.children.values().map(|c| c.encoded_size()).sum();
        // values + index
        values + index_size
    }

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>) {
        let mut index = TreeIndexBuilder::<L>::new(self.children.len());
        for (&segment, child) in self.children.iter() {
            child.encode(encoder);
            index.push(segment, encoder.bytes_written());
        }
        encoder.put_tree_index(index);
    }
}

impl<L: Level> Debug for TreePartition<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TreePartition<{}>", L::DEBUG_NAME)?;
        self.children.fmt(f)
    }
}

impl<L: Level> Default for TreePartition<L> {
    fn default() -> Self {
        Self {
            children: BTreeMap::new(),
            cardinality: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<L: Level> FromIterator<L::Value> for TreePartition<L> {
    fn from_iter<T: IntoIterator<Item = L::Value>>(iter: T) -> Self {
        let mut segmented = IterSegmented::new(iter.into_iter());
        let mut tree = TreePartition::default();

        let Some((mut child_segment, first_value)) = segmented.next() else {
            return tree;
        };

        // we amortize the cost of looking up child partitions to optimize the
        // common case of initializing a tree partition from an iterator of
        // sorted values

        let mut child = tree.children.entry(child_segment).or_default();

        child.insert(first_value);
        tree.cardinality += 1;

        for (segment, value) in segmented {
            if segment != child_segment {
                child_segment = segment;
                child = tree.children.entry(child_segment).or_default();
            }

            if child.insert(value) {
                tree.cardinality += 1;
            }
        }

        tree
    }
}

impl<L: Level> PartitionRead<L> for TreePartition<L> {
    fn cardinality(&self) -> usize {
        self.cardinality
    }

    fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    fn contains(&self, value: L::Value) -> bool {
        let (segment, value) = value.split();
        self.children
            .get(&segment)
            .is_some_and(|child| child.contains(value))
    }

    fn position(&self, value: L::Value) -> Option<usize> {
        let (segment, value) = value.split();
        let mut found = false;
        let pos = self
            .children
            .iter()
            .fold_while(0, |acc, (&child_segment, child)| {
                if child_segment < segment {
                    FoldWhile::Continue(acc + child.cardinality())
                } else if child_segment == segment {
                    if let Some(pos) = child.position(value) {
                        found = true;
                        FoldWhile::Done(acc + pos)
                    } else {
                        FoldWhile::Done(acc)
                    }
                } else {
                    FoldWhile::Done(acc)
                }
            })
            .into_inner();

        found.then_some(pos)
    }

    fn rank(&self, value: L::Value) -> usize {
        let (segment, value) = value.split();
        self.children
            .iter()
            .fold_while(0, |acc, (&child_segment, child)| {
                if child_segment < segment {
                    FoldWhile::Continue(acc + child.cardinality())
                } else if child_segment == segment {
                    FoldWhile::Done(acc + child.rank(value))
                } else {
                    FoldWhile::Done(acc)
                }
            })
            .into_inner()
    }

    fn select(&self, mut n: usize) -> Option<L::Value> {
        for (&segment, child) in self.children.iter() {
            let len = child.cardinality();
            if n < len {
                return child.select(n).map(|v| L::Value::unsplit(segment, v));
            }
            n -= len;
        }
        None
    }

    fn last(&self) -> Option<L::Value> {
        if let Some((&segment, child)) = self.children.last_key_value() {
            child.last().map(|v| L::Value::unsplit(segment, v))
        } else {
            None
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.children.iter().flat_map(|(&segment, child)| {
            child
                .iter()
                .map(move |value| L::Value::unsplit(segment, value))
        })
    }
}

impl<L: Level> PartitionWrite<L> for TreePartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let (segment, value) = value.split();
        if self.children.entry(segment).or_default().insert(value) {
            self.cardinality += 1;
            true
        } else {
            false
        }
    }

    fn remove(&mut self, value: L::Value) -> bool {
        let (segment, value) = value.split();
        match self.children.entry(segment) {
            Entry::Vacant(_) => (),
            Entry::Occupied(mut child) => {
                if child.get_mut().remove(value) {
                    if child.get().is_empty() {
                        child.remove();
                    }
                    self.cardinality -= 1;
                    return true;
                }
            }
        }
        false
    }

    fn remove_range<R: RangeBounds<L::Value>>(&mut self, values: R) {
        if let Some(values) = values.try_into_inclusive() {
            let p1 = (*values.start()).segment_end().min(*values.end());
            let p2 = (*values.end()).segment_start().max(*values.start());
            let segments = values.start().segment()..=values.end().segment();

            self.children.retain(|segment, child| {
                // special case first and last segment
                if segment == segments.start() {
                    let range = values.start().rest()..=p1.rest();
                    child.remove_range(range);
                    !child.is_empty()
                } else if segment == segments.end() {
                    let range = p2.rest()..=values.end().rest();
                    child.remove_range(range);
                    !child.is_empty()
                } else {
                    // this segment is fully contained in the range, drop it entirely
                    false
                }
            });

            self.refresh_cardinality();
        }
    }
}

impl<L: Level> PartialEq for TreePartition<L> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.children == other.children
    }
}

impl<L: Level> PartialEq<TreeRef<'_, L>> for TreePartition<L> {
    #[inline]
    fn eq(&self, other: &TreeRef<'_, L>) -> bool {
        // we want to fast path compare segments, and then recurse to comparing
        // each child
        itertools::equal(self.children.keys().copied(), other.segments())
            && self
                .children
                .values()
                .zip(other.children())
                .all(|(a, b)| *a == b)
    }
}

impl<L: Level> BitOrAssign<&TreePartition<L>> for TreePartition<L> {
    fn bitor_assign(&mut self, rhs: &Self) {
        for (&segment, child) in rhs.children.iter() {
            self.children
                .entry(segment)
                .or_default()
                .bitor_assign(child);
        }
        self.refresh_cardinality();
    }
}

impl<L: Level> BitOrAssign<&TreeRef<'_, L>> for TreePartition<L> {
    fn bitor_assign(&mut self, rhs: &TreeRef<'_, L>) {
        let zipped = rhs.segments().zip(rhs.children());
        for (segment, child) in zipped {
            self.children
                .entry(segment)
                .or_default()
                .bitor_assign(&child);
        }
        self.refresh_cardinality();
    }
}

impl<L: Level> BitAndAssign<&TreePartition<L>> for TreePartition<L> {
    fn bitand_assign(&mut self, rhs: &Self) {
        self.children.retain(|segment, child| {
            if let Some(rhs_child) = rhs.children.get(segment) {
                child.bitand_assign(rhs_child);
                !child.is_empty()
            } else {
                false
            }
        });
        self.refresh_cardinality();
    }
}

impl<L: Level> BitAndAssign<&TreeRef<'_, L>> for TreePartition<L> {
    fn bitand_assign(&mut self, rhs: &TreeRef<'_, L>) {
        self.children.retain(|&segment, child| {
            if let Some(rhs_child) = rhs.load_child_at_segment(segment) {
                child.bitand_assign(&rhs_child);
                !child.is_empty()
            } else {
                false
            }
        });
        self.refresh_cardinality();
    }
}

impl<L: Level> BitXorAssign<&TreePartition<L>> for TreePartition<L> {
    fn bitxor_assign(&mut self, rhs: &Self) {
        self.children = std::mem::take(&mut self.children)
            .into_iter()
            .merge_join_by(rhs.children.iter(), |(l, _), (r, _)| l.cmp(r))
            .flat_map(|x| match x {
                EitherOrBoth::Both((s, mut l), (_, r)) => {
                    l.bitxor_assign(r);
                    (!l.is_empty()).then_some((s, l))
                }
                EitherOrBoth::Left(l) => Some(l),
                EitherOrBoth::Right((&s, r)) => Some((s, r.clone())),
            })
            .collect();
        self.refresh_cardinality();
    }
}

impl<L: Level> BitXorAssign<&TreeRef<'_, L>> for TreePartition<L> {
    fn bitxor_assign(&mut self, rhs: &TreeRef<'_, L>) {
        let zipped = rhs.segments().zip(rhs.children());
        self.children = std::mem::take(&mut self.children)
            .into_iter()
            .merge_join_by(zipped, |(l, _), (r, _)| l.cmp(r))
            .flat_map(|x| match x {
                EitherOrBoth::Both((s, mut l), (_, r)) => {
                    l.bitxor_assign(&r);
                    (!l.is_empty()).then_some((s, l))
                }
                EitherOrBoth::Left(l) => Some(l),
                EitherOrBoth::Right((s, r)) => Some((s, L::Down::from(&r))),
            })
            .collect();
        self.refresh_cardinality();
    }
}

impl<L: Level> SubAssign<&TreePartition<L>> for TreePartition<L> {
    fn sub_assign(&mut self, rhs: &Self) {
        self.children.retain(|segment, child| {
            if let Some(rhs_child) = rhs.children.get(segment) {
                child.sub_assign(rhs_child);
                !child.is_empty()
            } else {
                true
            }
        });
        self.refresh_cardinality();
    }
}

impl<L: Level> SubAssign<&TreeRef<'_, L>> for TreePartition<L> {
    fn sub_assign(&mut self, rhs: &TreeRef<'_, L>) {
        self.children.retain(|&segment, child| {
            if let Some(rhs_child) = rhs.load_child_at_segment(segment) {
                child.sub_assign(&rhs_child);
                !child.is_empty()
            } else {
                true
            }
        });
        self.refresh_cardinality();
    }
}

impl<L: Level> Cut for TreePartition<L> {
    type Out = Partition<L>;

    fn cut(&mut self, rhs: &Self) -> Partition<L> {
        let mut intersection = Self::default();

        self.children.retain(|&segment, child| {
            if let Some(other) = rhs.children.get(&segment) {
                let child_intersection = child.cut(other);
                if !child_intersection.is_empty() {
                    intersection.children.insert(segment, child_intersection);
                }
                !child.is_empty()
            } else {
                true
            }
        });

        self.refresh_cardinality();
        intersection.refresh_cardinality();

        Partition::Tree(intersection)
    }
}

impl<L: Level> Cut<TreeRef<'_, L>> for TreePartition<L> {
    type Out = Partition<L>;

    fn cut(&mut self, rhs: &TreeRef<'_, L>) -> Self::Out {
        let mut intersection = Self::default();
        let zipped = rhs.segments().zip(rhs.children());

        for (segment, other) in zipped {
            if let Some(child) = self.children.get_mut(&segment) {
                let child_intersection = child.cut(&other);
                if !child_intersection.is_empty() {
                    intersection.children.insert(segment, child_intersection);
                }
            }
        }

        // remove empty children
        self.children.retain(|_, c| !c.is_empty());

        self.refresh_cardinality();
        intersection.refresh_cardinality();

        Partition::Tree(intersection)
    }
}

impl<L: Level> Complement for TreePartition<L> {
    fn complement(&mut self) {
        for segment in 0..=Segment::MAX {
            match self.children.entry(segment) {
                Entry::Vacant(vacant) => {
                    vacant.insert(L::Down::full());
                }
                Entry::Occupied(mut child) => {
                    child.get_mut().complement();
                    if child.get().is_empty() {
                        child.remove();
                    }
                }
            }
        }
        self.refresh_cardinality();
    }
}

impl<L: Level> From<&TreeRef<'_, L>> for TreePartition<L> {
    fn from(value: &TreeRef<'_, L>) -> Self {
        let children = value
            .segments()
            .zip(value.children())
            .map(|(s, c)| (s, L::Down::from(&c)))
            .collect();
        let mut partition = TreePartition {
            children,
            cardinality: 0,
            _marker: PhantomData,
        };
        partition.refresh_cardinality();
        partition
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use itertools::Itertools;
    use proptest::proptest;

    use crate::{
        level::Low,
        partition::tree::TreePartition,
        testutil::{test_partition_read, test_partition_write},
    };

    proptest! {
        #[test]
        fn test_tree_small_read_proptest(set: HashSet<u16>)  {
            let expected = set.iter().copied().sorted().collect_vec();
            let partition = TreePartition::<Low>::from_iter(set);
            test_partition_read(&partition, &expected);
        }

        #[test]
        fn test_tree_small_write_proptest(set: HashSet<u16>)  {
            let mut partition = TreePartition::<Low>::from_iter(set);
            test_partition_write(&mut partition);
        }
    }
}
