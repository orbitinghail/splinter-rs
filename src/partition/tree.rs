use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt::{self, Debug},
};

use bytes::BufMut;
use itertools::{FoldWhile, Itertools};

use crate::{
    codec::{
        Encodable,
        encoder::Encoder,
        tree_ref::{TreeIndexBuilder, TreeRef},
    },
    count::count_runs_sorted,
    level::Level,
    partition::Partition,
    segment::{Segment, SplitSegment},
    traits::{Cut, Merge, Optimizable, PartitionRead, PartitionWrite},
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
        let mut partition = TreePartition::default();
        for value in iter {
            partition.insert(value);
        }
        partition
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

    fn rank(&self, value: L::Value) -> usize {
        let (segment, value) = value.split();
        self.children
            .iter()
            .fold_while(0, move |acc, (&child_segment, child)| {
                if child_segment < segment {
                    FoldWhile::Continue(acc + child.cardinality())
                } else if child_segment == segment {
                    FoldWhile::Continue(acc + child.rank(value))
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

impl<L: Level> Merge for TreePartition<L> {
    fn merge(&mut self, rhs: &Self) {
        for (&segment, child) in rhs.children.iter() {
            self.children.entry(segment).or_default().merge(child);
        }
        self.refresh_cardinality();
    }
}

impl<L: Level> Merge<TreeRef<'_, L>> for TreePartition<L> {
    fn merge(&mut self, rhs: &TreeRef<'_, L>) {
        let zipped = rhs.segments().zip(rhs.children());
        for (segment, child) in zipped {
            self.children.entry(segment).or_default().merge(&child);
        }
        self.refresh_cardinality();
    }
}

impl<L: Level> Cut for TreePartition<L> {
    type Out = Partition<L>;

    fn cut(&mut self, rhs: &Self) -> Partition<L> {
        let mut intersection = Self::default();

        for (segment, child) in self.children.iter_mut() {
            if let Some(other) = rhs.children.get(segment) {
                intersection.children.insert(*segment, child.cut(other));
            }
        }

        // remove empty children
        self.children.retain(|_, c| !c.is_empty());

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
                intersection.children.insert(segment, child.cut(&other));
            }
        }

        // remove empty children
        self.children.retain(|_, c| !c.is_empty());

        self.refresh_cardinality();
        intersection.refresh_cardinality();

        Partition::Tree(intersection)
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::{
        level::Low,
        partition::tree::TreePartition,
        testutil::{test_partition_read, test_partition_write},
    };

    #[quickcheck]
    fn test_tree_small_read_quickcheck(set: Vec<u16>) -> TestResult {
        let expected = set.iter().copied().sorted().dedup().collect_vec();
        let partition = TreePartition::<Low>::from_iter(set);
        test_partition_read(&partition, &expected);
        TestResult::passed()
    }

    #[quickcheck]
    fn test_tree_small_write_quickcheck(set: Vec<u16>) -> TestResult {
        let mut partition = TreePartition::<Low>::from_iter(set);
        test_partition_write(&mut partition);
        TestResult::passed()
    }
}
