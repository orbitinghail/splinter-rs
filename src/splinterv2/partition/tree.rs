use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
};

use crate::splinterv2::{
    count::count_runs_sorted,
    encode::Encodable,
    level::{Block, Level},
    partition::{bitmap::BitmapPartition, vec::VecPartition},
    segment::{Segment, SplitSegment},
    traits::{Optimizable, PartitionRead, PartitionWrite},
};

#[derive(Clone, PartialEq, Eq)]
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
}

impl<L: Level> Encodable for TreePartition<L> {
    fn encoded_size(&self) -> usize {
        let index = {
            let vec_size = VecPartition::<Block>::encoded_size(self.children.len());
            let bitmap_size = BitmapPartition::<Block>::ENCODED_SIZE;
            vec_size.min(bitmap_size)
        };
        let offsets = self.children.len() * std::mem::size_of::<L::Offset>();
        let values: usize = self.children.values().map(|c| c.encoded_size()).sum();
        index + offsets + values
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
}
