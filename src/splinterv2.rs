use core::fmt;
use std::{collections::BTreeMap, fmt::Debug, usize};

use bitvec::{bitbox, boxed::BitBox, order::Lsb0};
use num::cast::AsPrimitive;
use zerocopy::{LE, U16, U32};

use crate::u24::u24;

pub type Splinter = Partition<High>;

static_assertions::const_assert_eq!(std::mem::size_of::<Splinter>(), 40);

pub type Segment = u8;

pub trait SplitSegment {
    type Rest;
    fn split(self) -> (Segment, Self::Rest);
}

macro_rules! impl_split {
    ($(($ty:ty,  $as:ty)),*) => {
        $(
            impl SplitSegment for $ty {
                type Rest = $as;

                fn split(self) -> (Segment, Self::Rest) {
                    let segment: Segment = (self >> (<$as>::BITS as usize)).as_();
                    let rest: $as = self.as_();
                    (segment, rest)
                }
            }
        )*
    };
}

impl_split!((u32, u24), (u24, u16), (u16, u8));

impl SplitSegment for u8 {
    type Rest = u8;
    fn split(self) -> (Segment, Self::Rest) {
        unreachable!("invalid splinter")
    }
}

pub trait Level {
    const DEBUG_NAME: &'static str;

    type Offset;
    type LevelDown: Level;
    type Down: PartitionRead<Self::LevelDown> + PartitionWrite<Self::LevelDown> + Default + Debug;
    type Value: num::PrimInt
        + AsPrimitive<usize>
        + SplitSegment<Rest = <Self::LevelDown as Level>::Value>
        + Debug;
    const BITS: usize;
    const MAX_LEN: usize = 1 << Self::BITS;
    const VEC_LIMIT: usize = (Self::MAX_LEN) / Self::BITS;
    const PREFER_TREE: bool = Self::BITS > 8;
}

#[derive(Debug, Default)]
pub struct High;
impl Level for High {
    const DEBUG_NAME: &'static str = "High";

    type Offset = U32<LE>;
    type LevelDown = Mid;
    type Down = Partition<Self::LevelDown>;
    type Value = u32;
    const BITS: usize = 32;
}

#[derive(Debug, Default)]
pub struct Mid;
impl Level for Mid {
    const DEBUG_NAME: &'static str = "Mid";

    type Offset = U32<LE>;
    type LevelDown = Low;
    type Down = Partition<Self::LevelDown>;
    type Value = u24;
    const BITS: usize = 24;
}

#[derive(Debug, Default)]
pub struct Low;
impl Level for Low {
    const DEBUG_NAME: &'static str = "Low";

    type Offset = U16<LE>;
    type LevelDown = ();
    type Down = BitmapPartition<()>;
    type Value = u16;
    const BITS: usize = 16;
}

impl Level for () {
    const DEBUG_NAME: &'static str = "Block";

    type Offset = ();
    type LevelDown = ();
    type Down = ();
    type Value = u8;
    const BITS: usize = 8;
}

pub trait PartitionRead<L: Level> {
    /// the total number of values accessible via this partition.
    fn cardinality(&self) -> usize;

    /// returns true if this partition is empty
    fn is_empty(&self) -> bool;

    /// returns true if this partition contains the given value
    fn contains(&self, value: L::Value) -> bool;
}

impl<L: Level> PartitionRead<L> for () {
    fn cardinality(&self) -> usize {
        unreachable!("invalid splinter")
    }

    fn is_empty(&self) -> bool {
        unreachable!("invalid splinter")
    }

    fn contains(&self, _value: L::Value) -> bool {
        unreachable!("invalid splinter")
    }
}

pub trait PartitionWrite<L: Level> {
    /// Inserts the value into the partition unless it already exists.
    /// Returns `true` if the insertion occurred, otherwise `false`.
    fn insert(&mut self, value: L::Value) -> bool;
}

impl<L: Level> PartitionWrite<L> for () {
    fn insert(&mut self, _value: L::Value) -> bool {
        unreachable!("invalid splinter")
    }
}

pub enum Partition<L: Level> {
    Vec(VecPartition<L>),
    Tree(TreePartition<L>),
    Bitmap(BitmapPartition<L>),
    Full,
}

impl<L: Level> Default for Partition<L> {
    fn default() -> Self {
        if L::PREFER_TREE {
            Partition::Tree(TreePartition::default())
        } else {
            Partition::Vec(VecPartition::default())
        }
    }
}

impl<L: Level> Debug for Partition<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Partition::Tree(partition) => partition.fmt(f),
            Partition::Vec(partition) => partition.fmt(f),
            Partition::Bitmap(partition) => partition.fmt(f),
            Partition::Full => write!(f, "Full"),
        }
    }
}

impl<L: Level> PartitionRead<L> for Partition<L> {
    fn cardinality(&self) -> usize {
        match self {
            Partition::Tree(partition) => partition.cardinality(),
            Partition::Vec(partition) => partition.cardinality(),
            Partition::Bitmap(partition) => partition.cardinality(),
            Partition::Full => L::MAX_LEN,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Partition::Tree(partition) => partition.is_empty(),
            Partition::Vec(partition) => partition.is_empty(),
            Partition::Bitmap(partition) => partition.is_empty(),
            Partition::Full => false,
        }
    }

    fn contains(&self, value: L::Value) -> bool {
        debug_assert!(value.as_() < L::MAX_LEN, "value out of range");

        match self {
            Partition::Tree(partition) => partition.contains(value),
            Partition::Vec(partition) => partition.contains(value),
            Partition::Bitmap(partition) => partition.contains(value),
            Partition::Full => true,
        }
    }
}

impl<L: Level> PartitionWrite<L> for Partition<L>
where
    usize: AsPrimitive<<L as Level>::Value>,
{
    fn insert(&mut self, value: L::Value) -> bool {
        let inserted = match self {
            Partition::Tree(partition) => partition.insert(value),
            Partition::Vec(partition) => partition.insert(value),
            Partition::Bitmap(partition) => partition.insert(value),
            Partition::Full => false,
        };

        if inserted {
            let new_partition = match self {
                Partition::Tree(p) => p.maybe_change_storage(),
                Partition::Vec(p) => p.maybe_change_storage(),
                Partition::Bitmap(p) => p.maybe_change_storage(),
                _ => None,
            };

            if let Some(new_partition) = new_partition {
                *self = new_partition;
            }
        }

        inserted
    }
}

pub struct TreePartition<L: Level> {
    children: BTreeMap<Segment, L::Down>,
    cardinality: usize,
    _marker: std::marker::PhantomData<L>,
}

impl<L: Level> TreePartition<L> {
    fn maybe_change_storage(&self) -> Option<Partition<L>> {
        if self.cardinality() == L::MAX_LEN {
            return Some(Partition::Full);
        }

        // if tree is sparse and/or there is low amounts of prefix sharing
        // transform to array container or bitmap container depending on
        // cardinality
        None
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
            .map_or(false, |child| child.contains(value))
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

#[derive(Clone, PartialEq, Eq)]
pub struct VecPartition<L: Level> {
    values: Vec<L::Value>,
}

impl<L: Level> Default for VecPartition<L> {
    fn default() -> Self {
        VecPartition { values: Vec::new() }
    }
}

impl<L: Level> Debug for VecPartition<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VecPartition<{}>({})", L::DEBUG_NAME, self.cardinality())
    }
}

impl<L: Level> VecPartition<L> {
    /// Construct an VecPartition from a sorted vector of values
    #[inline]
    pub fn from_sorted(values: Vec<L::Value>) -> Self {
        VecPartition { values }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.values.iter().copied()
    }

    fn maybe_change_storage(&self) -> Option<Partition<L>> {
        if self.cardinality() == L::MAX_LEN {
            return Some(Partition::Full);
        } else if self.cardinality() > L::VEC_LIMIT {
            // TODO: if the vec is dense and small, go to tree
            Some(Partition::Bitmap(self.iter().collect()))
        } else {
            None
        }
    }
}

impl<L: Level> FromIterator<L::Value> for VecPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let mut values: Vec<L::Value> = iter.into_iter().collect();
        values.sort();
        VecPartition::from_sorted(values)
    }
}

impl<L: Level> PartitionRead<L> for VecPartition<L> {
    fn cardinality(&self) -> usize {
        self.values.len()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn contains(&self, value: L::Value) -> bool {
        self.values.binary_search(&value).is_ok()
    }
}

impl<L: Level> PartitionWrite<L> for VecPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        assert!(value.as_() < L::MAX_LEN, "value out of range");
        match self.values.binary_search(&value) {
            // value already exists
            Ok(_) => false,
            // value doesn't exist, insert it
            Err(index) => {
                self.values.insert(index, value);
                true
            }
        }
    }
}

pub struct BitmapPartition<L: Level> {
    bitmap: BitBox<u64, Lsb0>,
    _marker: std::marker::PhantomData<L>,
}

impl<L: Level> Default for BitmapPartition<L> {
    fn default() -> Self {
        Self {
            bitmap: bitbox![u64, Lsb0; 0; L::MAX_LEN],
            _marker: std::marker::PhantomData,
        }
    }
}

impl<L: Level> Debug for BitmapPartition<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BitmapPartition<{}>({})",
            L::DEBUG_NAME,
            self.cardinality()
        )
    }
}

impl<L: Level> BitmapPartition<L>
where
    usize: AsPrimitive<<L as Level>::Value>,
{
    fn maybe_change_storage(&self) -> Option<Partition<L>> {
        let cardinality = self.cardinality();
        if cardinality == L::MAX_LEN {
            Some(Partition::Full)
        } else if cardinality <= L::VEC_LIMIT {
            // TODO: if the bitmap is dense and small, go to tree
            Some(Partition::Vec(self.iter().collect()))
        } else {
            None
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.bitmap.iter_ones().map(|i| i.as_())
    }
}

impl<L: Level> FromIterator<L::Value> for BitmapPartition<L> {
    fn from_iter<I: IntoIterator<Item = L::Value>>(iter: I) -> Self {
        let mut bitmap = bitbox![u64, Lsb0; 0; L::MAX_LEN];
        for v in iter {
            bitmap.set(v.as_(), true);
        }
        BitmapPartition {
            bitmap,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<L: Level> PartitionRead<L> for BitmapPartition<L> {
    fn cardinality(&self) -> usize {
        self.bitmap.count_ones()
    }

    fn is_empty(&self) -> bool {
        self.bitmap.not_any()
    }

    fn contains(&self, value: L::Value) -> bool {
        self.bitmap.get(value.as_()).is_some()
    }
}

impl<L: Level> PartitionWrite<L> for BitmapPartition<L> {
    fn insert(&mut self, value: L::Value) -> bool {
        let mut bit = self
            .bitmap
            .get_mut(value.as_())
            .expect("value out of range");
        !bit.replace(true)
    }
}

#[cfg(test)]
mod tests {

    use crate::testutil::SetGen;

    use super::*;

    #[test]
    fn test_sanity() {
        let mut splinter = Splinter::default();

        assert!(splinter.insert(1));
        assert!(!splinter.insert(1));
        assert!(splinter.contains(1));

        let values = [1024, 123, 16384];
        for v in values {
            assert!(splinter.insert(v));
            assert!(splinter.contains(v));
            assert!(!splinter.contains(v + 1));
        }

        for i in 0..8192 + 10 {
            splinter.insert(i);
        }

        dbg!(splinter);
    }

    #[test]
    fn test_wat() {
        let mut set_gen = SetGen::new(0xDEADBEEF);
        let set = set_gen.distributed(8, 8, 16, 32, 32768);
        let mut splinter = Splinter::default();
        for v in set {
            assert!(splinter.insert(v));
        }
        dbg!(splinter);
    }
}
