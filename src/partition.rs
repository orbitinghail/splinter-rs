use std::{
    collections::{BTreeMap, btree_map::Entry},
    convert::TryInto,
    fmt::Debug,
    marker::PhantomData,
    mem::size_of,
    ops::RangeInclusive,
};

use bytes::BufMut;
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::{
    Segment,
    bitmap::BitmapMutExt,
    block::Block,
    index::{IndexRef, index_serialized_size},
    ops::Merge,
    relational::Relation,
    util::{CopyToOwned, FromSuffix, SerializeContainer},
};

#[derive(Clone)]
pub struct Partition<Offset, V> {
    values: BTreeMap<Segment, V>,
    _phantom: PhantomData<Offset>,
}

impl<O, V> Default for Partition<O, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
            _phantom: Default::default(),
        }
    }
}

impl<O, V> Partition<O, V>
where
    V: Default,
{
    pub fn get_or_init(&mut self, segment: Segment) -> &mut V {
        self.values.entry(segment).or_default()
    }
}

impl<O, V> Partition<O, V> {
    pub const EMPTY: Self = Self {
        values: BTreeMap::new(),
        _phantom: PhantomData,
    };

    /// Inserts a value into the partition.
    ///
    /// # Panics
    ///
    /// Panics if the segment is already present.
    pub fn insert(&mut self, segment: Segment, value: V) {
        assert!(
            self.values.insert(segment, value).is_none(),
            "segment already present in partition"
        );
    }

    pub fn retain(&mut self, f: impl FnMut(&Segment, &mut V) -> bool) {
        self.values.retain(f);
    }

    pub fn last(&self) -> Option<(Segment, &V)> {
        self.values.last_key_value().map(|(k, v)| (*k, v))
    }
}

impl<O, V> Merge for Partition<O, V>
where
    V: Merge + Clone,
{
    fn merge(&mut self, rhs: &Self) {
        for (k, v) in rhs.iter() {
            match self.values.entry(k) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().merge(v);
                }
                Entry::Vacant(entry) => {
                    entry.insert(v.clone());
                }
            }
        }
    }
}

impl<'a, O, V, Rv> Merge<PartitionRef<'a, O, Rv>> for Partition<O, V>
where
    O: FromBytes + Immutable + Copy + Into<u32>,
    V: Merge<Rv>,
    Rv: FromSuffix<'a> + CopyToOwned<Owned = V>,
{
    fn merge(&mut self, rhs: &PartitionRef<'a, O, Rv>) {
        for (k, v) in rhs.iter() {
            match self.values.entry(k) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().merge(&v);
                }
                Entry::Vacant(entry) => {
                    entry.insert(v.copy_to_owned());
                }
            }
        }
    }
}

impl<O, V> Relation for Partition<O, V> {
    type ValRef<'a>
        = &'a V
    where
        Self: 'a;

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    fn iter(&self) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
        self.values.iter().map(|(k, v)| (*k, v))
    }

    fn get(&self, key: Segment) -> Option<Self::ValRef<'_>> {
        self.values.get(&key)
    }

    fn range(
        &self,
        range: RangeInclusive<Segment>,
    ) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
        self.values.range(range).map(|(k, v)| (*k, v))
    }
}

impl<O, V> SerializeContainer for Partition<O, V>
where
    V: SerializeContainer,
    O: TryFrom<u32, Error: Debug> + IntoBytes + Immutable,
{
    fn should_serialize(&self) -> bool {
        self.values.values().any(|v| v.should_serialize())
    }

    fn serialized_size(&self) -> usize {
        let index_size = index_serialized_size::<O>(self.values.len());
        self.values
            .iter()
            .fold(index_size, |acc, (_, value)| acc + value.serialized_size())
    }

    fn serialize<B: BufMut>(&self, out: &mut B) -> (usize, usize) {
        // keep track of segments, cardinalities, and offsets as we flush values
        let mut index = Block::default();
        let mut cardinalities: Vec<u8> = Vec::with_capacity(self.values.len());
        let mut offsets = Vec::with_capacity(self.values.len());
        let mut offset: u32 = 0;

        for (&segment, value) in self.values.iter().filter(|(_, v)| v.should_serialize()) {
            let (cardinality, n) = value.serialize(out);
            index.insert(segment);
            cardinalities.push((cardinality - 1).try_into().expect("cardinality overflow"));
            offset += TryInto::<u32>::try_into(n).expect("offset overflow");
            offsets.push(offset);
        }

        // write out the index
        // index keys
        let (cardinality, keys_size) = index.serialize(out);
        assert_eq!(cardinality, self.values.len(), "cardinality mismatch");

        // index cardinalities
        let cardinalities_size = cardinalities.len();
        out.put_slice(&cardinalities);

        // index offsets
        let offsets_size = offsets.len() * size_of::<O>();
        for value_offset in offsets {
            let value_offset = O::try_from(offset - value_offset).expect("offset overflow");
            out.put_slice(value_offset.as_bytes());
        }

        (
            cardinality,
            (offset as usize) + keys_size + cardinalities_size + offsets_size,
        )
    }
}

pub struct PartitionRef<'a, Offset, V> {
    values: &'a [u8],
    index: IndexRef<'a, Offset>,
    _phantom: PhantomData<V>,
}

impl<'a, Offset, V> FromSuffix<'a> for PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
{
    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self {
        let (values, index) = IndexRef::from_suffix(data, cardinality);
        Self { values, index, _phantom: PhantomData }
    }
}

impl<'a, Offset, V> CopyToOwned for PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: CopyToOwned + FromSuffix<'a>,
{
    type Owned = Partition<Offset, V::Owned>;

    fn copy_to_owned(&self) -> Self::Owned {
        let values: BTreeMap<Segment, V::Owned> =
            self.iter().map(|(k, v)| (k, v.copy_to_owned())).collect();
        Partition { values, _phantom: PhantomData }
    }
}

impl<'a, Offset, V> PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a> + 'a,
{
    /// Returns the cardinality of the partition by summing the cardinalities
    /// stored in the partition's index
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.index.cardinality()
    }

    pub fn into_iter(self) -> impl Iterator<Item = (Segment, V)> + 'a {
        PartitionRefIter {
            values: self.values,
            index_iter: self.index.into_iter(),
            _phantom: PhantomData,
        }
    }

    pub fn into_range(
        self,
        range: RangeInclusive<Segment>,
    ) -> impl Iterator<Item = (Segment, V)> + 'a {
        let r2 = range.clone();
        self.into_iter()
            .skip_while(move |(k, _)| !r2.contains(k))
            .take_while(move |(k, _)| range.contains(k))
    }

    pub fn last(&self) -> Option<(Segment, V)> {
        if let Some((segment, cardinality, offset)) = self.index.last() {
            Some((
                segment,
                read_partition_ref_value(cardinality, offset, self.values),
            ))
        } else {
            None
        }
    }
}

fn read_partition_ref_value<'a, V: FromSuffix<'a>>(
    cardinality: usize,
    offset: usize,
    values: &'a [u8],
) -> V {
    assert!(values.len() >= offset, "offset out of range");
    let data = &values[..(values.len() - offset)];
    V::from_suffix(data, cardinality)
}

struct PartitionRefIter<'a, I, V> {
    values: &'a [u8],
    index_iter: I,
    _phantom: PhantomData<V>,
}

impl<'a, I, V> Iterator for PartitionRefIter<'a, I, V>
where
    I: Iterator<Item = (Segment, usize, usize)> + 'a,
    V: FromSuffix<'a>,
{
    type Item = (Segment, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((segment, cardinality, offset)) = self.index_iter.next() {
            Some((
                segment,
                read_partition_ref_value(cardinality, offset, self.values),
            ))
        } else {
            None
        }
    }
}

impl<'a, Offset, V> Relation for PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a>,
{
    type ValRef<'b>
        = V
    where
        Self: 'b;

    #[inline]
    fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    fn iter(&self) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
        PartitionRefIter {
            values: self.values,
            index_iter: self.index.clone().into_iter(),
            _phantom: PhantomData,
        }
    }

    fn get(&self, key: Segment) -> Option<Self::ValRef<'_>> {
        if let Some((cardinality, offset)) = self.index.lookup(key) {
            Some(read_partition_ref_value(cardinality, offset, self.values))
        } else {
            None
        }
    }

    #[inline]
    fn range(
        &self,
        range: RangeInclusive<Segment>,
    ) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
        let r2 = range.clone();
        self.iter()
            .skip_while(move |(k, _)| !r2.contains(k))
            .take_while(move |(k, _)| range.contains(k))
    }
}

// Equality Operations

// Partition == Partition
impl<O, V: PartialEq> PartialEq for Partition<O, V> {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

// PartitionRef == PartitionRef
impl<'a, Offset, V> PartialEq for PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a> + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

// PartitionRef == Partition
impl<'a, O, V, V2> PartialEq<Partition<O, V2>> for PartitionRef<'a, O, V>
where
    O: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a> + PartialEq<V2>,
{
    fn eq(&self, other: &Partition<O, V2>) -> bool {
        if self.len() != other.values.len() {
            return false;
        }
        for ((k1, v1), (k2, v2)) in self.iter().zip(other.iter()) {
            if k1 != k2 || v1 != *v2 {
                return false;
            }
        }
        true
    }
}

// Partition == PartitionRef
impl<'a, O, V, V2> PartialEq<PartitionRef<'a, O, V>> for Partition<O, V2>
where
    O: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a> + PartialEq<V2>,
{
    fn eq(&self, other: &PartitionRef<'a, O, V>) -> bool {
        other == self
    }
}
