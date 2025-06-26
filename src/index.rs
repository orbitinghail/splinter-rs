use itertools::izip;
use zerocopy::{FromBytes, Immutable};

use crate::{
    Segment,
    block::{BlockRef, block_size},
};

#[inline]
pub(crate) const fn index_serialized_size<Offset: Sized>(cardinality: usize) -> usize {
    let block_size = block_size(cardinality);
    block_size + cardinality + (size_of::<Offset>() * cardinality)
}

#[derive(Clone)]
pub struct IndexRef<'a, Offset> {
    keys: BlockRef<'a>,
    cardinalities: &'a [u8],
    offsets: &'a [Offset],
}

impl<'a, Offset> IndexRef<'a, Offset>
where
    Offset: FromBytes + Immutable + Copy + Into<u32> + 'a,
{
    pub fn from_suffix(data: &'a [u8], cardinality: usize) -> (&'a [u8], Self) {
        let index_size = index_serialized_size::<Offset>(cardinality);
        assert!(data.len() >= index_size, "data too short");
        let (data, index) = data.split_at(data.len() - index_size);
        (data, Self::from_bytes(index, cardinality))
    }

    fn from_bytes(index: &'a [u8], cardinality: usize) -> Self {
        let (keys, index) = index.split_at(block_size(cardinality));
        let (cardinalities, index) = index.split_at(cardinality);
        let offsets =
            <[Offset]>::ref_from_bytes_with_elems(index, cardinality).expect("offsets too short");

        Self {
            keys: if cardinality == 256 {
                BlockRef::Full
            } else {
                BlockRef::from_bytes(keys)
            },
            cardinalities,
            offsets,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.cardinalities.len()
    }

    /// Returns the cardinality of the index by summing all of
    /// the index's entry cardinalities
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.cardinalities.iter().map(|&x| x as usize + 1).sum()
    }

    /// Lookup the segment in the index
    /// Returns the segment's cardinality and offset
    pub fn lookup(&self, segment: u8) -> Option<(usize, usize)> {
        if self.keys.contains(segment) {
            let rank = self.keys.rank(segment);
            self.get(rank - 1)
        } else {
            None
        }
    }

    /// Get the cardinality and offset of the segment at the given index
    pub fn get(&self, index: usize) -> Option<(usize, usize)> {
        if index < self.len() {
            let cardinality = self.cardinalities[index] as usize + 1;
            let offset = self.offsets[index].into() as usize;
            Some((cardinality, offset))
        } else {
            None
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = (Segment, usize, usize)> + 'a {
        let segments = self.keys.into_segments();
        let cardinalities = self.cardinalities.iter().map(|&x| x as usize + 1);
        let offsets = self.offsets.iter().map(|&x| x.into() as usize);

        // zip the segments, cardinalities, and offsets together
        izip!(segments, cardinalities, offsets)
    }

    /// Returns the last segment in the index along with its cardinality and
    /// offset if the index is non-empty
    pub fn last(&self) -> Option<(Segment, usize, usize)> {
        self.keys.last().and_then(|segment| {
            let last_offset = self.len().checked_sub(1).expect("index out of sync");
            self.get(last_offset)
                .map(|(cardinality, offset)| (segment, cardinality, offset))
        })
    }
}
