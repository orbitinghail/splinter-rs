use itertools::izip;
use zerocopy::{FromBytes, Immutable};

use crate::{
    Segment,
    block::{BlockRef, block_size},
};

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
    #[inline]
    fn serialized_size(cardinality: usize) -> usize {
        let block_size = block_size(cardinality);
        block_size + cardinality + (size_of::<Offset>() * cardinality)
    }

    pub fn from_suffix(data: &'a [u8], cardinality: usize) -> (&'a [u8], Self) {
        let index_size = Self::serialized_size(cardinality);
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
        self.keys.rank(segment).and_then(|idx| self.get(idx))
    }

    /// Returns the total number of values preceding `segment` and, if present,
    /// the cardinality and offset for `segment` itself.
    pub fn rank(&self, segment: u8) -> (usize, Option<(usize, usize)>) {
        let idx_opt = self.keys.rank(segment);
        let prefix_idx = match idx_opt {
            Some(i) => i,
            None => self.keys.prefix_len(segment),
        };
        let mut prefix = 0usize;
        for &c in &self.cardinalities[..prefix_idx.min(self.len())] {
            prefix += c as usize + 1;
        }
        if let Some(idx) = idx_opt {
            (prefix, self.get(idx))
        } else {
            (prefix, None)
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
}
