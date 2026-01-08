use num::traits::AsPrimitive;
use std::{marker::PhantomData, mem::size_of};
use zerocopy::FromBytes;

use crate::{
    PartitionRead, PartitionWrite,
    codec::{
        DecodeErr,
        partition_ref::{NonRecursivePartitionRef, PartitionRef, decode_len_from_suffix},
    },
    level::{Block, Level},
    partition::{Partition, bitmap::BitmapPartition},
    partition_kind::PartitionKind,
    segment::{Segment, SplitSegment},
    traits::TruncateFrom,
    util::{IteratorExt, RangeExt},
};

#[derive(Debug, Clone, Eq)]
pub struct TreeRef<'a, L: Level> {
    num_children: usize,
    segments: NonRecursivePartitionRef<'a, Block>,
    offsets: &'a [L::ValueUnaligned],
    cumulative_cardinalities: &'a [L::ValueUnaligned],
    children: &'a [u8],
}

impl<'a, L: Level> TreeRef<'a, L> {
    pub(super) fn from_suffix(data: &'a [u8]) -> Result<Self, DecodeErr> {
        let (data, num_children) = decode_len_from_suffix::<Block>(data)?;
        assert_ne!(
            num_children, 0,
            "BUG: encoded tree partition with 0 children"
        );

        let (segments_size, segments_kind) =
            TreeIndexBuilder::<L>::pick_segments_store(num_children);
        let cardinalities_size = TreeIndexBuilder::<L>::cardinalities_size(num_children);
        let offsets_size = TreeIndexBuilder::<L>::offsets_size(num_children);

        DecodeErr::ensure_bytes_available(data, segments_size + cardinalities_size + offsets_size)?;

        // Parse from end backwards:
        // 1. segments (just before num_children)
        // 2. cardinalities (just before segments)
        // 3. offsets (just before cardinalities)
        // 4. children_data (remainder)
        let segments_range = (data.len() - segments_size)..data.len();
        let cardinalities_range = (segments_range.start - cardinalities_size)..segments_range.start;
        let offsets_range = (cardinalities_range.start - offsets_size)..cardinalities_range.start;
        let data_range = 0..offsets_range.start;

        Ok(Self {
            num_children,
            segments: NonRecursivePartitionRef::tree_segments_from_suffix(
                segments_kind,
                num_children,
                &data[segments_range],
            )?,
            offsets: <[L::ValueUnaligned]>::ref_from_bytes_with_elems(
                &data[offsets_range],
                num_children,
            )?,
            cumulative_cardinalities: <[L::ValueUnaligned]>::ref_from_bytes_with_elems(
                &data[cardinalities_range],
                num_children,
            )?,
            children: &data[data_range],
        })
    }

    fn load_child(&self, idx: usize) -> PartitionRef<'a, L::LevelDown> {
        let relative_offset: usize = self.offsets[idx].into().as_();
        let offset = self.children.len() - relative_offset;
        PartitionRef::from_suffix(&self.children[..offset]).unwrap()
    }

    /// Returns the cumulative cardinality before the given index (sum of all children < idx)
    #[inline]
    fn prefix_cardinality(&self, idx: usize) -> usize {
        if idx == 0 {
            0
        } else {
            // Add 1 since we store cumulative - 1 to avoid overflow
            let encoded: usize = self.cumulative_cardinalities[idx - 1].into().as_();
            encoded + 1
        }
    }

    pub(crate) fn load_child_at_segment(
        &self,
        segment: Segment,
    ) -> Option<PartitionRef<'a, L::LevelDown>> {
        self.segments.position(segment).map(|p| self.load_child(p))
    }

    pub(crate) fn segments(&self) -> impl Iterator<Item = Segment> {
        self.segments.iter()
    }

    pub(crate) fn children(&'a self) -> impl Iterator<Item = PartitionRef<'a, L::LevelDown>> + 'a {
        (0..self.num_children).map(|idx| self.load_child(idx))
    }
}

impl<'a, L: Level> PartitionRead<L> for TreeRef<'a, L> {
    fn cardinality(&self) -> usize {
        if self.num_children == 0 {
            0
        } else {
            // Add 1 since we store cumulative - 1 to avoid overflow
            let encoded: usize = self.cumulative_cardinalities[self.num_children - 1]
                .into()
                .as_();
            encoded + 1
        }
    }

    fn is_empty(&self) -> bool {
        self.num_children == 0
    }

    fn contains(&self, value: L::Value) -> bool {
        let (segment, value) = value.split();
        if let Some(child) = self.load_child_at_segment(segment) {
            child.contains(value)
        } else {
            false
        }
    }

    fn position(&self, value: L::Value) -> Option<usize> {
        let (segment, value) = value.split();

        // First find the index of the segment and check if value is in the child
        let idx = self.segments.position(segment)?;
        let child = self.load_child(idx);
        let child_pos = child.position(value)?;

        // O(1) prefix cardinality lookup using cumulative cardinalities
        Some(self.prefix_cardinality(idx) + child_pos)
    }

    fn rank(&self, value: L::Value) -> usize {
        let (segment, value) = value.split();
        match self.segments.position(segment) {
            Some(idx) => {
                // Segment exists: O(1) prefix cardinality + rank within child
                let child = self.load_child(idx);
                self.prefix_cardinality(idx) + child.rank(value)
            }
            None => {
                // Segment doesn't exist: return cardinality of all segments < target
                let count_less = self.segments.rank(segment);
                self.prefix_cardinality(count_less)
            }
        }
    }

    fn select(&self, n: usize) -> Option<L::Value> {
        if n >= self.cardinality() {
            return None;
        }

        // Binary search to find the child containing position n
        // We're looking for the first index where cumulative_cardinalities[idx] > n
        // Since we store cumulative - 1, we compare encoded < n (equivalent to cumulative - 1 < n, i.e., cumulative <= n)
        let idx = self.cumulative_cardinalities.partition_point(|c| {
            let c: usize = (*c).into().as_();
            c < n
        });

        let prefix = self.prefix_cardinality(idx);
        let segment = self.segments.select(idx)?;
        let child = self.load_child(idx);
        child
            .select(n - prefix)
            .map(|v| L::Value::unsplit(segment, v))
    }

    fn last(&self) -> Option<L::Value> {
        if self.num_children > 0 {
            let segment = self.segments.last().unwrap();
            let child = self.load_child(self.num_children - 1);
            child.last().map(|v| L::Value::unsplit(segment, v))
        } else {
            None
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        self.segments
            .iter()
            .enumerate()
            .flat_map(|(idx, segment)| {
                let iter = self.load_child(idx).into_iter();
                iter.map(move |v| L::Value::unsplit(segment, v))
            })
            .with_size_hint(self.cardinality())
    }

    fn contains_all<R: std::ops::RangeBounds<L::Value>>(&self, values: R) -> bool {
        if let Some(values) = values.try_into_inclusive() {
            let p1 = (*values.start()).segment_end().min(*values.end());
            let p2 = (*values.end()).segment_start().max(*values.start());
            let segments = values.start().segment()..=values.end().segment();

            for segment in segments.clone() {
                let child = match self.load_child_at_segment(segment) {
                    Some(child) => child,
                    None => return false, // missing segment
                };

                // Check the appropriate range for this segment
                let child_contains_all = if segment == *segments.start() {
                    // First segment
                    child.contains_all(values.start().rest()..=p1.rest())
                } else if segment == *segments.end() {
                    // Last segment
                    child.contains_all(p2.rest()..=values.end().rest())
                } else {
                    // Middle segments must be full
                    child.cardinality() == L::LevelDown::MAX_LEN
                };

                if !child_contains_all {
                    return false;
                }
            }
            true
        } else {
            // empty range is trivially contained
            true
        }
    }

    fn contains_any<R: std::ops::RangeBounds<L::Value>>(&self, values: R) -> bool {
        if let Some(values) = values.try_into_inclusive() {
            let p1 = (*values.start()).segment_end().min(*values.end());
            let p2 = (*values.end()).segment_start().max(*values.start());
            let segments = values.start().segment()..=values.end().segment();

            for segment in segments.clone() {
                if let Some(child) = self.load_child_at_segment(segment) {
                    // Check the appropriate range for this segment
                    let has_any = if segment == *segments.start() {
                        // First segment
                        child.contains_any(values.start().rest()..=p1.rest())
                    } else if segment == *segments.end() {
                        // Last segment
                        child.contains_any(p2.rest()..=values.end().rest())
                    } else {
                        // Middle segment - any value would be in range
                        !child.is_empty()
                    };

                    if has_any {
                        return true;
                    }
                }
            }
            false
        } else {
            // empty range has no intersection
            false
        }
    }
}

impl<'a, L: Level + 'a> IntoIterator for TreeRef<'a, L> {
    type Item = L::Value;

    type IntoIter = Box<dyn Iterator<Item = L::Value> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        let cardinality = self.cardinality();
        Box::new(
            self.segments
                .clone()
                .into_iter()
                .enumerate()
                .flat_map(move |(idx, segment)| {
                    let iter = self.load_child(idx).into_iter();
                    iter.map(move |v| L::Value::unsplit(segment, v))
                })
                .with_size_hint(cardinality),
        )
    }
}

impl<'a, L: Level> PartialEq for TreeRef<'a, L> {
    fn eq(&self, other: &Self) -> bool {
        if self.num_children != other.num_children || self.segments != other.segments {
            return false;
        }
        itertools::equal(self.children(), other.children())
    }
}

pub struct TreeIndexBuilder<L: Level> {
    segments: Partition<Block>,
    offsets: Vec<usize>,
    cumulative_cardinalities: Vec<usize>,
    _marker: PhantomData<L>,
}

impl<L: Level> TreeIndexBuilder<L> {
    pub fn new(num_children: usize) -> Self {
        let (_, segments) = Self::pick_segments_store(num_children);
        Self {
            segments: segments.build(),
            offsets: Vec::with_capacity(num_children),
            cumulative_cardinalities: Vec::with_capacity(num_children),
            _marker: PhantomData,
        }
    }

    pub const fn encoded_size(num_children: usize) -> usize {
        let (segments_size, _) = Self::pick_segments_store(num_children);
        let offsets_size = Self::offsets_size(num_children);
        let cardinalities_size = Self::cardinalities_size(num_children);
        // offsets + cardinalities + segments + num_children
        offsets_size + cardinalities_size + segments_size + 1
    }

    const fn offsets_size(num_children: usize) -> usize {
        num_children * size_of::<L::ValueUnaligned>()
    }

    const fn cardinalities_size(num_children: usize) -> usize {
        num_children * size_of::<L::ValueUnaligned>()
    }

    /// Calculate the encoded size and partition kind for the segments store
    const fn pick_segments_store(num_children: usize) -> (usize, PartitionKind) {
        if num_children == Block::MAX_LEN {
            (0, PartitionKind::Full)
        } else {
            let as_bmp = BitmapPartition::<Block>::ENCODED_SIZE;
            if num_children <= as_bmp {
                (num_children, PartitionKind::Vec)
            } else {
                (as_bmp, PartitionKind::Bitmap)
            }
        }
    }

    pub fn push(&mut self, segment: Segment, offset: usize, cardinality: usize) {
        debug_assert_ne!(
            cardinality, 0,
            "BUG: tree children must have cardinality > 0"
        );
        self.segments.insert(segment);
        self.offsets.push(offset);
        let prev = self.cumulative_cardinalities.last().copied().unwrap_or(0);
        self.cumulative_cardinalities.push(prev + cardinality);
    }

    /// Consumes the builder and returns the components needed for encoding.
    ///
    /// Returns a tuple of:
    /// - `num_children`: The number of child partitions
    /// - `segments`: The partition storing which segments have children
    /// - `offsets`: Iterator of relative offsets (from end of children data)
    /// - `cardinalities`: Iterator of cumulative cardinalities minus 1 (to avoid overflow)
    pub fn build(
        self,
    ) -> (
        usize,
        Partition<Block>,
        impl Iterator<Item = L::Value>,
        impl Iterator<Item = L::Value>,
    ) {
        let num_children = self.offsets.len();
        assert_ne!(num_children, 0, "BUG: tree index builder with 0 children");
        let last_offset = self
            .offsets
            .last()
            .copied()
            .expect("BUG: offsets must be non-empty if num_children is not zero");
        let offsets = self.offsets.into_iter().map(move |offset| {
            let relative = last_offset - offset;
            L::Value::truncate_from(relative)
        });
        // Store cumulative cardinalities - 1 to avoid overflow when cardinality == L::MAX_LEN
        // (similar to how lengths are stored as len - 1)
        let cardinalities = self
            .cumulative_cardinalities
            .into_iter()
            .map(|c| L::Value::truncate_from(c - 1));
        (num_children, self.segments, offsets, cardinalities)
    }
}
