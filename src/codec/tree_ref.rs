use itertools::{FoldWhile, Itertools};
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
        let offsets_size = TreeIndexBuilder::<L>::offsets_size(num_children);

        DecodeErr::ensure_bytes_available(data, segments_size + offsets_size)?;

        let segments_range = (data.len() - segments_size)..data.len();
        let offsets_range = (segments_range.start - offsets_size)..segments_range.start;
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
            children: &data[data_range],
        })
    }

    fn load_child(&self, idx: usize) -> PartitionRef<'a, L::LevelDown> {
        let relative_offset: usize = self.offsets[idx].into().as_();
        let offset = self.children.len() - relative_offset;
        PartitionRef::from_suffix(&self.children[..offset]).unwrap()
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
        self.children().map(|c| c.cardinality()).sum()
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

        // Only now calculate the prefix cardinality
        let prefix_cardinality: usize = (0..idx).map(|i| self.load_child(i).cardinality()).sum();

        Some(prefix_cardinality + child_pos)
    }

    fn rank(&self, value: L::Value) -> usize {
        let (segment, value) = value.split();
        self.segments
            .iter()
            .enumerate()
            .fold_while(0, |acc, (idx, child_segment)| {
                if child_segment < segment {
                    let child = self.load_child(idx);
                    FoldWhile::Continue(acc + child.cardinality())
                } else if child_segment == segment {
                    let child = self.load_child(idx);
                    FoldWhile::Done(acc + child.rank(value))
                } else {
                    FoldWhile::Done(acc)
                }
            })
            .into_inner()
    }

    fn select(&self, mut n: usize) -> Option<L::Value> {
        let iter = self
            .segments
            .iter()
            .enumerate()
            .map(|(idx, segment)| (segment, self.load_child(idx)));
        for (segment, child) in iter {
            let len = child.cardinality();
            if n < len {
                return child.select(n).map(|v| L::Value::unsplit(segment, v));
            }
            n -= len;
        }
        None
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
    _marker: PhantomData<L>,
}

impl<L: Level> TreeIndexBuilder<L> {
    pub fn new(num_children: usize) -> Self {
        let (_, segments) = Self::pick_segments_store(num_children);
        Self {
            segments: segments.build(),
            offsets: Vec::with_capacity(num_children),
            _marker: PhantomData,
        }
    }

    pub const fn encoded_size(num_children: usize) -> usize {
        let (segments_size, _) = Self::pick_segments_store(num_children);
        let offsets_size = Self::offsets_size(num_children);
        // offsets + segments + num_children
        offsets_size + segments_size + 1
    }

    const fn offsets_size(num_children: usize) -> usize {
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

    pub fn push(&mut self, segment: Segment, offset: usize) {
        self.segments.insert(segment);
        self.offsets.push(offset);
    }

    pub fn build(self) -> (usize, Partition<Block>, impl Iterator<Item = L::Value>) {
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
        (num_children, self.segments, offsets)
    }
}
