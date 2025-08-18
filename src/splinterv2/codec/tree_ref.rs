use itertools::{FoldWhile, Itertools};
use num::traits::AsPrimitive;
use std::{marker::PhantomData, mem::size_of};
use zerocopy::FromBytes;

use crate::splinterv2::{
    Partition, PartitionRead, PartitionWrite,
    codec::{
        DecodeErr,
        partition_ref::{NonRecursivePartitionRef, PartitionRef, decode_len_from_suffix},
    },
    level::{Block, Level},
    partition::{PartitionKind, bitmap::BitmapPartition},
    segment::{Segment, SplitSegment},
    traits::TruncateFrom,
};

#[derive(Debug, Clone)]
pub struct TreeRef<'a, L: Level> {
    num_children: usize,
    segments: NonRecursivePartitionRef<'a, Block>,
    offsets: &'a [L::ValueUnaligned],
    children: &'a [u8],
}

impl<'a, L: Level> TreeRef<'a, L> {
    pub(super) fn from_suffix(data: &'a [u8]) -> Result<Self, DecodeErr> {
        let (data, num_children) = decode_len_from_suffix::<Block>(data)?;

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
        if self.segments.contains(segment) {
            let rank = self.segments.rank(segment) - 1;
            self.load_child(rank).contains(value)
        } else {
            false
        }
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
                    FoldWhile::Continue(acc + child.rank(value))
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
        self.segments.iter().enumerate().flat_map(|(idx, segment)| {
            let iter = self.load_child(idx).into_iter();
            iter.map(move |v| L::Value::unsplit(segment, v))
        })
    }
}

impl<'a, L: Level + 'a> IntoIterator for TreeRef<'a, L> {
    type Item = L::Value;

    type IntoIter = Box<dyn Iterator<Item = L::Value> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.segments
                .clone()
                .into_iter()
                .enumerate()
                .flat_map(move |(idx, segment)| {
                    let iter = self.load_child(idx).into_iter();
                    iter.map(move |v| L::Value::unsplit(segment, v))
                }),
        )
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
        assert_ne!(num_children, 0);
        let last_offset = self.offsets.last().copied().unwrap();
        let offsets = self.offsets.into_iter().map(move |offset| {
            let relative = last_offset - offset;
            L::Value::truncate_from(relative)
        });
        (num_children, self.segments, offsets)
    }
}
