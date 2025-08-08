use num::traits::AsPrimitive;
use std::marker::PhantomData;

use crate::splinterv2::{
    Partition, PartitionRead, PartitionWrite,
    codec::{
        DecodeErr,
        partition_ref::{NonRecursivePartitionRef, PartitionRef, decode_len},
    },
    level::{Block, Level},
    partition::{PartitionKind, bitmap::BitmapPartition, vec::VecPartition},
    segment::{Segment, SplitSegment},
    traits::TruncateFrom,
};

#[derive(Debug)]
pub struct TreeRef<'a, L: Level> {
    num_children: usize,
    segments: NonRecursivePartitionRef<'a, Block>,
    offsets: &'a [L::ValueUnaligned],
    children: &'a [u8],
}

impl<'a, L: Level> TreeRef<'a, L> {
    pub(super) fn from_suffix(data: &'a [u8]) -> Result<Self, DecodeErr> {
        let (data, num_children) = decode_len::<Block>(data)?;

        let (segments_size, segments_kind) =
            TreeIndexBuilder::<L>::pick_segments_store(num_children);
        let offsets_size = TreeIndexBuilder::<L>::offsets_size(num_children);

        DecodeErr::ensure_length_available(data, segments_size)?;
        DecodeErr::ensure_length_available(data, offsets_size)?;

        let segments_range = (data.len() - segments_size)..data.len();
        let offsets_range = (segments_range.start - offsets_size)..segments_range.start;
        let data_range = 0..offsets_range.start;

        Ok(Self {
            num_children,
            segments: NonRecursivePartitionRef::from_suffix_with_kind(
                segments_kind,
                &data[segments_range],
            )?,
            offsets: zerocopy::transmute_ref!(&data[offsets_range]),
            children: &data[data_range],
        })
    }

    fn load_child(&self, idx: usize) -> PartitionRef<'a, L::LevelDown> {
        let relative_offset: usize = self.offsets[idx].into().as_();
        let offset = self.children.len() - relative_offset;
        PartitionRef::from_suffix(&self.children[..offset]).unwrap()
    }

    fn children(&'a self) -> impl Iterator<Item = PartitionRef<'a, L::LevelDown>> + 'a {
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

    fn contains(&self, value: <L as Level>::Value) -> bool {
        let (segment, value) = value.split();
        if self.segments.contains(segment) {
            let rank = self.segments.rank(segment);
            self.load_child(rank).contains(value)
        } else {
            false
        }
    }

    fn iter(&self) -> impl Iterator<Item = L::Value> {
        /*
        Checkpoint!

        Ok so the issue with returning an iterator here is the need to hold
        onto a ref to the child as well as a ref to the child's iterator state.
        However, since child.iter() returns impl Iterator, constructing a
        concrete Iter here is tricky.

        Some ideas:
        1. just try harder! maybe after a break I'll figure out a way to express this
        2. use Box<dyn Iterator>, or some similar dyn reference.
            -> issue with this is a lot of nested allocations and vtable chasing
            when iterating a full tree. it should be possible to get this all on
            the stack...
        3. switch from iter to a visitor pattern

        Keep in mind that we also need to eventually implement iter_range as
        well as the various bitwise ops like union, cut, etc.
        */

        todo!();
        std::iter::empty()
    }

    fn rank(&self, value: <L as Level>::Value) -> usize {
        todo!()
    }

    fn select(&self, idx: usize) -> Option<L::Value> {
        todo!()
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
        // +1 for num_children
        1 + segments_size + offsets_size
    }

    const fn offsets_size(num_children: usize) -> usize {
        num_children * std::mem::size_of::<L::ValueUnaligned>()
    }

    /// Calculate the encoded size and partition kind for the segments store
    const fn pick_segments_store(num_children: usize) -> (usize, PartitionKind) {
        if num_children == Block::MAX_LEN {
            (0, PartitionKind::Full)
        } else {
            let as_vec = VecPartition::<Block>::encoded_size(num_children);
            let as_bmp = BitmapPartition::<Block>::ENCODED_SIZE;
            if as_vec <= as_bmp {
                (as_vec, PartitionKind::Vec)
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

// struct Iter<'a, L: Level> {
//     inner: &'a TreeRef<'a, L>,
//     cursor: usize,
//     child: Option<(Segment, PartitionRef<'a, L::LevelDown>)>,
// }

// impl<'a, L> Iterator for Iter<'a, L>
// where
//     L: Level,
//     I: Iterator<Item = <L::LevelDown as Level>::Value>,
// {
//     type Item = L::Value;

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.cursor >= self.inner.num_children {
//             return None;
//         }

//         if let Some((segment, child)) = &mut self.child {
//             if let Some(next) = child.next() {
//                 return Some(L::Value::unsplit(*segment, next));
//             }
//         }

//         // get next child
//         let segment = self.inner.segments.select(self.cursor).unwrap();
//         let child = self.inner.load_child(self.cursor);
//         self.cursor += 1;
//         let next = child.next().map(|n| L::Value::unsplit(segment, n));
//         self.child = Some((segment, child));
//         next
//     }
// }
