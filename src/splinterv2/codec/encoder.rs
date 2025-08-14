use std::ops::RangeInclusive;

use bitvec::{boxed::BitBox, order::Lsb0};
use bytes::BufMut;
use zerocopy::{IntoBytes, transmute_ref};

use crate::splinterv2::{
    Partition, PartitionRead,
    codec::{partition_ref::EncodedRun, tree_ref::TreeIndexBuilder},
    level::{Block, Level},
    partition::PartitionKind,
    traits::TruncateFrom,
};

pub struct Encoder<B: BufMut> {
    buf: B,
    start_offset: usize,
}

impl<B: BufMut> Encoder<B> {
    pub fn new(buf: B) -> Self {
        let start_offset = buf.remaining_mut();
        Self { buf, start_offset }
    }

    /// Retrieve the wrapped `BufMut` from the `Encoder`
    pub fn into_inner(self) -> B {
        self.buf
    }

    /// The total number of bytes written to the buffer since this Encoder was
    /// initialized.
    pub fn bytes_written(&self) -> usize {
        self.start_offset - self.buf.remaining_mut()
    }

    /// Encode a completely Empty partition into the buffer.
    pub fn put_empty_partition(&mut self) {
        self.buf.put_u8(PartitionKind::Empty as u8);
    }

    /// Encode a completely Full partition into the buffer.
    pub fn put_full_partition(&mut self) {
        self.buf.put_u8(PartitionKind::Full as u8);
    }

    /// Encode a Bitmap partition into the buffer.
    pub fn put_bitmap_partition(&mut self, bitmap: &BitBox<u64, Lsb0>) {
        self.put_bitmap_raw(bitmap);
        self.buf.put_u8(PartitionKind::Bitmap as u8);
    }

    /// Encode a Vec partition into the buffer.
    pub fn put_vec_partition<L: Level>(&mut self, values: &[L::Value]) {
        self.put_iter::<L>(values.iter().copied());
        self.put_length::<L>(values.len());
        self.buf.put_u8(PartitionKind::Vec as u8);
    }

    /// Encode a Run partition into the buffer.
    pub fn put_run_partition<'a, L: Level>(
        &mut self,
        runs: impl Iterator<Item = &'a RangeInclusive<L::Value>>,
    ) {
        let mut num_runs = 0;
        for run in runs {
            let run: EncodedRun<L> = run.into();
            self.buf.put_slice(run.as_bytes());
            num_runs += 1;
        }
        self.put_length::<L>(num_runs);
        self.buf.put_u8(PartitionKind::Run as u8);
    }

    /// Encode a Tree partition into the buffer.
    pub fn put_tree_index<L: Level>(&mut self, tree_index_builder: TreeIndexBuilder<L>) {
        let (num_children, segments, offsets) = tree_index_builder.build();
        assert!(num_children <= Block::MAX_LEN, "num_children out of range");

        self.put_iter::<L>(offsets);

        match segments {
            Partition::Full => {}
            Partition::Bitmap(p) => self.put_bitmap_raw(p.as_bitbox()),
            Partition::Vec(p) => self.put_iter::<Block>(p.iter()),
            Partition::Run(_) | Partition::Tree(_) => unreachable!(),
        }

        self.put_length::<Block>(num_children);
        self.buf.put_u8(PartitionKind::Tree as u8);
    }

    #[inline]
    fn put_length<L: Level>(&mut self, len: usize) {
        assert_ne!(len, 0, "Length must be greater than zero");
        // serialize lengths to len-1 to ensure that they fit in the storage type
        self.put_value::<L>(L::Value::truncate_from(len - 1));
    }

    #[inline]
    fn put_value<L: Level>(&mut self, v: L::Value) {
        self.buf.put_slice(L::ValueUnaligned::from(v).as_bytes());
    }

    fn put_bitmap_raw(&mut self, bitmap: &BitBox<u64, Lsb0>) {
        let raw = bitmap.as_raw_slice();
        static_assertions::assert_cfg!(target_endian = "little");
        let raw: &[zerocopy::U64<zerocopy::LittleEndian>] = transmute_ref!(raw);
        self.buf.put_slice(raw.as_bytes());
    }

    pub fn put_iter<L: Level>(&mut self, values: impl Iterator<Item = L::Value>) {
        for value in values {
            self.put_value::<L>(value);
        }
    }
}
