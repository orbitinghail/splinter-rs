use std::ops::RangeInclusive;

use bitvec::{boxed::BitBox, order::Lsb0};
use bytes::BufMut;
use zerocopy::{IntoBytes, transmute_ref};

use crate::splinterv2::{
    Partition,
    codec::container::{ContainerKind, EncodedRun},
    level::{Block, Level},
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

    /// Encode a completely Full container into the buffer.
    pub fn put_full_container(&mut self) {
        self.buf.put_u8(ContainerKind::Full as u8);
    }

    /// Encode a Bitmap container into the buffer.
    pub fn put_bitmap_container(&mut self, bitmap: &BitBox<u64, Lsb0>) {
        self.put_bitmap_raw(bitmap);
        self.buf.put_u8(ContainerKind::Bitmap as u8);
    }

    /// Encode a Vec container into the buffer.
    pub fn put_vec_container<L: Level>(&mut self, values: &[L::Value]) {
        self.put_vec_raw::<L>(values);
        self.put_length::<L>(values.len());
        self.buf.put_u8(ContainerKind::Vec as u8);
    }

    /// Encode a Run container into the buffer.
    pub fn put_run_container<'a, L: Level>(
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
        self.buf.put_u8(ContainerKind::Run as u8);
    }

    /// Encode a Tree container into the buffer.
    pub fn put_tree_container<L: Level>(
        &mut self,
        segments: Partition<Block>,
        offsets: Vec<L::ValueUnaligned>,
    ) {
        match segments {
            Partition::Full => {}
            Partition::Bitmap(p) => self.put_bitmap_raw(p.as_bitbox()),
            Partition::Vec(p) => self.put_vec_raw::<Block>(p.values()),
            Partition::Run(_) | Partition::Tree(_) => unreachable!(),
        }
        self.buf.put_slice(offsets.as_bytes());
        self.put_length::<L>(offsets.len());
        self.buf.put_u8(ContainerKind::Tree as u8);
    }

    #[inline]
    fn put_length<L: Level>(&mut self, len: usize) {
        assert_ne!(len, 0, "Length must be greater than zero");
        // serialize lengths to len-1 to ensure that they fit in the storage type
        self.put_value::<L>(L::Value::truncate_from(len - 1));
    }

    #[inline]
    fn put_value<L: Level>(&mut self, len: L::Value) {
        self.buf.put_slice(L::ValueUnaligned::from(len).as_bytes());
    }

    fn put_bitmap_raw(&mut self, bitmap: &BitBox<u64, Lsb0>) {
        let raw = bitmap.as_raw_slice();
        static_assertions::assert_cfg!(target_endian = "little");
        let raw: &[zerocopy::U64<zerocopy::LittleEndian>] = transmute_ref!(raw);
        self.buf.put_slice(raw.as_bytes());
    }

    pub fn put_vec_raw<L: Level>(&mut self, values: &[L::Value]) {
        for value in values {
            self.put_value::<L>(*value);
        }
    }
}
