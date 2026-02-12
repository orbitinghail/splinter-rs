use std::ops::RangeInclusive;

use bitvec::{boxed::BitBox, order::Lsb0};
use bytes::BufMut;
use crc64fast_nvme::Digest;
use zerocopy::{IntoBytes, transmute_ref};

use crate::{
    PartitionRead,
    codec::{footer::Footer, runs_ref::EncodedRun, tree_ref::TreeIndexBuilder},
    level::{Block, Level},
    partition::Partition,
    partition_kind::PartitionKind,
    traits::TruncateFrom,
};

pub struct Encoder<B: BufMut> {
    buf: B,
    bytes_written: usize,
    checksum: Digest,
    wrote_footer: bool,
}

impl<B: BufMut> Encoder<B> {
    pub fn new(buf: B) -> Self {
        Self {
            buf,
            bytes_written: 0,
            checksum: Digest::new(),
            wrote_footer: false,
        }
    }

    /// Retrieve the wrapped buffer from the `Encoder`
    pub fn into_inner(self) -> B {
        self.buf
    }

    /// Write an entire encoded splinter to the buffer
    pub(crate) fn write_splinter(&mut self, splinter: &[u8]) {
        self.buf.put_slice(splinter);
        self.bytes_written += splinter.len();
        // assuming the splinter is valid, it already has a footer
        self.wrote_footer = true;
    }

    /// Write the checksum and Splinter Magic value to the buffer
    pub(crate) fn write_footer(&mut self) {
        assert!(
            !self.wrote_footer,
            "invalid encoder usage: footer already present"
        );
        self.wrote_footer = true;
        let footer = Footer::from_checksum(self.checksum.sum64());
        self.put_slice(footer.as_bytes());
    }

    /// The total number of bytes written to the buffer since this Encoder was
    /// initialized.
    pub(crate) fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Encode a Bitmap partition into the buffer.
    pub(crate) fn put_bitmap_partition(&mut self, bitmap: &BitBox<u64, Lsb0>) {
        self.put_bitmap_raw(bitmap);
    }

    /// Encode a Vec partition into the buffer.
    pub(crate) fn put_vec_partition<L: Level>(&mut self, values: &[L::Value]) {
        self.put_iter::<L>(values.iter().copied());
        self.put_length::<L>(values.len());
    }

    /// Encode a Run partition into the buffer.
    pub(crate) fn put_run_partition<L: Level>(
        &mut self,
        runs: impl Iterator<Item = RangeInclusive<L::Value>>,
    ) {
        let mut num_runs = 0;
        for run in runs {
            let run: EncodedRun<L> = run.into();
            self.put_slice(run.as_bytes());
            num_runs += 1;
        }
        self.put_length::<L>(num_runs);
    }

    /// Encode a Tree partition into the buffer.
    pub(crate) fn put_tree_index<L: Level>(&mut self, tree_index_builder: TreeIndexBuilder<L>) {
        let (num_children, segments, offsets, cardinalities) = tree_index_builder.build();
        assert!(
            num_children > 0 && num_children <= Block::MAX_LEN,
            "num_children out of range"
        );

        // Encoding order: [offsets][cardinalities][segments][num_children]
        self.put_iter::<L>(offsets);
        self.put_iter::<L>(cardinalities);

        match segments {
            Partition::Full => {}
            Partition::Bitmap(p) => self.put_bitmap_raw(p.as_bitbox()),
            Partition::Vec(p) => self.put_iter::<Block>(p.iter()),
            Partition::Run(_) | Partition::Tree(_) => unreachable!(),
        }

        self.put_length::<Block>(num_children);
    }

    pub(crate) fn put_iter<L: Level>(&mut self, values: impl Iterator<Item = L::Value>) {
        for value in values {
            self.put_value::<L>(value);
        }
    }

    pub(crate) fn put_kind(&mut self, k: PartitionKind) {
        let d = [k as u8];
        self.put_slice(&d)
    }

    #[inline]
    fn put_length<L: Level>(&mut self, len: usize) {
        assert_ne!(len, 0, "Length must be greater than zero");
        // serialize lengths to len-1 to ensure that they fit in the storage type
        self.put_value::<L>(L::Value::truncate_from(len - 1));
    }

    #[inline]
    fn put_value<L: Level>(&mut self, v: L::Value) {
        self.put_slice(L::ValueUnaligned::from(v).as_bytes());
    }

    fn put_bitmap_raw(&mut self, bitmap: &BitBox<u64, Lsb0>) {
        let raw = bitmap.as_raw_slice();
        static_assertions::assert_cfg!(target_endian = "little");
        let raw: &[zerocopy::U64<zerocopy::LittleEndian>] = transmute_ref!(raw);
        self.put_slice(raw.as_bytes());
    }

    fn put_slice(&mut self, data: &[u8]) {
        self.checksum.write(data);
        self.buf.put_slice(data);
        self.bytes_written += data.len();
    }
}
