use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;
use zerocopy::{ConvertError, SizeError};

use crate::codec::encoder::Encoder;

pub mod encoder;
pub mod footer;
pub mod partition_ref;
pub mod runs_ref;
pub mod tree_ref;

pub trait Encodable {
    fn encoded_size(&self) -> usize;

    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>);

    fn encode_to_bytes(&self) -> Bytes {
        let size = self.encoded_size();
        let mut encoder = Encoder::new(BytesMut::with_capacity(size));
        self.encode(&mut encoder);
        encoder.into_inner().freeze()
    }
}

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("not enough bytes")]
    Length,

    #[error("invalid encoding")]
    Validity,

    #[error("unknown magic value")]
    Magic,

    #[error("invalid checksum")]
    Checksum,

    #[error("buffer contains serialized Splinter V1, decode using splinter-rs:v0.3.3")]
    SplinterV1,
}

impl DecodeErr {
    #[inline]
    fn ensure_bytes_available(data: &[u8], len: usize) -> Result<(), DecodeErr> {
        if data.len() < len {
            Err(Self::Length)
        } else {
            Ok(())
        }
    }
}

impl<S, D> From<SizeError<S, D>> for DecodeErr {
    fn from(_: SizeError<S, D>) -> Self {
        DecodeErr::Length
    }
}

impl<A, S, V> From<ConvertError<A, S, V>> for DecodeErr {
    fn from(err: ConvertError<A, S, V>) -> Self {
        match err {
            ConvertError::Alignment(_) => panic!("All zerocopy transmutations must be unaligned"),
            ConvertError::Size(_) => DecodeErr::Length,
            ConvertError::Validity(_) => DecodeErr::Validity,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use itertools::Itertools;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use crate::{
        Encodable, Splinter, SplinterRef,
        codec::{
            DecodeErr,
            footer::{Footer, SPLINTER_MAGIC},
            partition_ref::PartitionRef,
        },
        level::{Block, Level, Low},
        partition::PartitionKind,
        testutil::{
            LevelSetGen, mkpartition, mkpartition_buf, mksplinter_buf, mksplinter_manual,
            test_partition_read,
        },
        traits::{Optimizable, TruncateFrom},
    };

    #[test]
    fn test_encode_decode_direct() {
        let mut setgen = LevelSetGen::<Low>::new(0xDEADBEEF);
        let kinds = [
            PartitionKind::Bitmap,
            PartitionKind::Vec,
            PartitionKind::Run,
            PartitionKind::Tree,
        ];
        let sets = &[
            vec![0],
            vec![0, 1],
            vec![0, u16::MAX],
            vec![u16::MAX],
            setgen.random(8),
            setgen.random(4096),
            setgen.runs(4096, 0.01),
            setgen.runs(4096, 0.2),
            setgen.runs(4096, 0.5),
            setgen.runs(4096, 0.9),
            (0..Low::MAX_LEN)
                .map(|v| <Low as Level>::Value::truncate_from(v))
                .collect_vec(),
        ];

        for kind in kinds {
            for (i, set) in sets.iter().enumerate() {
                println!("Testing partition kind: {kind:?} with set {i}");

                let partition = mkpartition::<Low>(kind, &set);
                let buf = partition.encode_to_bytes();
                assert_eq!(
                    partition.encoded_size(),
                    buf.len(),
                    "encoded_size doesn't match actual size"
                );

                let partition_ref = PartitionRef::<'_, Low>::from_suffix(&buf).unwrap();

                assert_eq!(partition_ref.kind(), kind);
                test_partition_read(&partition_ref, &set);
            }
        }
    }

    #[quickcheck]
    fn test_encode_decode_quickcheck(values: Vec<u32>) -> TestResult {
        let expected = values.iter().copied().sorted().dedup().collect_vec();
        let mut splinter = Splinter::from_iter(values);
        splinter.optimize();
        let buf = splinter.encode_to_bytes();
        assert_eq!(
            buf.len(),
            splinter.encoded_size(),
            "encoded_size doesn't match actual size"
        );
        let splinter_ref = SplinterRef::from_bytes(buf).unwrap();

        test_partition_read(&splinter_ref, &expected);

        TestResult::passed()
    }

    #[test]
    fn test_length_corruption() {
        for i in 0..Footer::SIZE {
            let truncated = [0].repeat(i);
            assert_matches!(
                SplinterRef::from_bytes(truncated),
                Err(DecodeErr::Length),
                "Failed for truncated buffer of size {}",
                i
            );
        }
    }

    #[test]
    fn test_corrupted_root_partition_kind() {
        let mut buf = mksplinter_buf(&[1, 2, 3]);

        // Buffer with just footer size but corrupted partition kind
        let footer_offset = buf.len() - Footer::SIZE;
        let partitions = &mut buf[0..footer_offset];
        partitions[partitions.len() - 1] = 10;
        let corrupted = mksplinter_manual(partitions);

        assert_matches!(SplinterRef::from_bytes(corrupted), Err(DecodeErr::Validity));
    }

    #[test]
    fn test_corrupted_magic() {
        let mut buf = mksplinter_buf(&[1, 2, 3]);

        let magic_offset = buf.len() - SPLINTER_MAGIC.len();
        buf[magic_offset..].copy_from_slice(&[0].repeat(4));

        assert_matches!(SplinterRef::from_bytes(buf), Err(DecodeErr::Magic));
    }

    #[test]
    fn test_corrupted_data() {
        let mut buf = mksplinter_buf(&[1, 2, 3]);
        buf[0] = 123;
        assert_matches!(SplinterRef::from_bytes(buf), Err(DecodeErr::Checksum));
    }

    #[test]
    fn test_corrupted_checksum() {
        let mut buf = mksplinter_buf(&[1, 2, 3]);
        let checksum_offset = buf.len() - Footer::SIZE;
        buf[checksum_offset] = 123;
        assert_matches!(SplinterRef::from_bytes(buf), Err(DecodeErr::Checksum));
    }

    #[test]
    fn test_corrupted_vec_partition() {
        let mut buf = mkpartition_buf::<Block>(PartitionKind::Vec, &[1, 2, 3]);

        //                            1     2     3   len  kind
        assert_eq!(buf.as_ref(), &[0x01, 0x02, 0x03, 0x02, 0x03]);

        // corrupt the length
        buf[3] = 5;

        assert_matches!(
            PartitionRef::<Block>::from_suffix(&buf),
            Err(DecodeErr::Length)
        );
    }

    #[test]
    fn test_corrupted_run_partition() {
        let mut buf = mkpartition_buf::<Block>(PartitionKind::Run, &[1, 2, 3]);

        //                            1     3   len  kind
        assert_eq!(buf.as_ref(), &[0x01, 0x03, 0x00, 0x04]);

        // corrupt the length
        buf[2] = 5;

        assert_matches!(
            PartitionRef::<Block>::from_suffix(&buf),
            Err(DecodeErr::Length)
        );
    }

    #[test]
    fn test_corrupted_tree_partition() {
        let mut buf = mkpartition_buf::<Low>(PartitionKind::Tree, &[1, 2]);

        assert_eq!(
            buf.as_ref(),
            &[
                // Vec partition
                // 1     2   len  kind
                0x01, 0x02, 0x01, 0x03,
                // Tree partition
                // offsets (u16), segments, len, kind
                0x00, 0x00, 0x00, 0x00, 0x05
            ]
        );

        // corrupt the tree len
        buf[7] = 5;

        assert_matches!(
            PartitionRef::<Block>::from_suffix(&buf),
            Err(DecodeErr::Length)
        );
    }

    #[test]
    fn test_vec_byteorder() {
        let buf = mkpartition_buf::<Low>(PartitionKind::Vec, &[0x01_00, 0x02_00]);
        assert_eq!(
            buf.as_ref(),
            &[
                0x01, 0x00, // first value
                0x02, 0x00, // second value
                0x00, 0x01, // length
                0x03, // kind
            ]
        );
    }

    #[test]
    fn test_run_byteorder() {
        let buf = mkpartition_buf::<Low>(PartitionKind::Run, &[0x01_00, 0x02_00]);
        assert_eq!(
            buf.as_ref(),
            &[
                0x01, 0x00, 0x01, 0x00, // first run
                0x02, 0x00, 0x02, 0x00, // second run
                0x00, 0x01, // length
                0x04, // kind
            ]
        );
    }

    #[test]
    fn test_detect_splinter_v1() {
        let empty_splinter_v1 = b"\xda\xae\x12\xdf\0\0\0\0";
        assert_matches!(
            SplinterRef::from_bytes(empty_splinter_v1.as_slice()),
            Err(DecodeErr::SplinterV1)
        );
    }
}
