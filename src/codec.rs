use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;
use zerocopy::{ConvertError, SizeError};

use crate::codec::encoder::Encoder;

pub mod encoder;

pub(crate) mod footer;
pub(crate) mod partition_ref;
pub(crate) mod runs_ref;
pub(crate) mod tree_ref;

/// Trait for types that can be encoded into a binary format.
pub trait Encodable {
    /// Returns the number of bytes required to encode this value.
    ///
    /// This should return the exact number of bytes that [`encode`](Self::encode)
    /// will write, allowing for efficient buffer pre-allocation.
    ///
    /// Note: This function traverses the entire datastructure which scales with cardinality.
    fn encoded_size(&self) -> usize;

    /// Encodes this value into the provided encoder.
    fn encode<B: BufMut>(&self, encoder: &mut Encoder<B>);

    /// Convenience method that encodes this value to a [`Bytes`] buffer.
    ///
    /// This is the easiest way to serialize splinter data. It allocates
    /// a buffer of the exact required size and encodes the value into it.
    ///
    /// # Examples
    ///
    /// ```
    /// use splinter_rs::{Splinter, Encodable, PartitionWrite};
    ///
    /// let splinter = Splinter::from_iter([8, 42, 16]);
    /// let bytes = splinter.encode_to_bytes();
    /// assert!(!bytes.is_empty());
    /// assert_eq!(bytes.len(), splinter.encoded_size());
    /// ```
    fn encode_to_bytes(&self) -> Bytes {
        let size = self.encoded_size();
        let mut encoder = Encoder::new(BytesMut::with_capacity(size));
        self.encode(&mut encoder);
        encoder.into_inner().freeze()
    }
}

/// Errors that can occur when deserializing splinter data from bytes.
///
/// These errors indicate various types of corruption or invalid data that can
/// be encountered when attempting to decode serialized splinter data.
#[derive(Debug, Error)]
pub enum DecodeErr {
    /// The buffer does not contain enough bytes to decode the expected data.
    ///
    /// This error occurs when the buffer is truncated or smaller than the
    /// minimum required size for a valid splinter.
    #[error("not enough bytes")]
    Length,

    /// The data contains invalid or corrupted encoding structures.
    ///
    /// This error indicates that while the buffer has sufficient length and
    /// correct magic bytes, the internal data structures are malformed or
    /// contain invalid values.
    #[error("invalid encoding")]
    Validity,

    /// The buffer does not end with the expected magic bytes.
    ///
    /// Splinter data ends with specific magic bytes to identify the format.
    /// This error indicates the buffer does not contain valid splinter data
    /// or has been corrupted at the end.
    #[error("unknown magic value")]
    Magic,

    /// The calculated checksum does not match the stored checksum.
    ///
    /// This error indicates data corruption has occurred somewhere in the
    /// buffer, as the integrity check has failed.
    #[error("invalid checksum")]
    Checksum,

    /// The buffer contains data from the incompatible Splinter V1 format.
    ///
    /// This version of splinter-rs can only decode V2 format data. To decode
    /// V1 data, use splinter-rs version 0.3.3 or earlier.
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
    #[track_caller]
    fn from(_: SizeError<S, D>) -> Self {
        DecodeErr::Length
    }
}

impl<A, S, V> From<ConvertError<A, S, V>> for DecodeErr {
    #[track_caller]
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
    use itertools::Itertools;
    use proptest::proptest;

    use crate::{
        Encodable, Splinter, SplinterRef, assert_error,
        codec::{
            DecodeErr,
            footer::{Footer, SPLINTER_V2_MAGIC},
            partition_ref::PartitionRef,
        },
        level::{Block, Level, Low},
        partition_kind::PartitionKind,
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

    proptest! {
        #[test]
        fn test_encode_decode_proptest(
            values in proptest::collection::vec(0u32..16384, 0..1024),
        ) {
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
        }
    }

    #[test]
    fn test_length_corruption() {
        for i in 0..Footer::SIZE {
            let truncated = [0].repeat(i);
            assert_error!(
                SplinterRef::from_bytes(truncated),
                DecodeErr::Length,
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

        assert_error!(SplinterRef::from_bytes(corrupted), DecodeErr::Validity);
    }

    #[test]
    fn test_corrupted_magic() {
        let mut buf = mksplinter_buf(&[1, 2, 3]);

        let magic_offset = buf.len() - SPLINTER_V2_MAGIC.len();
        buf[magic_offset..].copy_from_slice(&[0].repeat(4));

        assert_error!(SplinterRef::from_bytes(buf), DecodeErr::Magic);
    }

    #[test]
    fn test_corrupted_data() {
        let mut buf = mksplinter_buf(&[1, 2, 3]);
        buf[0] = 123;
        assert_error!(SplinterRef::from_bytes(buf), DecodeErr::Checksum);
    }

    #[test]
    fn test_corrupted_checksum() {
        let mut buf = mksplinter_buf(&[1, 2, 3]);
        let checksum_offset = buf.len() - Footer::SIZE;
        buf[checksum_offset] = 123;
        assert_error!(SplinterRef::from_bytes(buf), DecodeErr::Checksum);
    }

    #[test]
    fn test_corrupted_vec_partition() {
        let mut buf = mkpartition_buf::<Block>(PartitionKind::Vec, &[1, 2, 3]);

        //                            1     2     3   len  kind
        assert_eq!(buf.as_ref(), &[0x01, 0x02, 0x03, 0x02, 0x03]);

        // corrupt the length
        buf[3] = 5;

        assert_error!(PartitionRef::<Block>::from_suffix(&buf), DecodeErr::Length);
    }

    #[test]
    fn test_corrupted_run_partition() {
        let mut buf = mkpartition_buf::<Block>(PartitionKind::Run, &[1, 2, 3]);

        //                            1     3   len  kind
        assert_eq!(buf.as_ref(), &[0x01, 0x03, 0x00, 0x04]);

        // corrupt the length
        buf[2] = 5;

        assert_error!(PartitionRef::<Block>::from_suffix(&buf), DecodeErr::Length);
    }

    #[test]
    fn test_corrupted_tree_partition() {
        let mut buf = mkpartition_buf::<Low>(PartitionKind::Tree, &[1, 2]);

        assert_eq!(
            buf.as_ref(),
            &[
                // Vec partition (child)
                // 1     2   len  kind
                0x01, 0x02, 0x01, 0x03,
                // Tree partition
                // offsets (u16), cumulative_cardinalities-1 (u16), segments, len, kind
                0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x05
            ]
        );

        // corrupt the tree len (now at position 9 due to added cardinalities)
        buf[9] = 5;

        assert_error!(PartitionRef::<Block>::from_suffix(&buf), DecodeErr::Length);
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
        assert_error!(
            SplinterRef::from_bytes(empty_splinter_v1.as_slice()),
            DecodeErr::SplinterV1
        );
    }
}
