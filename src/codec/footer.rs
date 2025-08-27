use crc64fast_nvme::Digest;
use zerocopy::{
    ByteEq, ByteHash, FromBytes, Immutable, IntoBytes, KnownLayout, LittleEndian, U64, Unaligned,
};

use crate::codec::DecodeErr;

/// The last four bytes of an encoded Splinter
pub const SPLINTER_MAGIC: [u8; 4] = [0x59, 0x11, 0xA7, 0xE2];

#[derive(FromBytes, IntoBytes, Immutable, Unaligned, KnownLayout, ByteHash, ByteEq)]
#[repr(C)]
pub struct Footer {
    checksum: U64<LittleEndian>,
    magic: [u8; 4],
}

impl Footer {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn from_checksum(checksum: u64) -> Self {
        Self {
            checksum: checksum.into(),
            magic: SPLINTER_MAGIC,
        }
    }

    pub fn validate(&self, data: &[u8]) -> Result<(), DecodeErr> {
        if self.magic != SPLINTER_MAGIC {
            return Err(DecodeErr::Magic);
        }

        let checksum = {
            let mut c = Digest::new();
            c.write(data);
            c.sum64()
        };
        if checksum != self.checksum.get() {
            return Err(DecodeErr::Checksum);
        }

        Ok(())
    }
}
