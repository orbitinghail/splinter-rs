use zerocopy::{KnownLayout, TryFromBytes};

use crate::partition::Partition;

use crate::level::Level;

/// `PartitionKind` is a one byte bitfield which currently only uses the first
/// three bits (LE). The remaining bits are reserved for future expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, TryFromBytes, KnownLayout)]
#[repr(u8)]
pub enum PartitionKind {
    #[default]
    Empty = 0b000,
    Full = 0b001,
    Bitmap = 0b010,
    Vec = 0b011,
    Run = 0b100,
    Tree = 0b101,
}

impl PartitionKind {
    pub fn build<L: Level>(self) -> Partition<L> {
        match self {
            PartitionKind::Empty => Partition::EMPTY,
            PartitionKind::Full => Partition::Full,
            PartitionKind::Bitmap => Partition::Bitmap(Default::default()),
            PartitionKind::Vec => Partition::Vec(Default::default()),
            PartitionKind::Run => Partition::Run(Default::default()),
            PartitionKind::Tree => Partition::Tree(Default::default()),
        }
    }
}
