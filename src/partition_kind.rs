use zerocopy::TryFromBytes;

use crate::partition::Partition;

use crate::level::Level;

/// PartitionKind is a one byte bitfield which currently only uses the first
/// three bits (LE). The remaining bits are reserved for future expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, TryFromBytes)]
#[repr(u8)]
pub enum PartitionKind {
    #[default]
    Empty = 0b00000_000,
    Full = 0b00000_001,
    Bitmap = 0b00000_010,
    Vec = 0b00000_011,
    Run = 0b00000_100,
    Tree = 0b00000_101,
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
