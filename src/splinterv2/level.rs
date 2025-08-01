use std::fmt::Debug;

use num::cast::AsPrimitive;
use zerocopy::{LE, U16, U32};

use crate::{
    splinterv2::{
        encode::Encodable,
        never::Never,
        partition::Partition,
        segment::SplitSegment,
        traits::{PartitionRead, PartitionWrite, TruncateFrom},
    },
    u24::u24,
};

pub trait Level {
    const DEBUG_NAME: &'static str;

    type Offset;

    type LevelDown: Level;

    type Down: PartitionRead<Self::LevelDown>
        + PartitionWrite<Self::LevelDown>
        + Default
        + Debug
        + Clone
        + Encodable;

    type Value: num::PrimInt
        + AsPrimitive<usize>
        + SplitSegment<Rest = <Self::LevelDown as Level>::Value>
        + TruncateFrom<usize>
        + Debug;

    const BITS: usize;
    const MAX_LEN: usize = 1 << Self::BITS;
    const VEC_LIMIT: usize = (Self::MAX_LEN) / Self::BITS;
    const TREE_MIN: usize = 32;
    const PREFER_TREE: bool = Self::BITS > 8;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct High;

impl Level for High {
    const DEBUG_NAME: &'static str = "High";

    type Offset = U32<LE>;
    type LevelDown = Mid;
    type Down = Partition<Self::LevelDown>;
    type Value = u32;
    const BITS: usize = 32;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Mid;

impl Level for Mid {
    const DEBUG_NAME: &'static str = "Mid";

    type Offset = U32<LE>;
    type LevelDown = Low;
    type Down = Partition<Self::LevelDown>;
    type Value = u24;
    const BITS: usize = 24;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Low;

impl Level for Low {
    const DEBUG_NAME: &'static str = "Low";

    type Offset = U16<LE>;
    type LevelDown = Block;
    type Down = Partition<Self::LevelDown>;
    type Value = u16;
    const BITS: usize = 16;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Block;

impl Level for Block {
    const DEBUG_NAME: &'static str = "Block";

    type Offset = ();
    type LevelDown = Never;
    type Down = Never;
    type Value = u8;
    const BITS: usize = 8;
}
