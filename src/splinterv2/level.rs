use std::fmt::{Debug, Display};

use ::u24::U24;
use num::{
    cast::AsPrimitive,
    traits::{ConstOne, ConstZero},
};
use u24::u24;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, LE, U16, U32, Unaligned};

use crate::splinterv2::{
    codec::{Encodable, partition_ref::PartitionRef},
    never::Never,
    partition::Partition,
    segment::SplitSegment,
    traits::{Optimizable, PartitionRead, PartitionWrite, TruncateFrom},
};

pub trait Level: Sized {
    const DEBUG_NAME: &'static str;

    type LevelDown: Level;

    type Down: PartitionRead<Self::LevelDown>
        + PartitionWrite<Self::LevelDown>
        + Optimizable
        + Encodable
        + Default
        + Debug
        + Clone
        + PartialEq
        + for<'a> PartialEq<PartitionRef<'a, Self::LevelDown>>
        + Eq;

    type Value: num::PrimInt
        + AsPrimitive<usize>
        + SplitSegment<Rest = <Self::LevelDown as Level>::Value>
        + TruncateFrom<usize>
        + ConstZero
        + ConstOne
        + Debug
        + Display
        + Copy;

    type ValueUnaligned: IntoBytes
        + FromBytes
        + Unaligned
        + Immutable
        + KnownLayout
        + Into<Self::Value>
        + From<Self::Value>
        + Ord
        + Debug
        + Display
        + Copy;

    const BITS: usize;
    const MAX_LEN: usize = 1 << Self::BITS;
    const TREE_MIN: usize = 32;
    const PREFER_TREE: bool = Self::BITS > 8;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct High;

impl Level for High {
    const DEBUG_NAME: &'static str = "High";

    type LevelDown = Mid;
    type Down = Partition<Self::LevelDown>;
    type Value = u32;
    type ValueUnaligned = U32<LE>;

    const BITS: usize = 32;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Mid;

impl Level for Mid {
    const DEBUG_NAME: &'static str = "Mid";

    type LevelDown = Low;
    type Down = Partition<Self::LevelDown>;
    type Value = u24;
    type ValueUnaligned = U24<LE>;

    const BITS: usize = 24;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Low;

impl Level for Low {
    const DEBUG_NAME: &'static str = "Low";

    type LevelDown = Block;
    type Down = Partition<Self::LevelDown>;
    type Value = u16;
    type ValueUnaligned = U16<LE>;

    const BITS: usize = 16;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Block;

impl Level for Block {
    const DEBUG_NAME: &'static str = "Block";

    type LevelDown = Never;
    type Down = Never;
    type Value = u8;
    type ValueUnaligned = u8;

    const BITS: usize = 8;
}
