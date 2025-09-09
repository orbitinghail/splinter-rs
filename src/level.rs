use std::{
    fmt::{Debug, Display},
    ops::BitOrAssign,
};

use ::u24::U24;
use num::{
    cast::AsPrimitive,
    traits::{ConstOne, ConstZero},
};
use u24::u24;
use zerocopy::{BE, FromBytes, Immutable, IntoBytes, KnownLayout, U16, U32, Unaligned};

use crate::{
    codec::{Encodable, partition_ref::PartitionRef},
    never::Never,
    partition::Partition,
    segment::SplitSegment,
    traits::{Cut, Optimizable, PartitionRead, PartitionWrite, TruncateFrom},
};

#[doc(hidden)]
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
        + Eq
        + PartialEq
        + Cut<Out = Self::Down>
        + for<'a> BitOrAssign<&'a Self::Down>
        + for<'a> PartialEq<PartitionRef<'a, Self::LevelDown>>
        + for<'a> BitOrAssign<&'a PartitionRef<'a, Self::LevelDown>>
        + for<'a> Cut<PartitionRef<'a, Self::LevelDown>, Out = Self::Down>;

    type Value: num::PrimInt
        + AsPrimitive<usize>
        + SplitSegment<Rest = <Self::LevelDown as Level>::Value>
        + TruncateFrom<usize>
        + ConstZero
        + ConstOne
        + Debug
        + Display
        + Clone
        + range_set_blaze::Integer;

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
    const ALLOW_TREE: bool = Self::BITS > 8;
}

/// High is an internal type which is only exposed in docs due to it's usage in
/// the `PartitionRead` trait.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct High;

impl Level for High {
    const DEBUG_NAME: &'static str = "High";

    type LevelDown = Mid;
    type Down = Partition<Self::LevelDown>;
    type Value = u32;
    type ValueUnaligned = U32<BE>;

    const BITS: usize = 32;
}

#[doc(hidden)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Mid;

impl Level for Mid {
    const DEBUG_NAME: &'static str = "Mid";

    type LevelDown = Low;
    type Down = Partition<Self::LevelDown>;
    type Value = u24;
    type ValueUnaligned = U24<BE>;

    const BITS: usize = 24;
}

#[doc(hidden)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Low;

impl Level for Low {
    const DEBUG_NAME: &'static str = "Low";

    type LevelDown = Block;
    type Down = Partition<Self::LevelDown>;
    type Value = u16;
    type ValueUnaligned = U16<BE>;

    const BITS: usize = 16;
}

#[doc(hidden)]
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
