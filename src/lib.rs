//! Splinter is a compressed bitmap format similar to [Roaring
//! Bitmaps](https://roaringbitmap.org/), optimized specifically for small,
//! sparse sets of 32-bit unsigned integers (`u32`).
//!
//! ## Key Features:
//!
//! - **Tree-based Encoding**: Splinter encodes `u32` values into a 256-way tree
//!   structure by decomposing integers into big-endian component bytes. Nodes
//!   throughout the tree (including the root) are optimized into four different
//!   storage classes: tree, vec, bitmap, run.
//! - **Zero-copy Access**: Designed for efficient querying without
//!   deserialization, the `SplinterRef` type allows direct, zero-copy reads from
//!   any type implementing `Deref<Target = [u8]>`.

pub mod codec;
pub mod cow;
pub mod level;
pub mod splinter;
pub mod splinter_ops;
pub mod splinter_ref;
pub mod splinter_ref_ops;
pub mod traits;

#[doc(hidden)]
pub mod count;

mod never;
mod partition;
mod partition_kind;
mod partition_ops;
mod segment;
mod util;

#[doc(inline)]
pub use cow::CowSplinter;
#[doc(inline)]
pub use splinter::Splinter;
#[doc(inline)]
pub use splinter_ref::SplinterRef;

#[doc(inline)]
pub use crate::{
    codec::{DecodeErr, Encodable},
    traits::{Cut, Merge, Optimizable, PartitionRead, PartitionWrite},
};

#[cfg(any(test, feature = "testutil"))]
pub mod testutil;
