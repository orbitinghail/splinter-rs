//! Splinter is a compressed bitmap format similar to [Roaring Bitmaps](https://roaringbitmap.org/), optimized specifically for small, sparse sets of 32-bit unsigned integers (`u32`).
//!
//! ## Key Features:
//!
//! - **Tree-based Encoding**: Splinter encodes `u32` values into a 256-way tree structure by decomposing integers into big-endian component bytes. Leaf nodes efficiently transition from byte lists to compact bitmaps at up to 32 values.
//!
//! - **Zero-copy Access**: Designed for efficient querying without deserialization, the `SplinterRef` type allows direct, zero-copy reads from any type implementing `AsRef<[u8]>`.

use thiserror::Error;

mod bitmap;
mod block;
mod index;
pub mod ops;
mod partition;
mod relational;
mod splinter;
mod util;

#[cfg(test)]
mod testutil;

pub use splinter::{SPLINTER_MAX_VALUE, Splinter, SplinterRef};

type Segment = u8;

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("Unable to decode header")]
    InvalidHeader,

    #[error("Invalid magic number")]
    InvalidMagic,

    #[error("Unable to decode footer")]
    InvalidFooter,
}
