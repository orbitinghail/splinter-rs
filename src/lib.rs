//! Splinter is a compressed bitmap format similar to [Roaring Bitmaps](https://roaringbitmap.org/), optimized specifically for small, sparse sets of 32-bit unsigned integers (`u32`).
//!
//! ## Key Features:
//!
//! - **Tree-based Encoding**: Splinter encodes `u32` values into a 256-way tree structure by decomposing integers into big-endian component bytes. Leaf nodes efficiently transition from byte lists to compact bitmaps at up to 32 values.
//!
//! - **Zero-copy Access**: Designed for efficient querying without deserialization, the `SplinterRef` type allows direct, zero-copy reads from any type implementing `AsRef<[u8]>`.

use std::ops::RangeBounds;

use thiserror::Error;

mod bitmap;
mod block;
pub mod cow;
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

pub trait SplinterRead {
    /// Returns `true` if the Splinter is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// assert!(splinter.is_empty());
    /// splinter.insert(1);
    /// assert!(!splinter.is_empty());
    /// ```
    fn is_empty(&self) -> bool;

    /// Returns `true` if the Splinter contains the given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(1);
    /// splinter.insert(3);
    ///
    /// assert!(splinter.contains(1));
    /// assert!(!splinter.contains(2));
    /// assert!(splinter.contains(3));
    /// ```
    fn contains(&self, key: u32) -> bool;

    /// Calculates the total number of values stored in the Splinter.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    ///
    /// assert_eq!(3, splinter.cardinality());
    /// ```
    fn cardinality(&self) -> usize;

    /// Returns an sorted [`Iterator`] over all keys.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    ///
    /// assert_eq!(&[1, 3, 6], &*splinter.iter().collect::<Vec<_>>());
    /// ```
    fn iter(&self) -> impl Iterator<Item = u32> + '_;

    /// Returns an sorted [`Iterator`] over all keys contained by the provided range.
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// splinter.insert(5);
    /// splinter.insert(9);
    ///
    /// assert_eq!(&[3, 5, 6], &*splinter.range(3..=6).collect::<Vec<_>>());
    /// ```
    fn range<'a, R>(&'a self, range: R) -> impl Iterator<Item = u32> + 'a
    where
        R: RangeBounds<u32> + 'a;

    /// Returns the last key in the set
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    ///
    /// assert_eq!(None, splinter.last());
    /// splinter.insert(6);
    /// splinter.insert(1);
    /// splinter.insert(3);
    /// assert_eq!(Some(6), splinter.last());
    /// ```
    fn last(&self) -> Option<u32>;
}

pub trait SplinterWrite {
    /// Attempts to insert a key into the Splinter, returning true if a key was inserted
    ///
    /// # Examples
    ///
    /// ```
    /// # use splinter_rs::{Splinter, SplinterRead, SplinterWrite};
    ///
    /// let mut splinter = Splinter::default();
    /// assert!(splinter.insert(6));
    /// assert!(!splinter.insert(6));
    ///
    /// assert_eq!(&[6], &*splinter.iter().collect::<Vec<_>>());
    /// ```
    fn insert(&mut self, key: u32) -> bool;
}
