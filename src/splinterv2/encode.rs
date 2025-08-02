//! TODO: Encoder/Encodable
//! The idea is to build a serde style encoder which can be abstracted over
//! various destination types, while making heavy use of `bytes::bufmut` and zerocopy.
//!
//! The main benefit of Splinter's tail encoding is that the offset indexes are
//! emitted after each partition. This allows us to efficiently seek through the
//! encoding without making copies.
//!
//! However, we could still put offset indexes in the beginning by reserving
//! space ahead of time. Since cardinality is no longer expensive to compute, we
//! should be able to easily do this with the `BufMut` API.
//!
//! The other benefit of tail encoding is that we can encode directly into a
//! socket/stream. We aren't currently taking advantage of this.

pub trait Encodable {
    fn encoded_size(&self) -> usize;
}
