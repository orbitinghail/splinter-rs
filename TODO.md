# Splinter V2

New Splinter encoding that dynamically switches between Bitmap, Vec, Run, and Tree storage at every level of the u32 segment hierarchy. The following tasks will complete feature parity with the original Splinter code:

- Most important set ops: BitAnd, BitAndAssign, Cut
- Performance benchmarks against Splinter V1 and Roaring
- Remaining set ops: BitOr, BitOrAssign

# SIMD/AVX

- implement SIMD/AVX versions of block_contains and block_rank
- implement 64-bit versions for non-AVX/SIMD
