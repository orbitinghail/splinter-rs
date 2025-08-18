# Splinter V2

New Splinter encoding that dynamically switches between Bitmap, Vec, Run, and Tree storage at every level of the u32 segment hierarchy. The following tasks will complete feature parity with the original Splinter code:

- Implement `Merge/Cut` for Splinter and SplinterRef
- Implement `PartialEq` for SplinterRef<>SplinterRef
- Performance benchmarks against Splinter V1 and Roaring
