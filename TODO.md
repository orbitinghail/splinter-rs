# Splinter V2

New Splinter encoding that dynamically switches between Bitmap, Vec, Run, and Tree storage at every level of the u32 segment hierarchy. The following tasks will complete feature parity with the original Splinter code:

- inverted partitions: if cardinality > 50% store the partition inverted
- add a recursive validation function to PartitionRef
