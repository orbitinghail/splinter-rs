# Feature ideas

## Merge + specialize optimize kind and switch kind to eliminate Never

Goal is to eliminate any code paths (after DCE) that could construct Never.

## Bench switching Tree partitions to HashMap

Seems like BTreeMap may introduce more pointer chasing than needed. Also tree in tree seems silly.

## Inverted partitions

When a partition is storing more than 50% of it's possible values, we should store it inverted. Thus partitions only grow until they hit 50% of their max storage, and then they start shrinking.

Some bits are already reserved in PartitionKind to support the concept of inversion.

## Recursive validation

Currently untrusted Splinters can trivially cause panics at runtime. No known memory unsafety exists, however for any usage of Splinter with untrusted data a better validation system will be required. This validation system should support scanning the entire Serialized Splinter and verify that it can be correctly decoded with no overflows. The most likely overflow cause is the offsets array stored with TreePartitions.

## Optimize range

The `PartitionRead::range()` function can be optimized to skip over entire partitions during tree iteration.
