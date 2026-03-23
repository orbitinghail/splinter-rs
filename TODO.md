# Feature ideas

## Inverted partitions

When a partition is storing more than 50% of it's possible values, we should store it inverted. Thus partitions only grow until they hit 50% of their max storage, and then they start shrinking.

Some bits are already reserved in PartitionKind to support the concept of inversion.

## Recursive validation

Currently untrusted Splinters can trivially cause panics at runtime. No known memory unsafety exists, however for any usage of Splinter with untrusted data a better validation system will be required. This validation system should support scanning the entire Serialized Splinter and verify that it can be correctly decoded with no overflows. The most likely overflow cause is the offsets array stored with TreePartitions.

## Optimize range

The `PartitionRead::range()` function can be optimized to skip over entire partitions during tree iteration.

---

# Tasks

## Performance

### Fix SubAssign fallback to use O(n+m) merge instead of O(n·log m) filter

`src/partition_ops.rs:291-296` and `src/partition_ops.rs:326-330`

The `SubAssign` fallback uses `.filter(|a| !b.contains(*a))`, calling `contains` (binary search or tree lookup) per element. The other ops (`BitAnd`, `BitXor`) use O(n+m) `merge_join_by` on sorted iterators. Replace with:

```rust
(a, b) => {
    *a = std::mem::take(a)
        .iter()
        .merge_join_by(b.iter(), L::Value::cmp)
        .filter_map(|x| match x {
            EitherOrBoth::Left(l) => Some(l),
            _ => None,
        })
        .collect();
}
```

Same fix applies to the `SubAssign<&PartitionRef>` impl.

### Avoid allocation in BitmapPartition::SubAssign

`src/partition/bitmap.rs:310-316`

Both `SubAssign` impls (for `&BitmapPartition` and `&BitSlice`) clone the RHS bitmap just to negate it. Use an in-place loop over raw slices instead:

```rust
for (dst, src) in self.bitmap.as_raw_mut_slice().iter_mut()
    .zip(rhs.bitmap.as_raw_slice().iter())
{
    dst.store_value(dst.load_value() & !src.load_value());
}
```

### Don't convert empty partitions to Tree in optimize_kind

`src/partition.rs:124-128`

When `cardinality == 0` and `L::ALLOW_TREE`, `optimize_kind` returns `PartitionKind::Tree`, which converts an empty `VecPartition` (const, no allocation) into a `TreePartition` (creates a `BTreeMap`) for no benefit — empty partitions encode as `PartitionKind::Empty` regardless. Return `PartitionKind::Empty` instead.

## Documentation

### Regenerate README compression table

The README table doesn't match current test expectations in `src/splinter.rs:882-927`. Several entries are stale after compression improvements:

| Test | README | Actual |
|------|--------|--------|
| fully dense | 121 | 87 |
| dense low | 529 | 291 |
| dense mid/low | 4113 | 2393 |
| dense throughout | 4113 | 2790 |

## Code quality

### Deduplicate contains_all / contains_any between owned and ref types

The logic for `contains_all` and `contains_any` is implemented three times for each partition variant (owned in `partition/*.rs`, ref in `codec/partition_ref.rs`, tree in both). The Vec and Run implementations are nearly identical. A shared free function taking iterators could reduce this duplication.

### Add precondition check to count_bitmap_runs

`src/count.rs:56-65`

`count_bitmap_runs` indexes `bitmap[0]` and `bitmap[1..]` without checking length. It's only called from `BitmapPartition::count_runs` where length is always ≥256, but the function is `pub` with no documented precondition. Add `debug_assert!(!bitmap.is_empty())` or handle the empty case.

### Remove debug leftover in test

`src/partition.rs:530-531`

```rust
if kind == PartitionKind::Tree && i == 5 {
    println!("break")  // debug leftover, remove
}
```

### Document little-endian platform restriction

`src/codec/encoder.rs:136` has `static_assertions::assert_cfg!(target_endian = "little")` which is a hard compile-time failure on big-endian. `Cargo.toml` and README should document this platform restriction.
