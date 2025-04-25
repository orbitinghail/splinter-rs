<h1 align="center">Splinter</h1>
<p align="center">
  <a href="https://docs.rs/splinter-rs"><img alt="docs.rs" src="https://img.shields.io/docsrs/splinter-rs"></a>
  &nbsp;
  <a href="https://github.com/orbitinghail/splinter-rs/actions"><img alt="Build Status" src="https://img.shields.io/github/actions/workflow/status/orbitinghail/splinter-rs/rust.yml"></a>
  &nbsp;
  <a href="https://crates.io/crates/splinter-rs"><img alt="crates.io" src="https://img.shields.io/crates/v/splinter-rs.svg"></a>
</p>

Splinter is a compressed bitmap format similar to [Roaring], optimized specifically for small, sparse sets of 32-bit unsigned integers (`u32`).

## Key Features:

- **Tree-based Encoding**: Splinter encodes `u32` values into a 256-way tree structure by decomposing integers into big-endian component bytes. Leaf nodes efficiently transition from byte lists to compact bitmaps at up to 32 values.

- **Zero-copy Access**: Designed for efficient querying without deserialization, the `SplinterRef` type allows direct, zero-copy reads from any type implementing `AsRef<[u8]>`.

[Roaring]: https://roaringbitmap.org/

## Serialized Format

```
header (4 bytes)
    magic (2 bytes)
    unused (2 bytes)

footer (4 bytes)
    partitions (2 bytes)
    unused (2 bytes)

block (cardinality)
    cardinality == 256
        data: OMITTED
    cardinality < 32
        data: [u8; cardinality]
    else
        data: [u8; 32]

index (cardinality, offset_size: u16|u32)
    keys: block(cardinality)
    cardinalities: [u8; cardinality] // 1 based
    offsets: [offset_size; cardinality]

map (cardinality, off_type, val_type)
    values: [val_type(index->cardinalities[i]); cardinality]
    index (cardinality, off_type)

splinter
    header
    map (footer->partitions, u32,
      map (cardinality, u32,
        map (cardinality, u16, block)))
    footer

```

## Comparison to Roaring

```
test                           bitmap         size   expected   relative         ok
empty                          Splinter          8          8       1.00         ok
                               Roaring           8          8       1.00         ok
                               Splinter LZ4     13         13       1.62          >
                               Roaring LZ4      13         13       1.00         ok
                               Baseline          0          0       0.00          <
1 element                      Splinter         25         25       1.00         ok
                               Roaring          18         18       0.72          <
                               Splinter LZ4     18         18       0.72          <
                               Roaring LZ4      20         20       1.11         ok
                               Baseline          4          4       0.16          <
1 dense block                  Splinter         24         24       1.00         ok
                               Roaring         528        528      22.00         ok
                               Splinter LZ4     20         20       0.83          <
                               Roaring LZ4     533        533      26.65         ok
                               Baseline       1024       1024      42.67         ok
1 half full block              Splinter         56         56       1.00         ok
                               Roaring         272        272       4.86         ok
                               Splinter LZ4     26         26       0.46          <
                               Roaring LZ4     276        276      10.62         ok
                               Baseline        512        512       9.14         ok
1 sparse block                 Splinter         40         40       1.00         ok
                               Roaring          48         48       1.20         ok
                               Splinter LZ4     37         37       0.93          <
                               Roaring LZ4      52         52       1.41         ok
                               Baseline         64         64       1.60         ok
8 half full blocks             Splinter        308        308       1.00         ok
                               Roaring        2064       2064       6.70         ok
                               Splinter LZ4     70         70       0.23          <
                               Roaring LZ4    2075       2075      29.64         ok
                               Baseline       4096       4096      13.30         ok
8 sparse blocks                Splinter         68         68       1.00         ok
                               Roaring          48         48       0.71          <
                               Splinter LZ4     56         56       0.82          <
                               Roaring LZ4      51         51       0.91          <
                               Baseline         64         64       0.94          <
64 half full blocks            Splinter       2432       2432       1.00         ok
                               Roaring       16520      16520       6.79         ok
                               Splinter LZ4    117        117       0.05          <
                               Roaring LZ4    1237       1237      10.57         ok
                               Baseline      32768      32768      13.47         ok
64 sparse blocks               Splinter        512        512       1.00         ok
                               Roaring         392        392       0.77          <
                               Splinter LZ4     84         84       0.16          <
                               Roaring LZ4     165        165       1.96         ok
                               Baseline        512        512       1.00         ok
256 half full blocks           Splinter       9440       9440       1.00         ok
                               Roaring       65800      65800       6.97         ok
                               Splinter LZ4    178        178       0.02          <
                               Roaring LZ4    2584       2584      14.52         ok
                               Baseline     131072     131072      13.88         ok
256 sparse blocks              Splinter       1760       1760       1.00         ok
                               Roaring        1288       1288       0.73          <
                               Splinter LZ4    122        122       0.07          <
                               Roaring LZ4     378        378       3.10         ok
                               Baseline       2048       2048       1.16         ok
512 half full blocks           Splinter      18872      18872       1.00         ok
                               Roaring      131592     131592       6.97         ok
                               Splinter LZ4    232        232       0.01          <
                               Roaring LZ4    3098       3098      13.35         ok
                               Baseline     262144     262144      13.89         ok
512 sparse blocks              Splinter       3512       3512       1.00         ok
                               Roaring        2568       2568       0.73          <
                               Splinter LZ4    142        142       0.04          <
                               Roaring LZ4     575        575       4.05         ok
                               Baseline       4096       4096       1.17         ok
fully dense                    Splinter         84         84       1.00         ok
                               Roaring        8208       8208      97.71         ok
                               Splinter LZ4     44         44       0.52          <
                               Roaring LZ4    8244       8244     187.36         ok
                               Baseline      16384      16384     195.05         ok
128/block; dense               Splinter       1172       1172       1.00         ok
                               Roaring        8208       8208       7.00         ok
                               Splinter LZ4    121        121       0.10          <
                               Roaring LZ4    8244       8244      68.13         ok
                               Baseline      16384      16384      13.98         ok
32/block; dense                Splinter       4532       4532       1.00         ok
                               Roaring        8208       8208       1.81         ok
                               Splinter LZ4    322        322       0.07          <
                               Roaring LZ4    8244       8244      25.60         ok
                               Baseline      16384      16384       3.62         ok
16/block; dense                Splinter       4884       4884       1.00         ok
                               Roaring        8208       8208       1.68         ok
                               Splinter LZ4    580        580       0.12          <
                               Roaring LZ4    8244       8244      14.21         ok
                               Baseline      16384      16384       3.35         ok
128/block; sparse mid          Splinter       1358       1358       1.00         ok
                               Roaring        8456       8456       6.23         ok
                               Splinter LZ4    176        176       0.13          <
                               Roaring LZ4     563        563       3.20         ok
                               Baseline      16384      16384      12.06         ok
128/block; sparse high         Splinter       1544       1544       1.00         ok
                               Roaring        8456       8456       5.48         ok
                               Splinter LZ4    179        179       0.12          <
                               Roaring LZ4     563        563       3.15         ok
                               Baseline      16384      16384      10.61         ok
1/block; sparse mid            Splinter      21774      21774       1.00         ok
                               Roaring       10248      10248       0.47          <
                               Splinter LZ4   1191       1191       0.05          <
                               Roaring LZ4    2138       2138       1.80         ok
                               Baseline      16384      16384       0.75          <
1/block; sparse high           Splinter      46344      46344       1.00         ok
                               Roaring       40968      40968       0.88          <
                               Splinter LZ4   1313       1313       0.03          <
                               Roaring LZ4   32931      32931      25.08         ok
                               Baseline      16384      16384       0.35          <
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE] or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT] or https://opensource.org/licenses/MIT)

at your option.

[LICENSE-APACHE]: ./LICENSE-APACHE
[LICENSE-MIT]: ./LICENSE-MIT

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be dual licensed as above, without any
additional terms or conditions.
