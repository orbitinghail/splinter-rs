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

The following table tests Splinter and Roaring with/without LZ4 compression against many different data distributions. The size column represents number of bytes used when serialized. View the actual [test code] to understand the precise meaning of each distribution.

Roaring tests use [`roaring-rs`] and optimize the bitmap before serialization. This means that range-compression is in use which can dramatically improve `Roaring`'s compression but affects its serialization performance.

[`roaring-rs`]: https://docs.rs/roaring/latest/roaring/

The `Baseline` "bitmap" is simply an array of `u32` integers.

The `ok` column compares each subsequent row to the `Splinter` for that test. The value `<` means the row is smaller than the `Splinter`. The value `>` means the row is larger than the `Splinter`.

[test code]: ./src/splinter.rs#L725

```
test                           bitmap         size   expected   relative         ok
empty                          Splinter          8          8       1.00         ok
                               Roaring           8          8       1.00         ok
                               Splinter LZ4      9          9       1.12          >
                               Roaring LZ4       9          9       1.00         ok
                               Baseline          0          0       0.00          <
1 element                      Splinter         25         25       1.00         ok
                               Roaring          18         18       0.72          <
                               Splinter LZ4     25         25       1.00          >
                               Roaring LZ4      20         20       0.80          <
                               Baseline          4          4       0.16          <
1 dense block                  Splinter         24         24       1.00         ok
                               Roaring          15         15       0.62          <
                               Splinter LZ4     24         24       1.00          >
                               Roaring LZ4      17         17       0.71          <
                               Baseline       1024       1024      42.67         ok
1 half full block              Splinter         56         56       1.00         ok
                               Roaring         247        247       4.41         ok
                               Splinter LZ4     57         57       1.02          >
                               Roaring LZ4     249        249       4.37         ok
                               Baseline        512        512       9.14         ok
1 sparse block                 Splinter         40         40       1.00         ok
                               Roaring          48         48       1.20         ok
                               Splinter LZ4     41         41       1.02          >
                               Roaring LZ4      50         50       1.22         ok
                               Baseline         64         64       1.60         ok
8 half full blocks             Splinter        308        308       1.00         ok
                               Roaring        2064       2064       6.70         ok
                               Splinter LZ4    310        310       1.01          >
                               Roaring LZ4    2074       2074       6.69         ok
                               Baseline       4096       4096      13.30         ok
8 sparse blocks                Splinter         68         68       1.00         ok
                               Roaring          48         48       0.71          <
                               Splinter LZ4     67         67       0.99          <
                               Roaring LZ4      50         50       0.75          <
                               Baseline         64         64       0.94          <
64 half full blocks            Splinter       2432       2432       1.00         ok
                               Roaring       16486      16486       6.78         ok
                               Splinter LZ4   2264       2264       0.93          <
                               Roaring LZ4   16551      16551       7.31         ok
                               Baseline      32768      32768      13.47         ok
64 sparse blocks               Splinter        512        512       1.00         ok
                               Roaring         392        392       0.77          <
                               Splinter LZ4    328        328       0.64          <
                               Roaring LZ4     395        395       1.20         ok
                               Baseline        512        512       1.00         ok
256 half full blocks           Splinter       9440       9440       1.00         ok
                               Roaring       65520      65520       6.94         ok
                               Splinter LZ4   8744       8744       0.93          <
                               Roaring LZ4   65666      65666       7.51         ok
                               Baseline     131072     131072      13.88         ok
256 sparse blocks              Splinter       1760       1760       1.00         ok
                               Roaring        1288       1288       0.73          <
                               Splinter LZ4   1053       1053       0.60          <
                               Roaring LZ4    1294       1294       1.23         ok
                               Baseline       2048       2048       1.16         ok
512 half full blocks           Splinter      18872      18872       1.00         ok
                               Roaring      130742     130742       6.93         ok
                               Splinter LZ4  17418      17418       0.92          <
                               Roaring LZ4  131208     131208       7.53         ok
                               Baseline     262144     262144      13.89         ok
512 sparse blocks              Splinter       3512       3512       1.00         ok
                               Roaring        2568       2568       0.73          <
                               Splinter LZ4   2029       2029       0.58          <
                               Roaring LZ4    2580       2580       1.27         ok
                               Baseline       4096       4096       1.17         ok
fully dense                    Splinter         84         84       1.00         ok
                               Roaring          75         75       0.89          <
                               Splinter LZ4     46         46       0.55          <
                               Roaring LZ4      77         77       1.67         ok
                               Baseline      16384      16384     195.05         ok
128/block; dense               Splinter       1172       1172       1.00         ok
                               Roaring        8195       8195       6.99         ok
                               Splinter LZ4   1157       1157       0.99          <
                               Roaring LZ4    8229       8229       7.11         ok
                               Baseline      16384      16384      13.98         ok
32/block; dense                Splinter       4532       4532       1.00         ok
                               Roaring        8208       8208       1.81         ok
                               Splinter LZ4   4195       4195       0.93          <
                               Roaring LZ4    8242       8242       1.96         ok
                               Baseline      16384      16384       3.62         ok
16/block; dense                Splinter       4884       4884       1.00         ok
                               Roaring        8208       8208       1.68         ok
                               Splinter LZ4   4664       4664       0.95          <
                               Roaring LZ4    8242       8242       1.77         ok
                               Baseline      16384      16384       3.35         ok
128/block; sparse mid          Splinter       1358       1358       1.00         ok
                               Roaring        8300       8300       6.11         ok
                               Splinter LZ4   1338       1338       0.99          <
                               Roaring LZ4    8307       8307       6.21         ok
                               Baseline      16384      16384      12.06         ok
128/block; sparse high         Splinter       1544       1544       1.00         ok
                               Roaring        8290       8290       5.37         ok
                               Splinter LZ4   1480       1480       0.96          <
                               Roaring LZ4    8324       8324       5.62         ok
                               Baseline      16384      16384      10.61         ok
1/block; sparse mid            Splinter      21774      21774       1.00         ok
                               Roaring       10248      10248       0.47          <
                               Splinter LZ4  10354      10354       0.48          <
                               Roaring LZ4   10290      10290       0.99          <
                               Baseline      16384      16384       0.75          <
1/block; sparse high           Splinter      46344      46344       1.00         ok
                               Roaring       40968      40968       0.88          <
                               Splinter LZ4  23916      23916       0.52          <
                               Roaring LZ4   41114      41114       1.72         ok
                               Baseline      16384      16384       0.35          <
1/block; spread low            Splinter      16494      16494       1.00         ok
                               Roaring        8328       8328       0.50          <
                               Splinter LZ4    682        682       0.04          <
                               Roaring LZ4     689        689       1.01         ok
                               Baseline      16384      16384       0.99          <
dense throughout               Splinter       6584       6584       1.00         ok
                               Roaring        2700       2700       0.41          <
                               Splinter LZ4    154        154       0.02          <
                               Roaring LZ4     608        608       3.95         ok
                               Baseline      16384      16384       2.49         ok
dense low                      Splinter       2292       2292       1.00         ok
                               Roaring         267        267       0.12          <
                               Splinter LZ4    177        177       0.08          <
                               Roaring LZ4     269        269       1.52         ok
                               Baseline      16384      16384       7.15         ok
dense mid/low                  Splinter       6350       6350       1.00         ok
                               Roaring        2376       2376       0.37          <
                               Splinter LZ4    245        245       0.04          <
                               Roaring LZ4     348        348       1.42         ok
                               Baseline      16384      16384       2.58         ok
random/32                      Splinter        546        546       1.00         ok
                               Roaring         328        328       0.60          <
                               Splinter LZ4    443        443       0.81          <
                               Roaring LZ4     331        331       0.75          <
                               Baseline        128        128       0.23          <
random/256                     Splinter       3655       3655       1.00         ok
                               Roaring        2560       2560       0.70          <
                               Splinter LZ4   2701       2701       0.74          <
                               Roaring LZ4    2568       2568       0.95          <
                               Baseline       1024       1024       0.28          <
random/1024                    Splinter      12499      12499       1.00         ok
                               Roaring       10168      10168       0.81          <
                               Splinter LZ4   7964       7964       0.64          <
                               Roaring LZ4   10185      10185       1.28         ok
                               Baseline       4096       4096       0.33          <
random/4096                    Splinter      45582      45582       1.00         ok
                               Roaring       39952      39952       0.88          <
                               Splinter LZ4  25524      25524       0.56          <
                               Roaring LZ4   40050      40050       1.57         ok
                               Baseline      16384      16384       0.36          <
random/16384                   Splinter     163758     163758       1.00         ok
                               Roaring      148600     148600       0.91          <
                               Splinter LZ4 102242     102242       0.62          <
                               Roaring LZ4  149184     149184       1.46         ok
                               Baseline      65536      65536       0.40          <
random/65535                   Splinter     543584     543584       1.00         ok
                               Roaring      462190     462190       0.85          <
                               Splinter LZ4 359107     359107       0.66          <
                               Roaring LZ4  464002     464002       1.29         ok
                               Baseline     262140     262140       0.48          <
average compression ratio (splinter_lz4 / splinter): 0.72
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
