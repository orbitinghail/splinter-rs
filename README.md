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

- **Tree-based Encoding**: Splinter encodes `u32` values into a 256-way tree structure by decomposing integers into big-endian component bytes. Nodes throughout the tree (including the root) are optimized into four different storage classes: tree, vec, bitmap, run.

- **Zero-copy Access**: Designed for efficient querying without deserialization, the `SplinterRef` type allows direct, zero-copy reads from any type implementing `Deref<Target = [u8]>`.

[Roaring]: https://roaringbitmap.org/

## Comparison to Roaring

The following table tests Splinter and Roaring with/without LZ4 compression against many different data distributions. The size column represents number of bytes used when serialized. View the actual [test code] to understand the precise meaning of each distribution.

All tests optimize the bitmap before serialization. This ensures that Splinter and Roaring are able to maximize compression via the use of run-length encoding for cases where it is helpful.

Roaring tests use [`roaring-rs`].

[`roaring-rs`]: https://docs.rs/roaring/latest/roaring/

The `Baseline` "bitmap" is simply an array of `u32` integers.

The `ok` column compares each subsequent row to the `Splinter` row for that test. `-` and `+` signs refer to the relative size of the row compared to the Splinter per the `relative` column. For example a row containing `++++` is many times larger than the corresponding Splinter.

[test code]: ./src/splinter.rs#L604

```
test                           bitmap         size   expected   relative         ok
empty                          Splinter         13         13       1.00         ok
                               Roaring           8          8       0.62         --
                               Splinter LZ4     14         14       1.08         ok
                               Roaring LZ4       9          9       0.64          -
                               Baseline          0          0       0.00       ----
1 element                      Splinter         21         21       1.00         ok
                               Roaring          18         18       0.86          -
                               Splinter LZ4     23         23       1.10         ok
                               Roaring LZ4      20         20       0.87          -
                               Baseline          4          4       0.19       ----
1 dense block                  Splinter         25         25       1.00         ok
                               Roaring          15         15       0.60         --
                               Splinter LZ4     27         27       1.08         ok
                               Roaring LZ4      17         17       0.63          -
                               Baseline       1024       1024      40.96       ++++
1 half full block              Splinter         72         72       1.00         ok
                               Roaring         255        255       3.54        +++
                               Splinter LZ4     70         70       0.97         ok
                               Roaring LZ4     257        257       3.67        +++
                               Baseline        512        512       7.11       ++++
1 sparse block                 Splinter         57         57       1.00         ok
                               Roaring          48         48       0.84          -
                               Splinter LZ4     55         55       0.96         ok
                               Roaring LZ4      50         50       0.91          -
                               Baseline         64         64       1.12          +
8 half full blocks             Splinter        338        338       1.00         ok
                               Roaring        2003       2003       5.93       ++++
                               Splinter LZ4    339        339       1.00         ok
                               Roaring LZ4    2012       2012       5.94       ++++
                               Baseline       4096       4096      12.12       ++++
8 sparse blocks                Splinter         67         67       1.00         ok
                               Roaring          48         48       0.72          -
                               Splinter LZ4     68         68       1.01         ok
                               Roaring LZ4      50         50       0.74          -
                               Baseline         64         64       0.96         ok
64 half full blocks            Splinter       2634       2634       1.00         ok
                               Roaring       16452      16452       6.25       ++++
                               Splinter LZ4   2375       2375       0.90          -
                               Roaring LZ4   16503      16503       6.95       ++++
                               Baseline      32768      32768      12.44       ++++
64 sparse blocks               Splinter        450        450       1.00         ok
                               Roaring         392        392       0.87          -
                               Splinter LZ4    450        450       1.00         ok
                               Roaring LZ4     395        395       0.88          -
                               Baseline        512        512       1.14          +
256 half full blocks           Splinter      10074      10074       1.00         ok
                               Roaring       65580      65580       6.51       ++++
                               Splinter LZ4   9101       9101       0.90          -
                               Roaring LZ4   65835      65835       7.23       ++++
                               Baseline     131072     131072      13.01       ++++
256 sparse blocks              Splinter       1402       1402       1.00         ok
                               Roaring        1288       1288       0.92         ok
                               Splinter LZ4   1269       1269       0.91          -
                               Roaring LZ4    1294       1294       1.02         ok
                               Baseline       2048       2048       1.46          +
512 half full blocks           Splinter      20134      20134       1.00         ok
                               Roaring      130810     130810       6.50       ++++
                               Splinter LZ4  18137      18137       0.90          -
                               Roaring LZ4  131248     131248       7.24       ++++
                               Baseline     262144     262144      13.02       ++++
512 sparse blocks              Splinter       2790       2790       1.00         ok
                               Roaring        2568       2568       0.92         ok
                               Splinter LZ4   2470       2470       0.89          -
                               Roaring LZ4    2580       2580       1.04         ok
                               Baseline       4096       4096       1.47          +
fully dense                    Splinter        121        121       1.00         ok
                               Roaring          63         63       0.52         --
                               Splinter LZ4    123        123       1.02         ok
                               Roaring LZ4      65         65       0.53         --
                               Baseline      16384      16384     135.40       ++++
128/block; dense               Splinter       1250       1250       1.00         ok
                               Roaring        8208       8208       6.57       ++++
                               Splinter LZ4   1256       1256       1.00         ok
                               Roaring LZ4    8242       8242       6.56       ++++
                               Baseline      16384      16384      13.11       ++++
32/block; dense                Splinter       4802       4802       1.00         ok
                               Roaring        8208       8208       1.71         ++
                               Splinter LZ4   4564       4564       0.95         ok
                               Roaring LZ4    8242       8242       1.81         ++
                               Baseline      16384      16384       3.41        +++
16/block; dense                Splinter       5666       5666       1.00         ok
                               Roaring        8208       8208       1.45          +
                               Splinter LZ4   5666       5666       1.00         ok
                               Roaring LZ4    8242       8242       1.45          +
                               Baseline      16384      16384       2.89        +++
128/block; sparse mid          Splinter       1529       1529       1.00         ok
                               Roaring        8282       8282       5.42       ++++
                               Splinter LZ4   1494       1494       0.98         ok
                               Roaring LZ4    8311       8311       5.56       ++++
                               Baseline      16384      16384      10.72       ++++
128/block; sparse high         Splinter       1870       1870       1.00         ok
                               Roaring        8224       8224       4.40       ++++
                               Splinter LZ4   1679       1679       0.90          -
                               Roaring LZ4    8258       8258       4.92       ++++
                               Baseline      16384      16384       8.76       ++++
1/block; sparse mid            Splinter      10521      10521       1.00         ok
                               Roaring       10248      10248       0.97         ok
                               Splinter LZ4  10525      10525       1.00         ok
                               Roaring LZ4   10290      10290       0.98         ok
                               Baseline      16384      16384       1.56          +
1/block; sparse high           Splinter      15374      15374       1.00         ok
                               Roaring       40968      40968       2.66        +++
                               Splinter LZ4  15319      15319       1.00         ok
                               Roaring LZ4   41084      41084       2.68        +++
                               Baseline      16384      16384       1.07         ok
1/block; spread low            Splinter       8377       8377       1.00         ok
                               Roaring        8328       8328       0.99         ok
                               Splinter LZ4    687        687       0.08       ----
                               Roaring LZ4     689        689       1.00         ok
                               Baseline      16384      16384       1.96         ++
dense throughout               Splinter       4113       4113       1.00         ok
                               Roaring        2700       2700       0.66          -
                               Splinter LZ4   3643       3643       0.89          -
                               Roaring LZ4     608        608       0.17       ----
                               Baseline      16384      16384       3.98        +++
dense low                      Splinter        529        529       1.00         ok
                               Roaring         267        267       0.50         --
                               Splinter LZ4    529        529       1.00         ok
                               Roaring LZ4     269        269       0.51         --
                               Baseline      16384      16384      30.97       ++++
dense mid/low                  Splinter       4113       4113       1.00         ok
                               Roaring        2376       2376       0.58         --
                               Splinter LZ4   4077       4077       0.99         ok
                               Roaring LZ4     348        348       0.09       ----
                               Baseline      16384      16384       3.98        +++
random/32                      Splinter        145        145       1.00         ok
                               Roaring         328        328       2.26         ++
                               Splinter LZ4    147        147       1.01         ok
                               Roaring LZ4     331        331       2.25         ++
                               Baseline        128        128       0.88          -
random/256                     Splinter       1041       1041       1.00         ok
                               Roaring        2544       2544       2.44         ++
                               Splinter LZ4   1047       1047       1.01         ok
                               Roaring LZ4    2553       2553       2.44         ++
                               Baseline       1024       1024       0.98         ok
random/1024                    Splinter       4113       4113       1.00         ok
                               Roaring       10168      10168       2.47         ++
                               Splinter LZ4   4131       4131       1.00         ok
                               Roaring LZ4   10208      10208       2.47         ++
                               Baseline       4096       4096       1.00         ok
random/4096                    Splinter      15374      15374       1.00         ok
                               Roaring       40056      40056       2.61        +++
                               Splinter LZ4  15380      15380       1.00         ok
                               Roaring LZ4   40208      40208       2.61        +++
                               Baseline      16384      16384       1.07         ok
random/16384                   Splinter      52238      52238       1.00         ok
                               Roaring      148656     148656       2.85        +++
                               Splinter LZ4  52444      52444       1.00         ok
                               Roaring LZ4  149229     149229       2.85        +++
                               Baseline      65536      65536       1.25          +
random/65536                   Splinter     199694     199694       1.00         ok
                               Roaring      461288     461288       2.31         ++
                               Splinter LZ4 200479     200479       1.00         ok
                               Roaring LZ4  463095     463095       2.31         ++
                               Baseline     262144     262144       1.31          +
random/32/65536                Splinter         99         99       1.00         ok
                               Roaring          80         80       0.81          -
                               Splinter LZ4     96         96       0.97         ok
                               Roaring LZ4      81         81       0.84          -
                               Baseline        128        128       1.29          +
random/256/65536               Splinter        547        547       1.00         ok
                               Roaring         528        528       0.97         ok
                               Splinter LZ4    551        551       1.01         ok
                               Roaring LZ4     530        530       0.96         ok
                               Baseline       1024       1024       1.87         ++
random/1024/65536              Splinter       2083       2083       1.00         ok
                               Roaring        2064       2064       0.99         ok
                               Splinter LZ4   2093       2093       1.00         ok
                               Roaring LZ4    2072       2072       0.99         ok
                               Baseline       4096       4096       1.97         ++
random/4096/65536              Splinter       5666       5666       1.00         ok
                               Roaring        8208       8208       1.45          +
                               Splinter LZ4   5690       5690       1.00         ok
                               Roaring LZ4    8241       8241       1.45          +
                               Baseline      16384      16384       2.89        +++
random/65536/65536             Splinter         25         25       1.00         ok
                               Roaring          15         15       0.60         --
                               Splinter LZ4     23         23       0.92         ok
                               Roaring LZ4      17         17       0.74          -
                               Baseline     262144     262144   10485.76       ++++
random/8/1024                  Splinter         49         49       1.00         ok
                               Roaring          32         32       0.65          -
                               Splinter LZ4     51         51       1.04         ok
                               Roaring LZ4      33         33       0.65          -
                               Baseline         32         32       0.65          -
random/16/1024                 Splinter         67         67       1.00         ok
                               Roaring          48         48       0.72          -
                               Splinter LZ4     63         63       0.94         ok
                               Roaring LZ4      49         49       0.78          -
                               Baseline         64         64       0.96         ok
random/32/1024                 Splinter         94         94       1.00         ok
                               Roaring          80         80       0.85          -
                               Splinter LZ4     89         89       0.95         ok
                               Roaring LZ4      81         81       0.91         ok
                               Baseline        128        128       1.36          +
random/64/1024                 Splinter        126        126       1.00         ok
                               Roaring         144        144       1.14          +
                               Splinter LZ4    124        124       0.98         ok
                               Roaring LZ4     145        145       1.17          +
                               Baseline        256        256       2.03         ++
random/128/1024                Splinter        183        183       1.00         ok
                               Roaring         272        272       1.49          +
                               Splinter LZ4    180        180       0.98         ok
                               Roaring LZ4     273        273       1.52          +
                               Baseline        512        512       2.80        +++
average compression ratio (splinter_lz4 / splinter): 0.96
```

## Adaptations

Splinter is available in Python using the [splynters](https://pypi.org/project/splynters/) package on PyPI, and as an open [repository](https://github.com/nrposner/splynters) of the same name.

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
