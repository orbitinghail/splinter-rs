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

[test code]: ./src/lib.rs#L308

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
1 half full block              Splinter         63         63       1.00         ok
                               Roaring         255        255       4.05       ++++
                               Splinter LZ4     64         64       1.02         ok
                               Roaring LZ4     257        257       4.02       ++++
                               Baseline        512        512       8.13       ++++
1 sparse block                 Splinter         48         48       1.00         ok
                               Roaring          48         48       1.00         ok
                               Splinter LZ4     49         49       1.02         ok
                               Roaring LZ4      50         50       1.02         ok
                               Baseline         64         64       1.33          +
8 half full blocks             Splinter        315        315       1.00         ok
                               Roaring        2003       2003       6.36       ++++
                               Splinter LZ4    318        318       1.01         ok
                               Roaring LZ4    2012       2012       6.33       ++++
                               Baseline       4096       4096      13.00       ++++
8 sparse blocks                Splinter         60         60       1.00         ok
                               Roaring          48         48       0.80          -
                               Splinter LZ4     62         62       1.03         ok
                               Roaring LZ4      50         50       0.81          -
                               Baseline         64         64       1.07         ok
64 half full blocks            Splinter       2442       2442       1.00         ok
                               Roaring       16452      16452       6.74       ++++
                               Splinter LZ4   2336       2336       0.96         ok
                               Roaring LZ4   16503      16503       7.06       ++++
                               Baseline      32768      32768      13.42       ++++
64 sparse blocks               Splinter        410        410       1.00         ok
                               Roaring         392        392       0.96         ok
                               Splinter LZ4    379        379       0.92         ok
                               Roaring LZ4     395        395       1.04         ok
                               Baseline        512        512       1.25          +
256 half full blocks           Splinter       9450       9450       1.00         ok
                               Roaring       65580      65580       6.94       ++++
                               Splinter LZ4   9015       9015       0.95         ok
                               Roaring LZ4   65835      65835       7.30       ++++
                               Baseline     131072     131072      13.87       ++++
256 sparse blocks              Splinter       1290       1290       1.00         ok
                               Roaring        1288       1288       1.00         ok
                               Splinter LZ4   1250       1250       0.97         ok
                               Roaring LZ4    1294       1294       1.04         ok
                               Baseline       2048       2048       1.59          +
512 half full blocks           Splinter      18886      18886       1.00         ok
                               Roaring      130810     130810       6.93       ++++
                               Splinter LZ4  17974      17974       0.95         ok
                               Roaring LZ4  131248     131248       7.30       ++++
                               Baseline     262144     262144      13.88       ++++
512 sparse blocks              Splinter       2566       2566       1.00         ok
                               Roaring        2568       2568       1.00         ok
                               Splinter LZ4   2416       2416       0.94         ok
                               Roaring LZ4    2580       2580       1.07         ok
                               Baseline       4096       4096       1.60          +
fully dense                    Splinter         80         80       1.00         ok
                               Roaring          63         63       0.79          -
                               Splinter LZ4     82         82       1.02         ok
                               Roaring LZ4      65         65       0.79          -
                               Baseline      16384      16384     204.80       ++++
128/block; dense               Splinter       1179       1179       1.00         ok
                               Roaring        8208       8208       6.96       ++++
                               Splinter LZ4   1185       1185       1.01         ok
                               Roaring LZ4    8242       8242       6.96       ++++
                               Baseline      16384      16384      13.90       ++++
32/block; dense                Splinter       4539       4539       1.00         ok
                               Roaring        8208       8208       1.81         ++
                               Splinter LZ4   4302       4302       0.95         ok
                               Roaring LZ4    8242       8242       1.92         ++
                               Baseline      16384      16384       3.61        +++
16/block; dense                Splinter       5147       5147       1.00         ok
                               Roaring        8208       8208       1.59          +
                               Splinter LZ4   5145       5145       1.00         ok
                               Roaring LZ4    8242       8242       1.60         ++
                               Baseline      16384      16384       3.18        +++
128/block; sparse mid          Splinter       1365       1365       1.00         ok
                               Roaring        8282       8282       6.07       ++++
                               Splinter LZ4   1372       1372       1.01         ok
                               Roaring LZ4    8311       8311       6.06       ++++
                               Baseline      16384      16384      12.00       ++++
128/block; sparse high         Splinter       1582       1582       1.00         ok
                               Roaring        8224       8224       5.20       ++++
                               Splinter LZ4   1539       1539       0.97         ok
                               Roaring LZ4    8258       8258       5.37       ++++
                               Baseline      16384      16384      10.36       ++++
1/block; sparse mid            Splinter       9749       9749       1.00         ok
                               Roaring       10248      10248       1.05         ok
                               Splinter LZ4   9750       9750       1.00         ok
                               Roaring LZ4   10290      10290       1.06         ok
                               Baseline      16384      16384       1.68         ++
1/block; sparse high           Splinter      14350      14350       1.00         ok
                               Roaring       40968      40968       2.85        +++
                               Splinter LZ4  14297      14297       1.00         ok
                               Roaring LZ4   41084      41084       2.87        +++
                               Baseline      16384      16384       1.14          +
1/block; spread low            Splinter       8325       8325       1.00         ok
                               Roaring        8328       8328       1.00         ok
                               Splinter LZ4    637        637       0.08       ----
                               Roaring LZ4     689        689       1.08         ok
                               Baseline      16384      16384       1.97         ++
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
random/4096                    Splinter      14350      14350       1.00         ok
                               Roaring       40056      40056       2.79        +++
                               Splinter LZ4  14359      14359       1.00         ok
                               Roaring LZ4   40208      40208       2.80        +++
                               Baseline      16384      16384       1.14          +
random/16384                   Splinter      51214      51214       1.00         ok
                               Roaring      148656     148656       2.90        +++
                               Splinter LZ4  51416      51416       1.00         ok
                               Roaring LZ4  149229     149229       2.90        +++
                               Baseline      65536      65536       1.28          +
random/65536                   Splinter     198670     198670       1.00         ok
                               Roaring      461288     461288       2.32         ++
                               Splinter LZ4 199451     199451       1.00         ok
                               Roaring LZ4  463095     463095       2.32         ++
                               Baseline     262144     262144       1.32          +
random/32/65536                Splinter         92         92       1.00         ok
                               Roaring          80         80       0.87          -
                               Splinter LZ4     92         92       1.00         ok
                               Roaring LZ4      81         81       0.88          -
                               Baseline        128        128       1.39          +
random/256/65536               Splinter        540        540       1.00         ok
                               Roaring         528        528       0.98         ok
                               Splinter LZ4    544        544       1.01         ok
                               Roaring LZ4     530        530       0.97         ok
                               Baseline       1024       1024       1.90         ++
random/1024/65536              Splinter       2071       2071       1.00         ok
                               Roaring        2064       2064       1.00         ok
                               Splinter LZ4   2081       2081       1.00         ok
                               Roaring LZ4    2072       2072       1.00         ok
                               Baseline       4096       4096       1.98         ++
random/4096/65536              Splinter       5147       5147       1.00         ok
                               Roaring        8208       8208       1.59          +
                               Splinter LZ4   5169       5169       1.00         ok
                               Roaring LZ4    8241       8241       1.59          +
                               Baseline      16384      16384       3.18        +++
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
random/16/1024                 Splinter         60         60       1.00         ok
                               Roaring          48         48       0.80          -
                               Splinter LZ4     61         61       1.02         ok
                               Roaring LZ4      49         49       0.80          -
                               Baseline         64         64       1.07         ok
random/32/1024                 Splinter         79         79       1.00         ok
                               Roaring          80         80       1.01         ok
                               Splinter LZ4     77         77       0.97         ok
                               Roaring LZ4      81         81       1.05         ok
                               Baseline        128        128       1.62         ++
random/64/1024                 Splinter        111        111       1.00         ok
                               Roaring         144        144       1.30          +
                               Splinter LZ4    110        110       0.99         ok
                               Roaring LZ4     145        145       1.32          +
                               Baseline        256        256       2.31         ++
random/128/1024                Splinter        168        168       1.00         ok
                               Roaring         272        272       1.62         ++
                               Splinter LZ4    167        167       0.99         ok
                               Roaring LZ4     273        273       1.63         ++
                               Baseline        512        512       3.05        +++
average compression ratio (splinter_lz4 / splinter): 0.97
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
