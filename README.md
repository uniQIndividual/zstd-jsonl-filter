# What this for?

zstd-jsonl-filter uses zstd's stream decoder and rayon's parallelism to very efficiently decompress and filter gigantic datasets in memory. The goal is minimal I/O and memory usage. I uploaded this just in case there's another person out there who, for whatever reason, needs to efficiently go through terabytes of zstd compressed data which can be interpreted line-by-line.

<img src=".\assets\explanation.svg" width="600"/>

\
\
To be clear: zstd-jsonl-filter is mainly useful if

- you have multiple zstd archives
- they each contain a single file that can be interpreted line-by-line\
(any newline separated file should work, although I only tested JSON Lines)
- the uncompressed data is infeasible to store in ram or save to disk

Make sure your data is in the following format:

```
input_path
├───12000000000-12010000000.jsonl.zst
│   └───12000000000-12010000000.jsonl
├───12010000000-12020000000.jsonl.zst
│   └───12010000000-12020000000.jsonl
.
.
.
```


The naming scheme is irrelevant, but the current implementation does not work with multiple files within an archive.

# Performance

Using a Ryzen 9 3900x and a test set of 200 GB zstd archives stored on NVMe drives, zstd-jsonl-filter was entirely CPU bound with average read speeds of ~430 MB/s from disk and ~6.8 GB/s of uncompressed data processed in memory. These operation took on average 8 min and processed 3 TB of uncompressed data.

### CPU

zstd-jsonl-filter uses rayon for parallelization across files. This means it decompresses one file per thread at once. If you have less files than suggested threads, you will not see any speedup. You can set the maximum number of threads ``max_threads``.

Matching is performed via regex because it was significantly faster than parsing each JSON. This depends on what you have to work with, adjust as needed.

### Memory

Stream decompression drastically reduces the memory usage, however the number of spawned threads and their respective write buffer request 100 MB each by default. These could add up to a few gigabytes though in practice you can stay far below that for strong filter ratios. It depends on how large these buffers grow simultaneously. You can change all these parameters in the config.

### I/O

Although zstd-jsonl-filter is usually CPU bound when reading from an NVMe, that can quickly change for other sources. Reading from network storage or hard drives can be a bottleneck. You can set ``max_threads`` to change the number of simultaneous file operations if that impacts your source medium.

Write speeds depends on how many entires are filtered, your storage speed and if compression is used.
By default zstd-jsonl-filter writes to a 100 MB buffer which you can adjust as needed.

# How you could use it

Say you had a couple billion ~~Destiny 2 PGCR sessions~~ log entires from your fortune 500 employer (please don't actually use it for that) which are neatly separated in smaller chunks and individually compressed with zstd. If you wanted to identify  ~~all instances of Team Scorched games~~ find suspicious patterns you'd have to temporarily store a decompressed version in ram or disk to filter all relevant entires. With zstd-jsonl-filter you can perform pattern matching without ever needing to hold the entire decompress version.

To make these tasks easier you can adjust ``config.json`` with your needed parameters. You can define your own [regex terms](https://regex101.com/), or use ``^`` to match anything and decompress everything. If you need more substantial filtering you can simply expand this code to implement your own logic. Created files will follow the structure ``{output_path}{file_stem_without_extension}{output_suffix}{output_file_extension}``.
```yaml
{
    # your input files, make sure to use slashes not backslashes
    "input_path": "C:/Users/User/Documents/Destiny_PGCR/test/",
    # where to store the output
    "output_path": "C:/Users/User/Documents/Destiny_PGCR/test/",
    # if the output should be compressed, overwrites output_file_extension when set to true
    "output_as_zstd": true,
    # the zstd compression level from 1-22, 0 means the default of 3
    "output_zstd_compression": 0,
    # the suffix added to the filename e.g. 12000000000-12010000000_filtered
    "output_suffix": "_filtered",
    # the file extension to be used (zstd-jsonl-filter does not interpret of convert files!)
    # e.g. 12000000000-12010000000_filtered.zst
    "output_file_extension": ".zst",
    # the regex pattern matching applied to a single line
    "regex_pattern": "\",\"mode\":62,\"",
    # max number of threads used by rayon
    "max_threads": 0,
    # max buffer limit, here 100 MB
    "buffer_limit": 100000000
}
```
This examples filters all entires for ``","mode":62"`` (the mode identifier for Team Scorched) and writes them to a compressed file called ``{file}_filtered.zst``. If you wish to keep an uncompressed version set ``output_as_zstd`` to ``false``.

Without ``config.json`` zstd-jsonl-filter will default back to extracting every .zst archive in the current directory without filtering any lines.

# Donate

You can support this project by donating! specifically *bungo-pgcr*, *bungo-pgcr-10b*, *bungo-pgcr-11b* and *bungo-pgcr-14b*

# Why Rust?

I read somewhere that it's fast and I wanted to learn something new.

# Why Team Scorched?

[It is the single best game mode.](https://docs.google.com/document/d/1064ABA7NWypUyMI50-fxj2dT97gUU-Lvvdj5cwIrP0Q/)
