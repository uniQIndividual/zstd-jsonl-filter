# What is zstd-jsonl-filter?

zstd-jsonl-filter can effortlessly filter terabytes of data with minimal memory usage and I/O writes. The only limitation is that your data must be interpretable line-by-line. It uses zstd's stream decoder and rayon's parallelism to very efficiently decompress and filter gigantic datasets in memory by streaming several files at once.


<p align="center">
<img src=".\assets\explanation.png" width="80%"/>
</p>

\
\
To be clear: zstd-jsonl-filter is mainly useful if

- you have multiple zstd archives
- they each contain a file that can be interpreted line-by-line\
(any newline separated file should work, although I only tested JSON Lines)
- the uncompressed data is infeasible to store in ram or on disk


# How to use zstd-jsonl-filter

Say you had a couple billion ~~Destiny 2 PGCR sessions~~ log entires from your fortune 500 employer (please don't actually use it for that) which are neatly separated in smaller chunks and compressed with zstd. If you wanted to extract only relevant entries  ~~( e.g. all Team Scorched matches)~~ you'd have to temporarily store a decompressed version in ram or disk to filter them. With zstd-jsonl-filter you can perform these pattern matching without ever needing to hold the entire file.

1. Build zstd-jsonl-filter or download [the latest release](https://github.com/uniQIndividual/zstd-jsonl-filter/releases/latest)
2. Open ``config.json`` and set your parameters (see [this section](https://github.com/uniQIndividual/zstd-jsonl-filter#what-this-for) for more details)
3. Make sure your data is in the following format (i.e. no tar): 
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
The names are irrelevant, but zstd-jsonl-filter does not support multiple files within an archive.

4. Run zstd-jsonl-filter


# Parameters in config.json

To make these tasks easier you can adjust ``config.json`` with your needed parameters. You can define your own [regex terms](https://regex101.com/), or use ``^`` to match anything and decompress everything. If you need more substantial filtering you can simply expand this code to implement your own logic. Created files will follow the structure ``{output_path}original_filename_without_extension{output_suffix}{output_file_extension}``.

## Here is a full list:

| Parameter      | Description      | Default |
| ------------- | ------------- | ------------- |
| input_path | The path where your .zst files are located.<br>Make sure to use ``/`` slashes not ``\`` backslashes! | ``./``
| output_path | Where the output should be stored | ``./`` current directory |
| output_as_zstd | Whether the output should be stored as a compressed .zst file. ``true`` will overwrite ``output_file_extension`` | ``false`` no zstd compression |
| output_zstd_compression | The zstd compression level from 1 (fastest) - 22 (smallest) | ``0`` normal |
| output_suffix | Name to be appended to output files. Will generate e.g.<br>``12000000000-12010000000_filtered.zst`` | ``_filtered`` |
| output_file_extension | The file extension for extracted files. Will be ignored if ``output_as_zstd`` is set to ``true`` | ``.jsonl`` |
| regex_pattern | The regex pattern to be applied line-by-line. A match means the line will be included in the output. The regex needs to be escaped for JSON e.g. ``"\",\"mode\":62,\""`` | ``^`` matches everything |
| max_threads | The maximum number of threads used by rayon. Since each thread reads from one file, changing this number also affects I/O.  | ``0`` unlimited |
| buffer_limit | The maximum buffer per thread before matches lines are written to disk. That means e.g. 24 threads x 100 MB buffer means up to 2.4 GB of memory can be used for buffering. | ``100000000`` 100 MB |

## Practical example

```yaml
{
    "input_path": "C:/Users/User/Documents/Destiny_PGCR/test/",
    "output_path": "C:/Users/User/Documents/Destiny_PGCR/test/",
    "output_as_zstd": true,
    "output_zstd_compression": 0,
    "output_suffix": "_filtered",
    "output_file_extension": ".zst",
    "regex_pattern": "\",\"mode\":62,\"",
    "max_threads": 0,
    "buffer_limit": 100000000
}
```
This examples filters all entires for ``","mode":62"`` (in my use case the mode identifier for "Team Scorched") and writes them to a compressed file called ``{file}_filtered.zst``. If you wish to keep an uncompressed version set ``output_as_zstd`` to ``false``.

Without ``config.json`` zstd-jsonl-filter will default back to extracting every .zst archive in the current directory without filtering any lines.


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

# Donate

You can support this project by donating! specifically *bungo-pgcr*, *bungo-pgcr-10b*, *bungo-pgcr-11b* and *bungo-pgcr-14b*

# Why Rust?

I read somewhere that it's fast and I wanted to learn something new. While I didn't intend to create a public tool, I uploaded this just in case there's another person out there who, for whatever reason, needs to efficiently go through terabytes of zstd compressed data. 

# Why Team Scorched?

[It is the single best game mode.](https://docs.google.com/document/d/1064ABA7NWypUyMI50-fxj2dT97gUU-Lvvdj5cwIrP0Q/)
