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
2. Open ``config.json`` and set your parameters (see [this section](https://github.com/uniQIndividual/zstd-jsonl-filter#options) for more details)
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


# Options

You can launch zstd-jsonl-filter

- with command line parameters (will take priority over the config file)
- with the included config.toml file

Also see [practical examples](https://github.com/uniQIndividual/zstd-jsonl-filter#practical-example)

You usually want to include a [regex term](https://regex101.com/) to filter the output and make zstd-jsonl-filter more than a decoder.
You can supply it with ``--pattern`` or in ``config.toml``. If you need more substantial filtering you can fork this code to implement your own logic. I might look into ways to make more powerful filtering in the future.

Created files will follow the structure ``{output_path}original_filename_without_extension{output_suffix}{output_file_extension}``.

## All parameters

| Parameter      | Description      | Default |
| ------------- | ------------- | ------------- |
| ``--config`` | Point zstd-jsonl-filter to the config file. | ``config.toml`` in the same folder |
| ``--input`` | The path where your .zst files are located.<br>Both ``/`` slashes and ``\`` backslashes work. | ``./`` current folder
| ``--output`` | Where the output files should be stored. | ``./`` current folder. |
| ``--zstd`` | Whether the output should be stored as a compressed .zst file. ``true`` will overwrite ``output_file_extension``. | ``false`` no zstd compression. |
| ``--compression-level`` | The zstd compression level from 1 (fastest) to 22 (smallest) | ``0`` use zstd default |
| ``--suffix`` | Name to be appended to output files. Will generate e.g.<br>``12000000000-12010000000_filtered.zst``. | ``_filtered`` |
| ``--file-extension`` | The file extension for extracted files. Will be ignored if ``output_as_zstd`` is set to ``true``. | ``.jsonl`` |
| ``--pattern`` | The regex pattern to be applied line-by-line. A match means the line will be included in the output. Keep in mind that regex terms with special characters need to be escaped properly. | ``^`` matches everything |
| ``--threads`` | The maximum number of threads used by rayon. Since each thread reads from one file, changing this number also affects I/O.  | ``0`` unlimited |
| ``--buffer`` | The maximum buffer per thread before matches lines are written to disk. That means e.g. 24 threads x 100 MB buffer means up to 2.4 GB of memory can be used for buffering. | ``100000000`` 100 MB |
|``--quiet``| Displays only the current progress and error messages | ``false`` |

## Practical examples

### Using config.toml
```toml
# Input Parameters
input = 'C:/Users/User/Documents/Destiny_PGCR/bungo-pgcr-12b/'

# Output Parameters
output = 'C:\Users\User\Documents\Destiny_PGCR\test' # backslashes also work
zstd = false
compression_level = 14
suffix = "_scorch"
file_extension = ".jsonl"

# Regex Filter
pattern = '","mode":62,"' # make sure to properly escape if needed

# Performance
threads = 0
buffer = 100000000
quiet = false
```
This finds all Team Scorched matches in Destiny PGCRs by identifying ``","mode":62,"``. Make sure your source files are well defined and your regex terms are robust enough. It then writes them to uncompressed files called ``{file}_scorch.jsonl``. 

### Using arguments
```powershell
.\zstd-jsonl-filter.exe --input \\10.0.0.2\D2_PGCR\bungo-pgcr-12b --output C:\Users\User\Documents\Destiny_PGCR\test --zstd --compression-level 14 --threads 2 --pattern '","mode":62,"' --quiet
```
This examples also finds all Team Scorched matches and writes them to compressed files called ``{file}_filtered.zst``. It is restricted to only 2 threads and only displays the current progress and important error messages.

Without arguments or ``config.toml`` zstd-jsonl-filter will default back to extracting every .zst archive in the current directory without filtering any lines.


# Performance

Using a Ryzen 9 3900x and a test set of 200 GB zstd archives stored on NVMe drives, zstd-jsonl-filter was entirely CPU bound with average read speeds of ~430 MB/s from disk and ~6.8 GB/s of uncompressed data processed in memory. These operation took on average 8 min and processed 3 TB of uncompressed data.

### CPU

zstd-jsonl-filter uses rayon for parallelization across files. This means it decompresses one file per thread at once. If you have less files than suggested threads, you will not see any speedup. You can set the maximum number of threads ``max_threads``.

Matching is performed via regex because it was significantly faster than parsing each JSON. This depends on what you have to work with, adjust as needed.

### Memory

Stream decompression drastically reduces the memory usage, however the number of spawned threads and their respective write buffer request 100 MB each by default. These could add up to a few gigabytes though in practice you can stay far below that for strong filter ratios. It depends on how large these buffers grow simultaneously. You can change all these parameters in the config.

### I/O

Although zstd-jsonl-filter is usually CPU bound when reading from an NVMe, that can quickly change for other sources. Reading from network storage or hard drives can be a bottleneck. You can set ``max_threads`` to change the number of simultaneous file operations if that impacts your source medium.

Write speeds depend on how many entries are filtered, your storage speed and if compression is used.
By default zstd-jsonl-filter writes to a 100 MB buffer which you can adjust as needed.

# Donate

You can support this project by donating! specifically *bungo-pgcr*, *bungo-pgcr-10b*, *bungo-pgcr-11b* and *bungo-pgcr-14b*

# Why Rust?

I read somewhere that it's fast and I wanted to learn something new. While I didn't intend to create a public tool, I uploaded this just in case there's another person out there who, for whatever reason, needs to efficiently go through terabytes of zstd compressed data. 

# Why Team Scorched?

[It is the single best game mode.](https://docs.google.com/document/d/1064ABA7NWypUyMI50-fxj2dT97gUU-Lvvdj5cwIrP0Q/)
