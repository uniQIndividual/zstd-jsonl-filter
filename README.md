# What is zstd-jsonl-filter?

zstd-jsonl-filter takes advantage of zstd's incredibly high decompression speed to effortlessly filter terabytes of data faster than you could read it from disk. It does so with minimal memory usage and I/O writes, holding only the relevant file section in memory. The only limitation is that your data must be interpretable line-by-line (e.g. jsonl, log and csv). It uses zstd's stream decoder and rayon's parallelism to very efficiently decompress and filter gigantic datasets in memory by streaming several files at once.

<br>
<p align="center">
<img src="https://github.com/user-attachments/assets/6a37abc9-2c06-4f69-aa75-96f2c732c2a3" width="80%"/>
</p>


<br>
<br>


# Is zstd-jsonl-filter the right tool for you?

- You have multiple zstd archives which must be can be interpretable line-by-line\
(any newline separated file should work, although I mainly use it for JSON Lines)
- The uncompressed data is infeasible to store in ram or on disk
- You want to process data faster than you could read it from your source

<br>
<p align="center">
<img src=".\assets\explanation.png" width="80%"/>
</p>
<br>

# How to use zstd-jsonl-filter

Say you had a couple billion ~~Destiny 2 PGCR sessions~~ log entires from your fortune 500 employer (please don't actually use it for that) which are neatly separated in smaller chunks and compressed with zstd. If you wanted to extract only relevant entries  ~~( e.g. all Team Scorched matches)~~ you'd have to temporarily store a decompressed version in ram or disk to filter them. With zstd-jsonl-filter you can perform this pattern matching without ever needing to hold the entire file.

1. Build zstd-jsonl-filter or download [the latest release](https://github.com/uniQIndividual/zstd-jsonl-filter/releases/latest)
2. Open ``config.toml`` and set your parameters (see [this section](https://github.com/uniQIndividual/zstd-jsonl-filter#options) for more details) and in particular ``pattern`` which is the regex term that is matched against a line from the decompressed files. When a pattern matches the entire line is included in the output, otherwise it is discarded.
3. Make sure your data is in the following format (i.e. no tar): 
```
input_path/
├───12000000000-12010000000.jsonl.zst 
│   └───12000000000-12010000000.jsonl
├───12010000000-12020000000.jsonl.zst
│   └───12010000000-12020000000.jsonl
.
.
.
```
The names are irrelevant, but you need to make sure your filetype and patterns are applicable line-by-line. zstd-jsonl-filter is not going to stop you from e.g. reading a .tar but you may get unexpected results.

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
| ``--input`` | The path where your .zst files are located.<br>Both ``/`` slashes and ``\`` backslashes work. It is also possible to point to a single file. | ``./`` current folder
| ``--output`` | Where the output files should be stored. | ``./`` current folder. |
| ``--zstd`` | Whether the output should be stored as a compressed .zst file. | ``false`` no zstd compression. |
| ``--compression-level`` | The zstd compression level from 1 (fastest) to 22 (smallest) | ``0`` use zstd default |
| ``--suffix`` | Name to be appended to output files. Will generate e.g.<br>``12000000000-12010000000_filtered.zst``. | ``_filtered`` |
| ``--file-extension`` | If you want to replace the file extension for output files. You can usually leave this empty, otherwise do not include a dot i.e. ``jsonl`` | ``""`` |
| ``--pattern`` | The regex pattern to be applied line-by-line. A match means the line will be included in the output. Keep in mind that regex terms with special characters need to be escaped properly. Look-around are not supported in favor of a worst case performance of [O(m * n)](https://docs.rs/regex/latest/regex/). | ``^`` matches everything |
| ``--threads`` | The maximum number of threads used by rayon. Since each thread reads from one file, changing this number also affects I/O.  | ``0`` unlimited |
| ``--buffer`` | The maximum buffer per thread before matches lines are written to disk. | ``4096`` 4KiB |
|``--quiet``| Displays only the current progress and error messages | ``false`` |
|``--no-write``| Does not write any output files. Can be used for testing or line counting. It will check for already existing output files so you can spot conflicts before running long tasks. | ``false`` |

## Practical examples

### Using config.toml
```toml
# Input Parameters
input = 'C:/Users/User/Documents/Destiny_PGCR/bungo-pgcr-12b/'

# Output Parameters
output = 'C:\Users\User\Documents\Destiny_PGCR\test' # Backslashes also work
suffix = "_scorch"
file_extension = "" # You can use this to change the output file extension e.g. from {file}.jsonl.zst to {file}_filtered.txt
no_write = false

# In this example we the output to be uncompressed thus we set zstd to false
zstd = false
compression_level = 0

# Regex Filter
pattern = '","mode":62,"' # Make sure to properly escape if needed, look-arounds are not supported

# Performance
threads = 0
buffer = 4096
quiet = false
```
This finds all Team Scorched matches in Destiny PGCRs by identifying ``","mode":62,"``. Make sure your source files are well defined and your regex terms are robust enough. Given a file named ``{file}.jsonl.zst`` it then writes the output to uncompressed files called ``{file}_scorch.jsonl``. 

### Using arguments
```powershell
.\zstd-jsonl-filter.exe --input "\\10.0.0.2\D2_PGCR\bungo-pgcr-12b" --output "C:\Users\User\Documents\Destiny_PGCR\test" --zstd --compression-level 14 --threads 2 --pattern '","mode":62,"' --quiet
```
This examples also finds all Team Scorched matches with ``","mode":62,"`` in the network share ``\\10.0.0.2\D2_PGCR\bungo-pgcr-12b`` and writes the output to compressed files called ``{file}_filtered.zst``. It is restricted to only ``2`` threads and with ``--quiet`` it will only display the current progress and important error messages.

Without arguments or ``config.toml`` zstd-jsonl-filter will default back to extracting every .zst archive in the current directory without filtering any lines.

### Only counting with --no-write

```powershell
.\zstd-jsonl-filter.exe --no-write --input "\\10.0.0.2\D2_PGCR\bungo-pgcr-12b"  --output "E:\already occupied path\" --threads 2 --pattern '("teams":\[\{("\w*":[\w\."]*,)*"score":6\d,("\w*":[\w\."]*,*)*\},\{("\w*":[\w\.]*,)*"score":0,)|("teams":\[\{("\w*":[\w\."]*,)*"score":0,("\w*":[\w\."]*,*)*\},\{("\w*":[\w\.]*,)*"score":6\d,)'
```

This is really useful if you just want to see how often your term occurs or if you want to test your regex before committing to a potentially long write. I would recommend to not use the ``--quiet`` flag so  shows you all potential issues that (would) occur if were to run it again without ``--no-write``. This includes e.g. already existing files in the output path which zstd-jsonl-filter will not overwrite. This is why ``--output`` will still affect the outcome.

If you're curious about the regex: It *should* return all matches won 60:0 or 0:60. As it turns out though, there can be more than 2 teams when a player was not assigned to one. These kinds of quick checks is what ``--no-write`` is very useful for.


# Performance

Using a Ryzen 9 3900x and a test set of 200 GB zstd archives stored on NVMe drives, zstd-jsonl-filter is pretty much always CPU bound with average read speeds of ~600 MB/s from disk and ~5 GB/s of uncompressed data processed in memory. These operation took on average 8 min and processed 3 TB of uncompressed data.

### CPU

zstd-jsonl-filter uses rayon for parallelization across files. This means it decompresses one file per thread at once. If you have less files than suggested threads, you will not see any speedup. You can set the maximum number of threads ``max_threads``.

Matching is performed via regex because it was significantly faster than parsing each JSON. It uses the regex crate implementation which runs in linear time. This depends on what you have to work with, adjust as needed.

### Memory

Stream decompression drastically reduces the memory usage. For my test set the usage sits around 200 MB. However the exact value depends on several factors like the size of a single line in your decompressed file and if you write to zstd compressed output.

### I/O

Although zstd-jsonl-filter is usually CPU bound when reading from an NVMe, that can quickly change for other sources. Reading from network storage or hard drives can be a bottleneck. You can set ``max_threads`` to change the number of simultaneous file operations if that impacts your source medium.

Write speeds depend on how many entries are filtered, your storage speed and if compression is used.
By default zstd-jsonl-filter writes to a 4 KiB buffer which you can adjust as needed.

# Donate

You can support this project by donating! specifically *bungo-pgcr*, *bungo-pgcr-10b*, *bungo-pgcr-11b* and *bungo-pgcr-14b*

# Why Rust?

I read somewhere that it's fast and I wanted to learn something new. While I didn't intend to create a public tool, I uploaded this just in case there's another person out there who, for whatever reason, needs to efficiently go through terabytes of zstd compressed data. 

# Why Team Scorched?

[It is the single best game mode.](https://docs.google.com/document/d/1064ABA7NWypUyMI50-fxj2dT97gUU-Lvvdj5cwIrP0Q/)
