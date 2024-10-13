use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Error as IoError, Lines, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use zstd::compression_level_range;
use zstd::stream::read::Decoder;
use zstd::stream::write::Encoder;

#[derive(Serialize, Deserialize)]
struct Config {
    input_path: String,
    output_path: String,
    output_as_zstd: bool,
    output_zstd_compression: i32,
    output_suffix: String,
    output_file_extension: String,
    regex_pattern: String,
    max_threads: usize,
    buffer_limit: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let start_time = Instant::now();
    // Attempt to read the config, otherwise use fallbacks
    let mut config = load_config("config.json");

    rayon::ThreadPoolBuilder::new()
        .num_threads(config.max_threads)
        .build_global()
        .unwrap();

    if !config.input_path.ends_with('/') {
        config.input_path.push('/');
    }
    if !PathBuf::from(&config.input_path).is_dir() {
        eprintln!(
            "Error: The input path '{:?}' is not a valid directory.",
            &config.input_path
        );
        std::process::exit(1);
    }

    if !config.output_path.ends_with('/') {
        config.output_path.push('/');
    }
    if !PathBuf::from(&config.output_path).is_dir() {
        eprintln!(
            "Error: The output path '{:?}' is not a valid directory.",
            &config.output_path
        );
        std::process::exit(1);
    }

    // Find all .zst files in input_path
    let zstd_files: Vec<_> = fs::read_dir(&config.input_path)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("zst") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    let total_files = zstd_files.len();
    let display_limit = 5;
    println!("Found {} .zstd files:", total_files);
    for file in zstd_files.iter().take(display_limit) {
        if let Some(file_name) = file.file_name() {
            println!("- {:?}", file_name);
        }
    }
    if total_files > display_limit {
        println!("...");
    }

    // Shared total counter for the total decompressed size
    let total_decompressed_size = Arc::new(AtomicUsize::new(0));

    // progress bar
    let pb = ProgressBar::new(zstd_files.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta}) {msg}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    // Start a file operation for every available thread
    zstd_files.par_iter().for_each(|file_path| {
        let _ = read_lines(
            &file_path,
            &config,
            &pb,
            Arc::clone(&total_decompressed_size),
            start_time,
        );
        pb.inc(1);
    });

    //pb.finish_with_message("All files processed.");
    pb.finish();
    println!("All files processed.");
    /*
    let final_size = total_decompressed_size.load(Ordering::SeqCst);
    println!(
        "Total time elapsed: {:.2?}s",
        start_time.elapsed().as_secs()
    );
    println!("Total size processed: {}", HumanBytes(final_size as u64));
    println!(
        "Average processing speed: {}/s",
        HumanBytes(final_size as u64 / start_time.elapsed().as_secs())
    );*/
    Ok(())
}

fn read_lines(
    file_path: &Path,
    config: &Config,
    pb: &ProgressBar,
    total_decompressed_size: Arc<AtomicUsize>,
    start_time: Instant,
) -> std::io::Result<()> {
    // Operates on a single zstd file decompressing it line by line

    // Skip if input file is empty
    if let Ok(metadata) = fs::metadata(file_path) {
        if metadata.len() == 0 {
            pb.suspend(|| {
                println!(
                    "Skipping empty file: {:?}",
                    file_path.file_name().unwrap_or_default()
                );
            });
            return Ok(());
        }
    } else {
        pb.suspend(|| {
            eprintln!("Failed to get metadata for: {:?}", file_path);
        });
        return Ok(());
    }

    let output_file_path =
        generate_output_filename(&file_path.to_string_lossy().to_string(), &config);

    // Skip already existing existing files
    if Path::new(&output_file_path).exists() {
        pb.suspend(|| {
            println!(
                "Skipping existing output file {:?}",
                Path::new(&output_file_path).file_name().unwrap_or_default()
            );
        });
        return Ok(());
    }

    // Verify if the file is not a Zstd archive containing a TAR
    if let Err(err) = verify_zstd_without_tar(file_path) {
        pb.suspend(|| eprintln!("{}", err));
        return Ok(());
    }

    // In in-memory buffer for storing matching lines
    let mut buffer: Vec<u8> = Vec::with_capacity(config.buffer_limit);

    // Track the last matching line to avoid trailing newline
    let mut last_matching_line: Option<String> = None;

    let pattern = Regex::new(&config.regex_pattern.as_str()).unwrap();

    let output_file = File::create(output_file_path)?;
    let mut writer = BufWriter::new(output_file);

    // Function to handle output either (compressed or uncompressed)
    let mut write_to_output = |data: &[u8]| -> std::io::Result<()> {
        if config.output_as_zstd {
            // Use a ZSTD encoder to write compressed data
            let mut encoder = Encoder::new(writer.by_ref(), config.output_zstd_compression)?;
            encoder.write_all(data)?;
            encoder.finish()?;
        } else {
            // Write uncompressed data directly
            writer.write_all(data)?;
        }
        Ok(())
    };

    // Using https://stackoverflow.com/questions/77304382/how-to-decode-and-read-a-zstd-file-in-rust
    fn a(
        reader: BufReader<Decoder<'static, BufReader<File>>>,
    ) -> Result<Lines<BufReader<Decoder<'static, BufReader<File>>>>, IoError> {
        Ok(reader.lines())
    }
    let file = File::open(file_path)?;

    let decoder = Decoder::new(file)?;
    let reader = BufReader::new(decoder);

    // Measure the size of decompressed data
    let mut decompressed_size = 0;

    // : Result<Lines<BufReader<Decoder<'static, BufReader<File>>>>, IoError>
    if let Ok(lines) = a(reader) {
        for line in lines {
            if let Ok(line) = line {
                decompressed_size += line.len();

                // Test regex pattern
                // This is the place to add new line-by-line logic
                if pattern.is_match(&line) {
                    // Write matches to buffer to decrease the number individual disk writes
                    if let Some(last_line) = last_matching_line.take() {
                        let line_bytes = format!("{}\n", last_line).into_bytes(); // Convert the line to bytes
                        buffer.extend_from_slice(&line_bytes); // Append to the buffer
                    }

                    // Store the current matching line as the last line
                    last_matching_line = Some(line.to_string());

                    // If the buffer size exceeds the limit, flush it to the output file
                    if buffer.len() >= config.buffer_limit {
                        flush_buffer(&mut buffer, &mut write_to_output).unwrap();
                    }
                }
            } else {
                panic!(
                    "Error when decompressing {} with the error: {line:?}\n\
                Make sure your zstd archive includes a single jsonl file.",
                    &file_path.to_string_lossy().to_string()
                );
            }
        }
    }

    total_decompressed_size.fetch_add(decompressed_size, Ordering::SeqCst);
    let current_size = total_decompressed_size.load(Ordering::SeqCst);
    let mut start_time = start_time.elapsed().as_secs();
    if start_time == 0 {
        start_time += 1;
    }
    pb.set_message(format!(
        "@ {}/s, Total: {}",
        HumanBytes(current_size as u64 / start_time),
        HumanBytes(current_size as u64)
    ));

    // Flush any remaining data in the buffer to the output file
    if !buffer.is_empty() {
        flush_buffer(&mut buffer, &mut write_to_output)?;
    }

    // Write the last matching line without an extra newline
    if let Some(last_line) = last_matching_line {
        write_to_output(last_line.as_bytes())?;
    }

    Ok(())
}

fn flush_buffer(
    buffer: &mut Vec<u8>,
    write_to_output: &mut impl FnMut(&[u8]) -> std::io::Result<()>,
) -> std::io::Result<()> {
    write_to_output(&buffer)?; // Write the buffer content to the output
    buffer.clear(); // Clear the buffer after writing
    Ok(())
}

fn generate_output_filename(input_file_path: &str, config: &Config) -> String {
    let path = Path::new(input_file_path);

    // Strip the ".jsonl.zst" extension
    let input_stem = path.file_stem().unwrap().to_string_lossy().to_string(); // e.g. "13030000000-13040000000.jsonl"
    let file_stem_without_extension = Path::new(&input_stem)
        .file_stem()
        .unwrap()
        .to_string_lossy(); // Get only base file name if applicable, i.e. "13030000000-13040000000"
    if config.output_as_zstd {
        // ignore output_file_extension and set .zst
        format!(
            "{}{file_stem_without_extension}{}.zst",
            config.output_path, config.output_suffix
        )
    } else {
        format!(
            "{}{file_stem_without_extension}{}{}",
            config.output_path, config.output_suffix, config.output_file_extension
        )
    }
}

// Verify that the file is a valid zstd file and contains no tar
fn verify_zstd_without_tar(file_path: &Path) -> Result<(), String> {
    let mut file = File::open(file_path).map_err(|e| format!("Failed to open file: {}", e))?;

    // Read the first few bytes to detect Zstd magic number
    let mut magic_bytes = [0u8; 4];
    file.read_exact(&mut magic_bytes).map_err(|_| {
        format!(
            "Skipped not valid zstd {:?}",
            file_path.file_name().unwrap_or_default()
        )
    })?;

    // Check if the magic bytes match Zstd's magic number
    if magic_bytes == [0x28, 0xB5, 0x2F, 0xFD] {
        // It's a Zstd archive; attempt to decompress it
        let _ = Decoder::new(file).map_err(|_| {
            format!(
                "Failed to decode zstd for {:?}",
                file_path.file_name().unwrap_or_default()
            )
        })?;
    } else {
        return Err(format!(
            "Skipped not valid zstd {:?}",
            file_path.file_name().unwrap_or_default()
        ));
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct ConfigFile {
    input_path: Option<String>,
    output_path: Option<String>,
    output_as_zstd: Option<bool>,
    output_zstd_compression: Option<i32>,
    output_suffix: Option<String>,
    output_file_extension: Option<String>,
    regex_pattern: Option<String>,
    max_threads: Option<usize>,
    buffer_limit: Option<usize>,
}

fn load_config(config_path: &str) -> Config {
    // Fallback values if no config file was found
    let input_path = String::from("./"); // directory where to search for zstd files
    let output_path = String::from("./"); // directory where to write files to
    let output_as_zstd = false; // by default extract everything
    let output_zstd_compression = 0; // zstd compression level between 1-22, 0 means the default of 3
    let output_suffix = String::from("_filtered"); // suffix for your output file
    let output_file_extension = String::from(".jsonl"); // suffix for your output file
    let regex_pattern = String::from(r#"^"#); // match everything
    let max_threads = 0; // max number of threads rayon spawn, 0 means no limit
    let buffer_limit = 100 * 1024 * 1024; // the buffer size after which data is written to disk, here: 100MB

    // Verify valid zstd compression level range
    fn verify_compression_level(level: i32) -> i32 {
        if compression_level_range().contains(&level) {
            level
        } else {
            0
        }
    }

    if let Ok(file) = File::open(config_path) {
        let reader = BufReader::new(file);
        if let Ok(config) = serde_json::from_reader::<_, ConfigFile>(reader) {
            return Config {
                input_path: config.input_path.unwrap_or(input_path),
                output_path: config.output_path.unwrap_or(output_path),
                output_as_zstd: config.output_as_zstd.unwrap_or(output_as_zstd),
                output_zstd_compression: verify_compression_level(
                    config
                        .output_zstd_compression
                        .unwrap_or(output_zstd_compression),
                ),
                output_suffix: config.output_suffix.unwrap_or(output_suffix),
                output_file_extension: config
                    .output_file_extension
                    .unwrap_or(output_file_extension),
                regex_pattern: config.regex_pattern.unwrap_or(regex_pattern),
                max_threads: config.max_threads.unwrap_or(max_threads),
                buffer_limit: config.buffer_limit.unwrap_or(buffer_limit),
            };
        } else {
            println!("Failed to parse config file. Using default values.");
        }
    } else {
        println!(
            "Config file '{}' not found. Using default values.",
            config_path
        );
    }
    Config {
        input_path: input_path,
        output_path: output_path,
        output_as_zstd: output_as_zstd,
        output_zstd_compression: output_zstd_compression,
        output_suffix: output_suffix,
        output_file_extension: output_file_extension,
        regex_pattern: regex_pattern,
        max_threads: max_threads,
        buffer_limit: buffer_limit,
    }
}
