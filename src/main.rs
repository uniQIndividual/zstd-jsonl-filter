use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Error as IoError, Lines, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use zstd::stream::read::Decoder;

#[derive(Serialize, Deserialize)]
struct Config {
    input_path: String,
    output_path: String,
    output_suffix: String,
    output_file_extension: String,
    regex_pattern: String,
    max_threads: usize,
    buffer_limit: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let now = Instant::now();
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

    // progress bar
    let pb = ProgressBar::new(zstd_files.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} ({eta}) {msg}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );

    // Start a file operation for every available thread
    zstd_files.par_iter().for_each(|file_path| {
        let _ = read_lines(&file_path, &config);
        pb.inc(1);
    });

    pb.finish_with_message("All files processed.");
    println!("Total time elapsed: {:.2?}s", now.elapsed().as_secs());
    Ok(())
}

fn read_lines(file_path: &Path, config: &Config) -> std::io::Result<()> {
    // Operates on a single zstd file decompressing it line by line

    // In in-memory buffer for storing matching lines
    let mut buffer: Vec<u8> = Vec::with_capacity(config.buffer_limit);

    // Track the last matching line to avoid trailing newline
    let mut last_matching_line: Option<String> = None;

    let pattern = Regex::new(&config.regex_pattern.as_str()).unwrap();
    let output_file_path =
        generate_output_filename(&file_path.to_string_lossy().to_string(), &config);

    if Path::new(&output_file_path).exists() {
        println!(
            "Output file '{}' already exists. Skipping...",
            output_file_path
        );
        return Ok(());
    }

    let output_file = File::create(output_file_path)?;

    let mut writer = BufWriter::new(output_file);

    if let Ok(lines) = decompress_lines(file_path) {
        for line in lines {
            if let Ok(line) = line {
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
                        flush_buffer(&mut buffer, &mut writer).unwrap();
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

    // Flush any remaining data in the buffer to the output file
    if !buffer.is_empty() {
        flush_buffer(&mut buffer, &mut writer)?;
    }

    // Write the last matching line without an extra newline
    if let Some(last_line) = last_matching_line {
        writer.write_all(last_line.as_bytes())?;
    }

    // Ensure the writer flushes remaining buffered data
    writer.flush()?;

    Ok(())
}

// Using https://stackoverflow.com/questions/77304382/how-to-decode-and-read-a-zstd-file-in-rust
fn decompress_lines(
    filename: &Path,
) -> Result<Lines<BufReader<Decoder<'static, BufReader<File>>>>, IoError>
where
{
    let file = File::open(filename)?;
    let decoder = Decoder::new(file)?;
    Ok(BufReader::new(decoder).lines())
}

fn flush_buffer(buffer: &mut Vec<u8>, writer: &mut BufWriter<File>) -> std::io::Result<()> {
    writer.write_all(&buffer)?; // Write the buffer content to the file
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
    format!(
        "{}{file_stem_without_extension}{}{}",
        config.output_path, config.output_suffix, config.output_file_extension
    )
}

#[derive(Serialize, Deserialize, Debug)]
struct ConfigFile {
    input_path: Option<String>,
    output_path: Option<String>,
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
    let output_suffix = String::from("_filtered"); // suffix for your output file
    let output_file_extension = String::from(".jsonl"); // suffix for your output file
    let regex_pattern = String::from(r#"^"#); // match everything
    let max_threads = 0; // max number of threads rayon spawn, 0 means no limit
    let buffer_limit = 100 * 1024 * 1024; // the buffer size after which data is written to disk, here: 100MB

    if let Ok(file) = File::open(config_path) {
        let reader = BufReader::new(file);
        if let Ok(config) = serde_json::from_reader::<_, ConfigFile>(reader) {
            return Config {
                input_path: config.input_path.unwrap_or(input_path),
                output_path: config.output_path.unwrap_or(output_path),
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
        output_suffix: output_suffix,
        output_file_extension: output_file_extension,
        regex_pattern: regex_pattern,
        max_threads: max_threads,
        buffer_limit: buffer_limit,
    }
}
