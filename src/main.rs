use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Error as IoError, Lines, Read, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use clap::Parser;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use zstd::compression_level_range;
use zstd::stream::read::Decoder;
use zstd::stream::write::Encoder;

fn main() -> Result<(), Box<dyn Error>> {
    let start_time = Instant::now();
    // Set up config parameters from cli, the config file and fallback values
    let mut config = set_config();

    rayon::ThreadPoolBuilder::new()
        .num_threads(config.threads)
        .build_global()
        .unwrap();

    if !config.input.ends_with('/') {
        config.input.push('/');
    }
    if !PathBuf::from(&config.input).is_dir() {
        eprintln!(
            "Error: The input path '{:?}' is not a valid directory.",
            &config.input
        );
        std::process::exit(1);
    }

    if !config.output.ends_with('/') {
        config.output.push('/');
    }
    if !PathBuf::from(&config.output).is_dir() {
        eprintln!(
            "Error: The output path '{:?}' is not a valid directory.",
            &config.output
        );
        std::process::exit(1);
    }

    // Find all .zst files in input_path
    let zstd_files: Vec<_> = fs::read_dir(&config.input)?
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
    print_if_not_quiet(config.quiet, &format!("Found {} .zstd files:", total_files));
    for file in zstd_files.iter().take(display_limit) {
        if let Some(file_name) = file.file_name() {
            print_if_not_quiet(config.quiet, &format!("- {:?}", file_name));
        }
    }
    if total_files > display_limit {
        print_if_not_quiet(config.quiet, &format!("..."));
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
                print_if_not_quiet(
                    config.quiet,
                    &format!(
                        "Skipping empty file: {:?}",
                        file_path.file_name().unwrap_or_default()
                    ),
                );
            });
            return Ok(());
        }
    } else {
        pb.suspend(|| {
            print_if_not_quiet(
                config.quiet,
                &format!("Failed to get metadata for: {:?}", file_path),
            );
        });
        return Ok(());
    }

    let output_file_path =
        generate_output_filename(&file_path.to_string_lossy().to_string(), &config);

    // Skip already existing existing files
    if Path::new(&output_file_path).exists() {
        pb.suspend(|| {
            print_if_not_quiet(
                config.quiet,
                &format!(
                    "Skipping existing output file {:?}",
                    Path::new(&output_file_path).file_name().unwrap_or_default()
                ),
            );
        });
        return Ok(());
    }

    // Verify if the file is not a Zstd archive containing a TAR
    if let Err(err) = verify_zstd_without_tar(file_path) {
        pb.suspend(|| print_if_not_quiet(config.quiet, &format!("{}", err)));
        return Ok(());
    }

    // In in-memory buffer for storing matching lines
    let mut buffer: Vec<u8> = Vec::with_capacity(config.buffer);

    // Track the last matching line to avoid trailing newline
    let mut last_matching_line: Option<String> = None;

    let pattern = Regex::new(&config.pattern.as_str()).unwrap();

    let output_file = File::create(output_file_path)?;
    let mut writer = BufWriter::new(output_file);

    // Function to handle output either (compressed or uncompressed)
    let mut write_to_output = |data: &[u8]| -> std::io::Result<()> {
        if config.zstd {
            // Use a ZSTD encoder to write compressed data
            let mut encoder = Encoder::new(writer.by_ref(), config.compression_level)?;
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
                    if buffer.len() >= config.buffer {
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
    if config.zstd {
        // ignore output_file_extension and set .zst
        format!(
            "{}{file_stem_without_extension}{}.zst",
            config.output, config.suffix
        )
    } else {
        format!(
            "{}{file_stem_without_extension}{}{}",
            config.output, config.suffix, config.file_extension
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

/// Command line argument structure
#[derive(Parser, Debug)]
#[command(author, version, about = "Rust Configuration Demo", long_about = None)]
struct Cli {
    #[arg(long = "input")]
    input: Option<String>,
    #[arg(long = "output")]
    output: Option<String>,
    #[arg(long = "zstd")]
    zstd: bool,
    #[arg(long = "compression-level")]
    compression_level: Option<i32>,
    #[arg(long = "suffix")]
    suffix: Option<String>,
    #[arg(long = "file-extension")]
    file_extension: Option<String>,
    #[arg(long = "pattern")]
    pattern: Option<String>,
    #[arg(long = "threads")]
    threads: Option<usize>,
    #[arg(long = "buffer")]
    buffer: Option<usize>,
    #[arg(long = "quiet")]
    quiet: bool,
    #[arg(long = "config", default_value = "config.toml")]
    config: String,
}

// Internal and config.toml structure
#[derive(Serialize, Deserialize)]
struct Config {
    input: String,
    output: String,
    zstd: bool,
    compression_level: i32,
    suffix: String,
    file_extension: String,
    pattern: String,
    threads: usize,
    buffer: usize,
    quiet: bool,
}

fn validate_regex(pattern: &str) -> Result<Regex, String> {
    Regex::new(pattern).map_err(|e| format!("Invalid regex: {}", e))
}

/// Check --quiet before printing
fn print_if_not_quiet(quiet: bool, message: &str) {
    if !quiet {
        println!("{}", message);
    }
}

fn set_config() -> Config {
    // Fallback values if no config file was found
    let fallback_input = String::from("./"); // directory where to search for zstd files
    let fallback_output = String::from("./"); // directory where to write files to
    let fallback_zstd = false; // by default extract everything
    let fallback_compression_level = 0; // zstd compression level between 1-22, 0 means the default of 3
    let fallback_suffix = String::from("_filtered"); // suffix for your output file
    let fallback_file_extension = String::from(".jsonl"); // suffix for your output file
    let fallback_pattern = String::from(r#"^"#); // match everything
    let fallback_threads = 0; // max number of threads rayon spawn, 0 means no limit
    let fallback_buffer = 100000000; // the buffer size after which data is written to disk, here: 100MB
    let fallback_quiet = false;

    // Parse command-line arguments.
    let cli = Cli::parse();

    // Attempt to read the config file
    let config: Option<Config> = if Path::new(&cli.config).exists() {
        match fs::read_to_string(&cli.config) {
            Ok(content) => toml::from_str(&content).ok(),
            Err(e) => {
                eprintln!("Failed to read config file: {}", e);
                process::exit(1);
            }
        }
    } else {
        None
    };

    // Input path.
    let input = cli
        .input
        .or_else(|| Some(config.as_ref()?.input.clone()))
        .unwrap_or_else(|| fallback_input);

    // Output path
    let output = cli
        .output
        .or_else(|| Some(config.as_ref()?.output.clone()))
        .unwrap_or_else(|| fallback_output);

    // Use zstd compression in output
    let zstd = cli.zstd
        || config
            .as_ref()
            .and_then(|c| Some(c.zstd))
            .unwrap_or(fallback_zstd);

    // Zstd compression level
    let mut compression_level = cli
        .compression_level
        .or_else(|| Some(config.as_ref()?.compression_level.clone()))
        .unwrap_or_else(|| fallback_compression_level);

    // Output file suffix
    let suffix = cli
        .suffix
        .or_else(|| Some(config.as_ref()?.suffix.clone()))
        .unwrap_or_else(|| fallback_suffix);

    // Output file extension
    let file_extension = cli
        .file_extension
        .or_else(|| Some(config.as_ref()?.file_extension.clone()))
        .unwrap_or_else(|| fallback_file_extension);

    // Regex pattern.
    let pattern = cli
        .pattern
        .or_else(|| Some(config.as_ref()?.pattern.clone()))
        .unwrap_or_else(|| fallback_pattern);

    // Max threads.
    let threads = cli
        .threads
        .or_else(|| Some(config.as_ref()?.threads.clone()))
        .unwrap_or_else(|| fallback_threads);

    // Max buffer size
    let buffer = cli
        .buffer
        .or_else(|| Some(config.as_ref()?.buffer.clone()))
        .unwrap_or_else(|| fallback_buffer);

    // Mute most announcements
    let quiet = cli.quiet
        || config
            .as_ref()
            .and_then(|c| Some(c.quiet))
            .unwrap_or(fallback_quiet);

    // Validate the regex pattern.
    let _ = match validate_regex(&pattern) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    };

    // Verify valid zstd compression level range
    compression_level = if compression_level_range().contains(&compression_level) {
        compression_level
    } else {
        0
    };

    Config {
        input: input,
        output: output,
        zstd: zstd,
        compression_level: compression_level,
        suffix: suffix,
        file_extension: file_extension,
        pattern: pattern,
        threads: threads,
        buffer: buffer,
        quiet: quiet,
    }
}
