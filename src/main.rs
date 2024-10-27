use std::error::Error;
use std::ffi::c_float;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Error as IoError, Lines, Read, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::AtomicU64;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use std::{fs, u64};

use clap::Parser;
use colored::*;
use indicatif::{HumanBytes, HumanCount, HumanDuration, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sysinfo::System;
use zstd::stream::read::Decoder;
use zstd::stream::write::Encoder;

const PB_UPDATE_INTERVAL: u64 = 1000; // Update interval in ms

fn main() -> Result<(), Box<dyn Error>> {
    // Shared counter for the total decompressed size
    let global_decompressed_size = Arc::new(AtomicUsize::new(0));
    let global_decompressed_lines = Arc::new(AtomicUsize::new(0));
    let global_filtered_lines = Arc::new(AtomicUsize::new(0));
    let global_processed_size = Arc::new(AtomicU64::new(0));

    // Set up config parameters from cli, the config file and fallback values
    let config = set_config();

    // Create thread pool for file processing, we also need to reserve one for the progress updater
    let threads = if config.threads == 0 {
        0
    } else {
        config.threads + 1
    };

    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .unwrap();

    // Find all .zst files in input_path
    let mut total_dir_size = 0;
    let mut zstd_files = Vec::new();

    // Verify that the input path is valid and create it if necessary
    let input_path = PathBuf::from(&config.input);
    if input_path.exists() {
        if !input_path.is_dir() {
            if input_path.extension().and_then(|ext| ext.to_str()) == Some("zst") {
                let metadata_res = input_path.metadata();
                if let Ok(metadata) = metadata_res {
                    total_dir_size += metadata.len();
                    zstd_files.push(input_path);
                }
            }
        } else {
            zstd_files = fs::read_dir(&config.input)?
                .filter_map(|entry| {
                    let entry = entry.ok()?;
                    let path = entry.path();
                    if path.extension().and_then(|ext| ext.to_str()) == Some("zst") {
                        let metadata = entry.metadata().ok()?;
                        total_dir_size += metadata.len();
                        Some(path)
                    } else {
                        None
                    }
                })
                .collect();
        }
    } else {
        eprintln!(
            "Error: The input path '{:?}' is not a valid directory.",
            &config.input
        );
        std::process::exit(1);
    }

    if !Path::new(&config.input).is_dir() {}

    // Verify that the output path is valid and create it if necessary
    let output_path = Path::new(&config.output);
    if output_path.exists() {
        if !output_path.is_dir() {
            eprintln!(
                "Error: The output path '{:?}' is not a valid directory.",
                &config.output
            );
            std::process::exit(1);
        }
    } else {
        println!("Output directory does not exist. Creating directory...");
        fs::create_dir_all(output_path)?;
    }

    // Display files
    let total_files = zstd_files.len();
    //let display_limit = 5;
    print_if_not_quiet(
        config.quiet,
        &format!(
            "Found {} .zst file(s) ({})",
            total_files,
            HumanBytes(total_dir_size)
        ),
    );
    //for file in zstd_files.iter().take(display_limit) {
    //    if let Some(file_name) = file.file_name() {
    //        print_if_not_quiet(config.quiet, &format!("- {:?}", file_name));
    //    }
    //}
    //if total_files > display_limit {
    //    print_if_not_quiet(config.quiet, &format!("..."));
    //}

    // Create progress bar
    let pb = ProgressBar::new(zstd_files.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{spinner:.cyan}{bar:40.cyan/blue}] {pos}/{len} {msg}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(50));

    let start_time = Instant::now(); // We need to initialize this early to prevent funny PiB/s records

    // Pretty sure there's a better way, though I can't find it
    let a = pb.clone();
    let b = config.clone();
    let c = global_decompressed_size.clone();
    let d = global_decompressed_lines.clone();
    let e = global_filtered_lines.clone();
    let f = global_processed_size.clone();
    rayon::spawn(move || start_progress_updater(start_time, total_dir_size, a, &b, &c, &d, &e, &f));

    // Start a file operation for every available thread
    zstd_files.par_iter().for_each(|file_path| {
        let _ = read_lines(
            &file_path,
            &config,
            &pb,
            &global_decompressed_size,
            &global_decompressed_lines,
            &global_filtered_lines,
            &global_processed_size,
        );
        pb.inc(1);
    });

    // Wait PB_UPDATE_INTERVAL so the progressbar updates one last time
    std::thread::sleep(Duration::from_millis(PB_UPDATE_INTERVAL * 2));

    //pb.finish_with_message("All files processed.");
    pb.finish();
    println!("All files processed.");
    /*
    let final_size = global_decompressed_size.load(Ordering::SeqCst);
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
    input_file_path: &Path,
    config: &Config,
    pb: &ProgressBar,
    global_decompressed_size: &Arc<AtomicUsize>,
    global_decompressed_lines: &Arc<AtomicUsize>,
    global_filtered_lines: &Arc<AtomicUsize>,
    global_processed_size: &Arc<AtomicU64>,
) -> std::io::Result<()> {
    // Operates on a single zstd file decompressing it line by line
    let filesize;
    // Skip if input file is empty
    if let Ok(metadata) = fs::metadata(input_file_path) {
        if metadata.len() == 0 {
            pb.suspend(|| {
                print_if_not_quiet(
                    config.quiet,
                    &format!(
                        "Skipping empty file: {:?}",
                        input_file_path.file_name().unwrap_or_default()
                    ),
                );
            });
            return Ok(());
        } else {
            filesize = metadata.len();
        }
    } else {
        pb.suspend(|| {
            print_if_not_quiet(
                config.quiet,
                &format!("Failed to get metadata for: {:?}", input_file_path),
            );
        });
        return Ok(());
    }

    let output_file_path =
        generate_output_filename(&input_file_path.to_string_lossy().to_string(), &config);

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

    // Verify if the file is a valid zstd
    if let Err(err) = verify_zstd(input_file_path) {
        pb.suspend(|| print_if_not_quiet(config.quiet, &format!("{}", err)));
        return Ok(());
    }

    // In in-memory buffer for storing matching lines
    let mut buffer: Vec<u8> = Vec::with_capacity(config.buffer);

    // Track the last matching line to avoid trailing newline
    let mut last_matching_line: Option<String> = None;

    let pattern = Regex::new(&config.pattern.as_str()).unwrap(); //unwrap because already verified

    let output_file = File::create(&output_file_path)?;
    let mut writer = BufWriter::new(&output_file);

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
    fn start_reading(
        reader: BufReader<Decoder<'static, BufReader<File>>>,
    ) -> Result<Lines<BufReader<Decoder<'static, BufReader<File>>>>, IoError> {
        Ok(reader.lines())
    }
    let file = File::open(input_file_path)?;

    let decoder = Decoder::new(file)?;
    let reader = BufReader::new(decoder);

    // Measure the size of decompressed data
    let mut decompressed_size = 0;
    let mut line_counter = 0;
    let mut line_filtered_counter = 0;
    let mut flag_data_written = false;
    
    
    if let Ok(lines) = start_reading(reader) {
        for line in lines {
            if let Ok(line) = line {
                line_counter += 1;
                // Test regex pattern
                // This is the place to add new line-by-line logic
                if pattern.is_match(&line) {
                    // Pattern matches
                    line_filtered_counter += 1;

                    if !config.no_write {
                        // Skip if no output should be written
                        flag_data_written = true;

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
                }

                decompressed_size += line.len();
                if decompressed_size > 500000000 {
                    // Update in 500 MB intervals
                    global_decompressed_size.fetch_add(decompressed_size, Ordering::SeqCst);
                    decompressed_size = 0;
                    global_decompressed_lines.fetch_add(line_counter, Ordering::SeqCst);
                    line_counter = 0;
                    global_filtered_lines.fetch_add(line_filtered_counter, Ordering::SeqCst);
                    line_filtered_counter = 0;
                }
            } else {
                panic!(
                    "Error when decompressing {} with the error: {line:?}\n\
                Make sure your zstd archive includes a single jsonl file.",
                    &input_file_path.to_string_lossy().to_string()
                );
            }
        }
    }

    // Update the process bar by adding the remaining size
    global_decompressed_size.fetch_add(decompressed_size, Ordering::SeqCst);
    global_decompressed_lines.fetch_add(line_counter, Ordering::SeqCst);
    global_filtered_lines.fetch_add(line_filtered_counter, Ordering::SeqCst);
    global_processed_size.fetch_add(filesize, Ordering::SeqCst);

    // Flush any remaining data in the buffer to the output file
    if !buffer.is_empty() {
        flush_buffer(&mut buffer, &mut write_to_output)?;
    }

    // Write the last matching line without an extra newline
    if let Some(last_line) = last_matching_line {
        write_to_output(last_line.as_bytes())?;
    }

    // Delete the file if nothing was ever written to it
    if !flag_data_written {
        // Check if the file is empty
        fs::remove_file(&output_file_path)?;
        pb.suspend(|| {
            print_if_not_quiet(
                config.quiet,
                &format!(
                    "Empty output file deleted {:?}",
                    Path::new(&output_file_path).file_name().unwrap_or_default()
                ),
            );
        });
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
    let input_stem = path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string(); // e.g. "13030000000-13040000000.jsonl"

    let file_stem_without_extension = Path::new(&input_stem)
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy(); // Get only base file name if applicable, i.e. "13030000000-13040000000"

    let original_file_extension = Path::new(&input_stem)
        .extension()
        .unwrap_or_default()
        .to_string_lossy();

    let output_file_extention = {
        if config.file_extension.is_empty() {
            format!(".{}", original_file_extension)
        } else {
            format!(".{}", config.file_extension)
        }
    };
    if config.zstd {
        format!(
            "{}{file_stem_without_extension}{}{}.zst",
            config.output, config.suffix, output_file_extention
        )
    } else {
        format!(
            "{}{file_stem_without_extension}{}{}",
            config.output, config.suffix, output_file_extention
        )
    }
}

// Verify that the file is a valid zstd file
fn verify_zstd(file_path: &Path) -> Result<(), String> {
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

// Function to start a separate thread for updating the progress bar.
fn start_progress_updater(
    start_time: Instant,
    total_dir_size: u64,
    pb: ProgressBar,
    config: &Config,
    global_size: &Arc<AtomicUsize>,
    global_decompressed_lines: &Arc<AtomicUsize>,
    global_filtered_lines: &Arc<AtomicUsize>,
    global_processed_size: &Arc<AtomicU64>,
) {
    let mut sys = System::new_all();
    let mut last_accurate_proc_size = 0;
    let mut processed_size_estimate = 0;
    loop {
        let elapsed = start_time.elapsed().as_secs_f64();
        let global_size = global_size.load(Ordering::SeqCst);
        let global_filtered_lines = global_filtered_lines.load(Ordering::SeqCst);
        let global_decompressed_lines = global_decompressed_lines.load(Ordering::SeqCst);
        let global_processed_size = global_processed_size.load(Ordering::SeqCst);
        let line_ratio = {
            if global_decompressed_lines == 0 {
                0 as f64
            } else {
                ((global_filtered_lines) * 100) as f64
                    / global_decompressed_lines as f64
            }
        };

        sys.refresh_all();
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        let pid = sysinfo::get_current_pid().unwrap();
        let process = sys.process(pid).unwrap();

        // Fetch CPU, memory, and I/O stats
        let cpu_usage = process.cpu_usage() / sys.cpus().len() as c_float;
        let mut cpu_usage_string = cpu_usage.to_string();
        if cpu_usage < 10 as f32 {
            cpu_usage_string.insert_str(0, " ");
        };
        let memory_usage = process.memory();
        let disk_usage = process.disk_usage();

        //  update or estimate the size
        if last_accurate_proc_size == global_processed_size {
            processed_size_estimate += disk_usage.read_bytes;
        } else {
            last_accurate_proc_size = global_processed_size;
            processed_size_estimate = global_processed_size;
        }
        let avg_speed = global_size as f64 / elapsed;
        let line_speed = global_decompressed_lines as f64 / elapsed;
        let remaining_compressed_data = total_dir_size - processed_size_estimate;
        let remaining_time = remaining_compressed_data / (disk_usage.read_bytes + 1); // just don't panic please
        let remaining_percentage = (processed_size_estimate * 100) as f64 / total_dir_size as f64;
        
        pb.set_message(format!(
            "({} remaining)\nCPU: {} | Memory: {} | Speed: {} | I/O Reads: {} | I/O Writes: {}\nDecompressed: {} ({}) | Read Progress: {}/{} ({})\nKept/Total Lines: {}/{} ({})",
            HumanDuration(Duration::new(remaining_time as u64, 0)),
            format!("{:.5}%", cpu_usage_string).bright_blue(),
            format!("{}", HumanBytes(memory_usage)).bright_blue(),
            format!("{:.0} lines/s", line_speed).bright_blue(),
            format!("{}/s", HumanBytes(disk_usage.read_bytes)).bright_blue(),
            format!("{}/s", HumanBytes(disk_usage.written_bytes)).bright_blue(),
            format!("{}", HumanBytes(global_size as u64)),
            format!("{}/s", HumanBytes(avg_speed as u64)).bright_blue(),
            format!("{}", HumanBytes(processed_size_estimate)),
            format!("{}", HumanBytes(total_dir_size)),
            format!("{:.2}%", remaining_percentage).bright_blue(),
            format!("{}", HumanCount(global_filtered_lines as u64)),
            format!("{}", HumanCount(global_decompressed_lines as u64)),
            format!("{:.4}%", line_ratio).bright_blue()
        ));

        // Exit the updater if the progress bar is finished
        if pb.is_finished() {
            break;
        }

        // Update every PB_UPDATE_INTERVAL
        std::thread::sleep(Duration::from_millis(PB_UPDATE_INTERVAL));
    }
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
    #[arg(long = "no-write")]
    no_write: bool,
    #[arg(long = "quiet")]
    quiet: bool,
    #[arg(long = "config", default_value = "config.toml")]
    config: String,
}

// Internal and config.toml structure
#[derive(Debug, Serialize, Deserialize, Clone)]
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
    no_write: bool,
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
    let fallback_file_extension = String::from(""); // file extension for your output file
    let fallback_pattern = String::from(r#"^"#); // match everything
    let fallback_threads = 0; // max number of threads rayon spawn, 0 means no limit
    let fallback_buffer = 4096; // the buffer size after which data is written to disk, here: 4KiB
    let fallback_no_write = false; // do not write to output
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

    // Do not write to output
    let no_write = cli.no_write
        || config
            .as_ref()
            .and_then(|c| Some(c.no_write))
            .unwrap_or(fallback_no_write);

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
    compression_level = if zstd::compression_level_range().contains(&compression_level) {
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
        no_write: no_write,
        quiet: quiet,
    }
}
