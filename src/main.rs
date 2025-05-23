use std::collections::HashSet;
use std::path::Path;
use std::{env, fs, io};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::error::Error;
use std::time::Instant;
use csv::WriterBuilder;
use std::fs::OpenOptions;
use chrono::Local;
use indicatif::{ProgressBar, ProgressStyle};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        help();
        return Ok(());
    }

    let command = &args[1];

    match command.as_str() {
        "clean" => {
            if args.len() != 4 {
                eprintln!("Usage: csv_tool clean <input_file> <output_file>");
                return Ok(());
            }
            let input_file = &args[2];
            let output_file = &args[3];
            clean_headers(input_file, output_file)?;
        },
        "filter" => {
            if args.len() != 6 {
                eprintln!("Usage: csv_tool filter <input_file> <output_file> <column_name> <value>");
                return Ok(());
            }
            let input_file = &args[2];
            let output_file = &args[3];
            let column_name = &args[4];
            let value = &args[5];
            filter_rows(input_file, output_file, column_name, value)?;
        },
        "check" => {
            if args.len() != 3 {
                eprintln!("Usage: csv_tool check <input_file>");
                return Ok(());
            }
            let input_file = &args[2];
            if has_duplicate_header(input_file)? {
                println!("Duplicate header found.");
            } else {
                println!("No duplicate header found.");
            }
        },
        "count" => {
            if args.len() != 3 {
                eprintln!("Usage: csv_tool count <input_file>");
                return Ok(());
            }
            print!("Counting lines in file: {}...", args[2]);
            let input_file = &args[2];
            let line_count = count_lines(input_file)?;
            println!("Number of lines in the file: {}", line_count);
        },
        "merge" => {
            merge_csv_files()?
        },
        "help" => {
            help();
        },
        "version" => {
            println!("CSV Tool version 0.1.0");
        },
        _ => {
            eprintln!("Unknown command: {}", command);
            eprintln!("Valid commands are: clean, filter");
        }
    }

    Ok(())
}

fn merge_csv_files() -> Result<(), Box<dyn Error + 'static>> {
 let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 || args[1] != "merge" {
        println!("Uso: csv_tools merge <directorio> [extensión] [archivo_salida]");
        return Ok(());
    }

    let dir = args.get(2).map(|s| s.as_str()).unwrap_or(".");
    let extension = args.get(3).map(|s| s.as_str()).unwrap_or("csv");

    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let output_filename = format!("merged_output_{}.csv", timestamp);
    let merged_csv_file = Path::new(dir).join(&output_filename);

    let csv_files = collect_csv_files(dir, extension, &output_filename)?;
    println!("Found {} files. Merging...", csv_files.len());

    if csv_files.is_empty() {
        return Ok(());
    }

    // Calcular tamaño total
    let total_bytes: u64 = csv_files.iter()
        .filter_map(|p| fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();

    let pb = ProgressBar::new(total_bytes);
    pb.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})"
    ).unwrap());

    let mut writer = OpenOptions::new()
        .create(true)
        .write(true)
        .append(false)
        .open(&merged_csv_file)?;

    let start = Instant::now();
    let mut seen_keys = HashSet::new();
    let mut total_lines = 0;
    let mut skipped = 0;

    for file_path in &csv_files {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut _processed_bytes = 0;

        for (i, line) in reader.lines().enumerate() {
            let line = line?;
            _processed_bytes += line.len() as u64 + 1;

            if i == 0 && total_lines > 0 {
                continue; // skip header
            }

            let key = line.split(',').next().unwrap_or("").to_string();
            if seen_keys.insert(key) {
                writeln!(writer, "{}", line)?;
                total_lines += 1;
            } else {
                skipped += 1;
            }

            pb.inc(line.len() as u64 + 1);
        }
    }

    pb.finish_with_message("✅ Merge completo");

    let duration = start.elapsed();
    println!("✏️  Líneas escritas: {}", total_lines);
    println!("🧹 Duplicados ignorados: {}", skipped);
    println!("⏱️ Tiempo: {:.2?}", duration);

    Ok(())

}

fn help() {
    println!("Usage:");
    println!("  Clean headers: csv_tool clean <input_file> <output_file>");
    println!("  Filter rows: csv_tool filter <input_file> <output_file> <column_name> <value>");
    println!("  Check for duplicate headers: csv_tool check <input_file>");
    println!("  Count csv lines: csv_tool count <input_file>");
    println!("  Merge CSV files: csv_tool merge <directory> [extension] [output_file]");
    print!("  Help: csv_tool help");
    println!("  Version: csv_tool version");
    println!("  Example: csv_tool merge ./data csv merged_output.csv");
    println!("  Example: csv_tool clean ./data/input.csv ./data/output.csv");
    println!("  Example: csv_tool filter ./data/input.csv ./data/output.csv column_name value");
    println!("  Example: csv_tool check ./data/input.csv");
    println!("  Example: csv_tool count ./data/input.csv");
}

fn count_lines(input_file: &str) -> Result<usize, Box<dyn Error>> {

    print!("Counting lines in file: {}...", input_file);
    let start = Instant::now();
    let file = File::open(input_file).expect("Failed to open file");
    let reader = BufReader::new(file);

    let line_count = reader.lines().count();

    let _ = start.elapsed().as_secs_f64();
    println!("Time taken to count {} lines: {:.2} seconds",line_count, start.elapsed().as_secs_f64());

    Ok(line_count)
}


pub fn collect_csv_files(dir: &str, extension: &str, output_name: &str) -> io::Result<Vec<String>> {
    let abs_path = fs::canonicalize(dir)?;
    println!("Reading directory: {:?}", abs_path);

    let expected_ext = extension.trim_start_matches('.').to_ascii_lowercase();
    let mut csv_files = Vec::new();

    for entry in fs::read_dir(&abs_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                let file_ext = ext.to_string_lossy().to_ascii_lowercase();

                if file_ext == expected_ext {
                    if let Some(name) = path.file_name() {
                        let name_str = name.to_string_lossy();

                        if name_str == output_name {
                            println!("Ignorando archivo de salida: {:?}", name_str);
                            continue;
                        }

                        println!("--> MATCH ✅ {}", name_str);
                        csv_files.push(path.to_string_lossy().to_string());
                    }
                }
            }
        }
    }

    Ok(csv_files)
}


fn has_duplicate_header(file_path: &str) -> Result<bool, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut result = false;

    let mut first_line = String::new();
    if reader.read_line(&mut first_line)? == 0 {
        result = false; // Empty file, no duplicates
    }

    let header = first_line.trim_end().to_string();
    let mut line_number = 1;

    for line in reader.lines() {
        line_number += 1;
        let line = line?;
        if line.trim_end() == header {
            println!("Duplicate header found on line {}", line_number);
            result = true;
        }
    }

    Ok(result)
}

fn clean_headers(input_file: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    let input = File::open(input_file)?;
    let reader = BufReader::new(input);
    let output = File::create(output_file)?;
    let mut writer = BufWriter::new(output);

    let mut first_line = String::new();
    let mut lines = reader.lines();

    if let Some(Ok(header)) = lines.next() {
        first_line = header;
        writer.write_all(first_line.as_bytes())?;
        writer.write_all(b"\n")?;
    }

    for line in lines {
        let line = line?;
        if line != first_line {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
        }
    }

    writer.flush()?;
    println!("Header cleanup complete.");
    Ok(())
}

fn filter_rows(input_file: &str, output_file: &str, column_name: &str, value: &str) -> Result<(), Box<dyn Error>> {
    let input = File::open(input_file)?;
    let reader = BufReader::new(input);
    let output = File::create(output_file)?;
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(BufWriter::new(output));

    let mut rdr = csv::Reader::from_reader(reader);
    let headers = rdr.headers()?.clone();
    writer.write_record(headers.iter())?;

    let column_index = headers.iter().position(|h| h == column_name).ok_or_else(|| {
        format!("Column '{}' not found in input file", column_name)
    })?;

    for result in rdr.records() {
        let record = result?;
        if record.get(column_index).unwrap_or("") == value {
            writer.write_record(&record)?;
        }
    }

    writer.flush()?;
    println!("Row filtering complete.");
    Ok(())
}
