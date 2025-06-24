use std::path::{Path, PathBuf};
use std::{env, fs, io};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Seek, Write, SeekFrom};
use std::error::Error;
use std::time::Instant;
use csv::WriterBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::Read; // <<--- Importa Read acá


fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        help();
        return Ok(());
    }

    let command = &args[1];

    match command.as_str() {
        "view" => {
            if args.len() != 3 {
                eprintln!("Usage: csv_tool view <input_file>");
                return Ok(());
            }

            let cant = &args[3];
            let result = view_data(cant);
            lines = result?.lines;

            for line in lines.iter().rev() {
                println!("{}", line);
            }
        },
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
            print!("Checking for duplicate headers in file: {}...", input_file);
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
            let args: Vec<String> = env::args().collect();

    if args.len() >= 3 && args[1] == "merge" {
        let dir = &args[2];
        let ext = if args.len() >= 4 { &args[3] } else { "csv" };
        if let Err(e) = merge_csv_files(dir, ext) {
            eprintln!("❌ Error en merge: {}", e);
            std::process::exit(1);
        }
         } else {
            eprintln!("❌ Uso: csv_tools merge <directorio> [extension]");
            std::process::exit(1);
            }
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

fn view_data(cant : int) -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let path = &args[1];
    let num_lines = if args.len() > 2 { args[2].parse::<usize>().unwrap_or(10) } else { 10 };

    let mut file = File::open(path)?;
    let mut pos = file.seek(SeekFrom::End(0))?;
    let mut buffer = [0u8; 4096];
    let mut lines = Vec::new();
    let mut chunk = Vec::new();

    while pos > 0 && lines.len() <= num_lines {
        let read_size = if pos >= 4096 { 4096 } else { pos as usize };
        pos = file.seek(SeekFrom::Start(pos - read_size as u64))?;
        file.read_exact(&mut buffer[..read_size])?;

        for &b in buffer[..read_size].iter().rev() {
            if b == b'\n' && !chunk.is_empty() {
                lines.push(String::from_utf8_lossy(&chunk.iter().rev().cloned().collect::<Vec<_>>()).to_string());
                chunk.clear();
                if lines.len() >= num_lines {
                    break;
                }
            } else {
                chunk.push(b);
            }
        }
    }

    if !chunk.is_empty() {
        lines.push(String::from_utf8_lossy(&chunk.iter().rev().cloned().collect::<Vec<_>>()).to_string());
    }

    Ok((lines))
}


fn merge_csv_files(dir: &str, extension: &str) -> io::Result<()> {
    let output_name = format!("unificado.{}", extension.trim_start_matches('.'));
    let abs_path = fs::canonicalize(dir)?;

    let csv_files = collect_csv_files(&abs_path, extension, &output_name)?;

    if csv_files.is_empty() {
        eprintln!("⚠️ No se encontraron archivos .{} en {}", extension, abs_path.display());
        std::process::exit(0);
    }

    let output_path = abs_path.join(&output_name);
    println!("📄 Archivo de salida: {:?}", output_path);

    let mut output = BufWriter::new(File::create(&output_path)?);

    let pb = set_progress_bar(&csv_files);

    let mut first = true;
    for (i, file_path) in csv_files.iter().enumerate() {
        pb.set_position(i as u64);
        pb.println(format!("📥 Abriendo archivo: {:?}", file_path));

        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut total_written: u64 = 0;

        for (j, line) in reader.lines().enumerate() {
            let line = line?;
            if j == 0 && !first {
                continue;
            }
            writeln!(output, "{}", line)?;
            total_written += line.len() as u64 + 1; // +1 por \n
            pb.set_position(total_written);
        }
        first = false;
    }

    pb.finish_with_message("✅ Merge completo");
    println!("� Archivo generado en: {}", output_path.display());
    Ok(())
}

fn set_progress_bar(csv_files: &Vec<PathBuf>) -> ProgressBar {
    let total_size: u64 = csv_files.iter()
                                    .map(|f| fs::metadata(f).map(|m| m.len()).unwrap_or(0))
                                    .sum();

    let pb = ProgressBar::new(total_size);

    pb.set_style(
    ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .progress_chars("# "),
    );

    pb.set_message("🔄 Procesando");
    pb
}

fn help() {
    println!("Usage:");
    println!("  View last N lines: csv_tool view <input_file> <num_lines>");
    println!("  Clean headers:     csv_tool clean <input_file> <output_file>");
    println!("  Filter rows:       csv_tool filter <input_file> <output_file> <column_name> <value>");
    println!("  Check headers:     csv_tool check <input_file>");
    println!("  Count lines:       csv_tool count <input_file>");
    println!("  Merge CSV files:   csv_tool merge <directory> [extension]");
    println!("  Help:              csv_tool help");
    println!("  Version:           csv_tool version");
    println!();
    println!("Examples:");
    println!("  csv_tool view ./data/input.csv 10");
    println!("  csv_tool clean ./data/input.csv ./data/output.csv");
    println!("  csv_tool filter ./data/input.csv ./data/output.csv column_name value");
    println!("  csv_tool check ./data/input.csv");
    println!("  csv_tool count ./data/input.csv");
    println!("  csv_tool merge ./data csv");
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


pub fn collect_csv_files(abs_path: &Path, extension: &str, output_name: &str) -> io::Result<Vec<PathBuf>> {
    println!("📂 Reading directory: {:?}", abs_path);

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
                            println!("⚠️ Ignorando archivo de salida: {:?}", name_str);
                            continue;
                        }

                        println!("--> MATCH ✅ {}", name_str);
                        csv_files.push(path);
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

    let lines = reader.lines();

    println!("Checking for duplicate headers in file: {}", file_path);

    let pb = set_progress_bar(&vec![PathBuf::from(file_path)]);

    let header = first_line.trim_end().to_string();
    let mut line_number = 1;

    for (i,line) in lines.enumerate() {
        pb.set_position(i as u64);
        pb.println(format!("📥 Abriendo archivo: {:?}", file_path));
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

    let pb = set_progress_bar(&vec![PathBuf::from(input_file)]);
    println!("Cleaning headers in file: {}", input_file);

    for (i,line) in lines.enumerate() {
        pb.set_position(i as u64);
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
