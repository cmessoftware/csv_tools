use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::error::Error;
use std::time::Instant;
use csv::WriterBuilder;



fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage:");
        eprintln!("  Clean headers: csv_tool clean <input_file> <output_file>");
        eprintln!("  Filter rows: csv_tool filter <input_file> <output_file> <column_name> <value>");
        eprintln!("  Check for duplicate headers: csv_tool check <input_file>");
        eprintln!("  Count csv lines: csv_tool count <input_file>");        
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
            let input_file = &args[2];
            let line_count = count_lines(input_file)?;
            println!("Number of lines in the file: {}", line_count);
        },
        _ => {
            eprintln!("Unknown command: {}", command);
            eprintln!("Valid commands are: clean, filter");
        }
    }

    Ok(())
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
