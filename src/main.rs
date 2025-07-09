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
        eprintln!("  Show last 10 records: csv_tool tail <input_file>");
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
            println!("Cleaning headers in file: {}...", input_file);
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
            print!("Filtering rows in file: {}...", input_file);
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
            println!("Counting csv rows...");
            let line_count = count_lines(input_file)?;
            println!("Number of lines in the file: {}", line_count);
        },
        "count_all" => {
            if args.len() != 3 {
                eprintln!("Usage: csv_tool count_all <file_list>");
                return Ok(());
            }
            let file_list = &args[2];
            count_all_files(file_list)?;
        },
        "merge_dedup" => {
            if args.len() != 4 {
                eprintln!("Usage: csv_tool merge_dedup <file_list> <output_file>");
                return Ok(());
            }
            let file_list = &args[2];
            let output_file = &args[3];
            merge_and_deduplicate(file_list, output_file)?;
        },
        "tail" => {
            if args.len() != 3 {
                eprintln!("Usage: csv_tool tail <input_file>");
                return Ok(());
            }
            let input_file = &args[2];
            println!("Showing last 10 records from file: {}...", input_file);
            show_tail_records(input_file)?;
        },
        _ => {
            eprintln!("Unknown command: {}", command);
            eprintln!("Valid commands are: clean, filter, check, count, count_all, merge_dedup, tail");
        }
    }

    Ok(())
}

fn count_all_files(file_list_path: &str) -> Result<(), Box<dyn Error>> {
    let file = File::open(file_list_path)?;
    let reader = BufReader::new(file);

    let mut total = 0;

    for line in reader.lines() {
        let filename = line?;
        let count = count_lines(&filename)?;
        println!("{}: {} líneas", filename, count);
        total += count;
    }

    println!("\nTotal de líneas en todos los archivos: {}", total);
    Ok(())
}

fn merge_and_deduplicate(file_list_path: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    use std::collections::HashSet;

    let file_list = File::open(file_list_path)?;
    let reader = BufReader::new(file_list);
    let mut seen_lines = HashSet::new();
    let mut writer = BufWriter::new(File::create(output_file)?);

    let mut header_written = false;

    for line in reader.lines() {
        let filename = line?;
        let input = File::open(&filename)?;
        let file_reader = BufReader::new(input);

        for (i, file_line) in file_reader.lines().enumerate() {
            let line_content = file_line?;
            if i == 0 {
                if !header_written {
                    writer.write_all(line_content.as_bytes())?;
                    writer.write_all(b"\n")?;
                    header_written = true;
                }
            } else {
                if seen_lines.insert(line_content.clone()) {
                    writer.write_all(line_content.as_bytes())?;
                    writer.write_all(b"\n")?;
                }
            }
        }
    }

    writer.flush()?;
    println!("Merge completado, duplicados eliminados.");
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

fn show_tail_records(input_file: &str) -> Result<(), Box<dyn Error>> {
    use std::io::{Read, Seek, SeekFrom};
    
    let start_time = Instant::now();
    let mut file = File::open(input_file)?;
    let file_size = file.metadata()?.len();
    
    // Para archivos muy grandes, empezamos leyendo desde cerca del final
    let chunk_size = 64 * 1024; // 64KB chunks
    let mut header: Option<String> = None;
    
    // Si el archivo es muy pequeño, leemos todo
    if file_size <= chunk_size {
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&file);
        let lines: Vec<String> = reader.lines().collect::<Result<Vec<_>, _>>()?;
        
        if let Some(first_line) = lines.first() {
            header = Some(first_line.clone());
            println!("Header: {}", first_line);
            println!("---");
        }
        
        let data_lines: Vec<&String> = lines.iter().skip(1).collect();
        let start_idx = if data_lines.len() > 10 { data_lines.len() - 10 } else { 0 };
        
        for (i, line) in data_lines.iter().skip(start_idx).enumerate() {
            println!("Record {}: {}", start_idx + i + 1, line);
        }
        
        println!("\nShowing last {} records of {} total data records", 
                data_lines.len().min(10), data_lines.len());
        println!("Time taken: {:.3} seconds", start_time.elapsed().as_secs_f64());
        return Ok(());
    }
    
    // Para archivos grandes, usamos una estrategia optimizada
    let mut position = file_size.saturating_sub(chunk_size);
    let mut buffer = Vec::new();
    let mut all_lines = Vec::new();
    
    // Leemos la cabecera primero
    file.seek(SeekFrom::Start(0))?;
    let mut header_reader = BufReader::new(&file);
    let mut header_line = String::new();
    if header_reader.read_line(&mut header_line)? > 0 {
        header = Some(header_line.trim_end().to_string());
    }
    
    // Ahora leemos desde el final hacia atrás en chunks
    loop {
        file.seek(SeekFrom::Start(position))?;
        buffer.clear();
        buffer.resize(chunk_size as usize, 0);
        
        let bytes_read = file.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        
        // Convertimos a string y dividimos en líneas
        let chunk_str = String::from_utf8_lossy(&buffer);
        let mut lines: Vec<&str> = chunk_str.lines().collect();
        
        // Si no estamos al principio del archivo, la primera línea puede estar incompleta
        if position > 0 && !lines.is_empty() {
            lines.remove(0);
        }
        
        // Agregamos las líneas al principio de nuestro vector
        for line in lines.iter().rev() {
            if !line.trim().is_empty() {
                all_lines.insert(0, line.to_string());
                
                // Si tenemos suficientes líneas de datos, podemos parar
                if all_lines.len() >= 11 { // 10 + algún margen
                    break;
                }
            }
        }
        
        if position == 0 || all_lines.len() >= 15 {
            break;
        }
        
        position = position.saturating_sub(chunk_size);
    }
    
    // Mostramos la cabecera
    if let Some(h) = &header {
        println!("Header: {}", h);
        println!("---");
    }
    
    // Filtramos líneas que no sean la cabecera y mostramos las últimas 10
    let data_lines: Vec<String> = all_lines.into_iter()
        .filter(|line| {
            if let Some(ref h) = header {
                line.trim() != h.trim() && !line.trim().is_empty()
            } else {
                !line.trim().is_empty()
            }
        })
        .collect();
    
    let start_idx = if data_lines.len() > 10 { data_lines.len() - 10 } else { 0 };
    let last_records: Vec<&String> = data_lines.iter().skip(start_idx).collect();
    
    for (i, record) in last_records.iter().enumerate() {
        println!("Record {}: {}", start_idx + i + 1, record);
    }
    
    println!("\nShowing last {} records (optimized for large files)", last_records.len());
    println!("Time taken: {:.3} seconds", start_time.elapsed().as_secs_f64());
    
    Ok(())
}
