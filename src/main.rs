use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::error::Error;
use std::time::{Instant, Duration};
use csv::WriterBuilder;
use chrono::{DateTime, NaiveDateTime}; // add chron o import near top

// Importar módulos locales
mod progress;
mod file_utils;
mod models;

use progress::ProgressTracker;
use file_utils::estimate_total_lines_from_list;
use models::MorososTransmitDynamoDbModel;

fn validate_csv_schema_with_model_progress(
    input_file: &str,
    error_file: &str,
    max_errors_to_show: usize,
    cancel_on_max_errors: bool,
) -> Result<(), Box<dyn Error>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader, BufWriter, Write};
    use std::time::{Instant, Duration};
    use csv::StringRecord;

    let file = File::open(input_file)?;
    let reader = BufReader::new(file);

    let mut lines = reader.lines();
    let header_line = match lines.next() {
        Some(Ok(h)) => h,
        _ => return Err("Archivo vacío o sin header".into()),
    };
    let expected_len = header_line.split(',').count();

    let mut error_writer = BufWriter::new(File::create(error_file)?);
    let mut line_number = 2;
    let mut error_count = 0;
    let mut processed = 0;
    let mut shown_errors = 0;

    // Para la barra de progreso, estimar total de líneas
    let total_lines = count_lines(input_file)? - 1;
    let start = Instant::now();

    println!("Validando {} registros...", total_lines);

    for line in lines {
        let line = line?;
        let csv_line = line.clone();
        let record = StringRecord::from(line.split(',').collect::<Vec<_>>());

        if record.len() != expected_len {
            let msg = format!(
                "Error en línea {}: cantidad de columnas {} (esperado: {})\n",
                line_number,
                record.len(),
                expected_len
            );
            if shown_errors < max_errors_to_show {
                print!("{}", msg);
                println!("Registro CSV con error:\n{}", csv_line);
                shown_errors += 1;
            }
            error_writer.write_all(msg.as_bytes())?;
            error_writer.write_all(csv_line.as_bytes())?;
            error_writer.write_all(b"\n")?;
            error_count += 1;
            if cancel_on_max_errors && shown_errors >= max_errors_to_show {
                println!("\nSe alcanzó el máximo de errores a mostrar ({}). Cancelando validación.", max_errors_to_show);
                break;
            }
        } else {
            // Intentar deserializar solo si la longitud es correcta
            let deser_result = record.deserialize::<MorososTransmitDynamoDbModel>(None);
            if let Err(e) = deser_result {
                let msg = format!("Error en línea {}: {}\n", line_number, e);
                if shown_errors < max_errors_to_show {
                    print!("{}", msg);
                    println!("Registro CSV con error:\n{}", csv_line);
                    shown_errors += 1;
                }
                error_writer.write_all(msg.as_bytes())?;
                error_writer.write_all(csv_line.as_bytes())?;
                error_writer.write_all(b"\n")?;
                error_count += 1;
                if cancel_on_max_errors && shown_errors >= max_errors_to_show {
                    println!("\nSe alcanzó el máximo de errores a mostrar ({}). Cancelando validación.", max_errors_to_show);
                    break;
                }
            }
        }
        processed += 1;
        line_number += 1;

        // Actualiza la barra de progreso cada 1000 registros o al final
        if processed % 1000 == 0 || processed == total_lines {
            let elapsed = start.elapsed();
            let percent = (processed as f64 / total_lines.max(1) as f64) * 100.0;
            let speed = processed as f64 / elapsed.as_secs_f64().max(0.01);
            let remaining = if speed > 0.0 {
                Duration::from_secs_f64((total_lines.saturating_sub(processed) as f64 / speed).max(0.0))
            } else {
                Duration::from_secs(0)
            };
            print!(
                "\r[{:6.2}%] {}/{} | errores: {} | tiempo: {:.0?} | ETA: {:.0?}    ",
                percent,
                processed,
                total_lines,
                error_count,
                elapsed,
                remaining
            );
            std::io::stdout().flush().ok();
        }
    }

    println!("\nValidación completada. Total de errores: {}. Detalles en '{}'", error_count, error_file);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
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
        "count_unique" => {
            if args.len() != 3 {
                eprintln!("Usage: csv_tool count_unique <file_list>");
                return Ok(());
            }
            let file_list = &args[2];
            count_unique_records(file_list)?;
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
        "external_dedup" => {
            if args.len() != 4 {
                eprintln!("Usage: csv_tool external_dedup <file_list> <output_file>");
                return Ok(());
            }
            let file_list = &args[2];
            let output_file = &args[3];
            external_merge_dedup(file_list, output_file)?;
        },
        "estimate_memory" => {
            if args.len() != 3 {
                eprintln!("Usage: csv_tool estimate_memory <file_list>");
                return Ok(());
            }
            let file_list = &args[2];
            estimate_memory_usage(file_list)?;
        },
        "compare" => {
            if args.len() != 5 {
                eprintln!("Usage: csv_tool compare <file1> <file2> <num_rows>");
                return Ok(());
            }
            let file1 = &args[2];
            let file2 = &args[3];
            let num_rows: usize = args[4].parse().unwrap_or(100);
            compare_first_n(file1, file2, num_rows)?;
        },
        "tail" => {
            if args.len() != 4 {
                eprintln!("Usage: csv_tool tail <input_file> <num_rows>");
                return Ok(());
            }
            let input_file = &args[2];
            let num_rows: usize = args[3].parse().unwrap_or(10);
            tail_csv(input_file, num_rows)?;
        },
        "help" => {
            help();
        },
        "merge" => {
            if args.len() != 4 {
                eprintln!("Usage: csv_tool merge <file_list> <output_file>");
                return Ok(());
            }
            let file_list = &args[2];
            let output_file = &args[3];
            merge_files(file_list, output_file)?;
        },
        "validate_model" => {
            if args.len() != 7 {
                eprintln!("Usage: csv_tool validate_model <input_file> <error_file> <dynamo_table> <max_errors_to_show> <cancel_on_max_errors>");
                return Ok(());
            }
            let input_file = &args[2];
            let error_file = &args[3];
            let table = &args[4];
            let max_errors_to_show: usize = args[5].parse().unwrap_or(10);
            let cancel_on_max_errors: bool = args[6].parse().unwrap_or(false);
            match table.as_str() {
                "tabla_custom" => {
                    validate_csv_schema_with_model_progress(input_file, error_file, max_errors_to_show, cancel_on_max_errors)?;
                }
                // Agrega aquí otros modelos si es necesario
                _ => {
                    eprintln!("Unknown DynamoDB table/model: {}", table);
                }
            }
        },
        "clean_invalid_lines" => {
            if args.len() != 5 {
                eprintln!("Usage: csv_tool clean_invalid_lines <input_file> <output_file> <error_file>");
                return Ok(());
            }
            let input_file = &args[2];
            let output_file = &args[3];
            let error_file = &args[4];

            // Detectar cantidad de columnas esperadas desde el header
            let header = {
                let file = File::open(input_file)?;
                let mut reader = BufReader::new(file);
                let mut header_line = String::new();
                reader.read_line(&mut header_line)?;
                header_line.trim_end().split(',').count()
            };

            clean_invalid_lines(input_file, output_file, error_file, header)?;
        },
        "inspect_line" => {
            if args.len() < 4 {
                eprintln!("Usage: csv_tool inspect_line <input_file> <line_number> [context]");
                return Ok(());
            }
            let input = &args[2];
            let target: usize = args[3].parse().unwrap_or(1);
            let context: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(5);
            inspect_line_range(input, target, context, false)?;
        },
        "find_missing_key" => {
            if args.len() < 4 {
                eprintln!("Usage: csv_tool find_missing_key <input_file> <key_column> [max_report]");
                return Ok(());
            }
            let input = &args[2];
            let key = &args[3];
            let maxr: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(100);
            find_missing_key(input, key, maxr)?;
        },
        "find_oversize" => {
            if args.len() < 3 {
                eprintln!("Usage: csv_tool find_oversize <input_file> [threshold_bytes] [max_report]");
                return Ok(());
            }
            let input = &args[2];
            let threshold: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(350_000);
            let maxr: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(10);
            find_oversize_items(input, threshold, maxr)?;
        },
        "find_invalid_numeric" => {
            if args.len() < 4 {
                eprintln!("Usage: csv_tool find_invalid_numeric <input_file> <col1,col2,...> [max_report]");
                return Ok(());
            }
            let input = &args[2];
            let cols = &args[3];
            let maxr: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(100);
            find_invalid_numeric(input, cols, maxr)?;
        },
        _ => {
            eprintln!("Unknown command: {}", command);
            help();
        }
    }

    Ok(())
}

/// Enum para seleccionar el modelo según el nombre de la tabla DynamoDB
pub enum DynamoModel {
    TablaMorosos,
    // Aquí puedes agregar otros modelos en el futuro
}

/// Valida un CSV contra el modelo especificado por nombre de tabla DynamoDB.
/// Guarda los errores en un archivo de texto.
pub fn validate_csv_schema_with_model(
    input_file: &str,
    error_file: &str,
    model: DynamoModel,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs::File;
    use std::io::{BufWriter, Write};

    let file = File::open(input_file)?;
    let mut rdr = csv::Reader::from_reader(file);
    let mut error_writer = BufWriter::new(File::create(error_file)?);
    let mut line_number = 2;
    let mut error_count = 0;

    match model {
        DynamoModel::TablaMorosos => {
            for result in rdr.deserialize::<MorososTransmitDynamoDbModel>() {
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        let msg = format!("Error en línea {}: {}\n", line_number, e);
                        print!("{}", msg);
                        error_writer.write_all(msg.as_bytes())?;
                        error_count += 1;
                    }
                }
                line_number += 1;
            }
        }
        // Aquí puedes agregar otros modelos usando else if o match adicional
        // DynamoModel::OtroModelo => { ... }
    }

    error_writer.flush()?;
    println!(
        "Validación completada. Total de errores: {}. Detalles en '{}'",
        error_count, error_file
    );
    Ok(())
}

fn help() {
    println!("Available commands:");
    println!("  clean: Clean duplicate headers from a CSV file.");
    println!("  filter: Filter rows based on a column value.");
    println!("  check: Check for duplicate headers in a CSV file.");
    println!("  count: Count the number of lines in a CSV file.");
    println!("  count_all: Count lines in multiple files listed in a text file.");
    println!("  count_unique: Count unique records across multiple files (fast, but needs RAM).");
    println!("  merge_dedup: Merge multiple CSV files and remove duplicates (in-memory).");
    println!("  external_dedup: Merge and deduplicate using external sort (for HUGE files).");
    println!("  estimate_memory: Estimate RAM needed for in-memory deduplication.");
    println!("  compare: Compare first N rows of two CSV files.");
    println!("  tail: Show the last N rows of a CSV file.");
    println!("  merge: Merge multiple CSV files (no deduplication).");
    println!("  validate_model: Validate a CSV file against a DynamoDB model schema with progress bar.");
    println!("      Usage: csv_tool validate_model <input_file> <error_file> <dynamo_table> <max_errors_to_show> <cancel_on_max_errors>");
    println!("      - <input_file>: CSV file to validate");
    println!("      - <error_file>: Output file for error details");
    println!("      - <dynamo_table>: DynamoDB table/model name (e.g. tabla_morosos)");
    println!("      - <max_errors_to_show>: Show up to N errors on screen (integer)");
    println!("      - <cancel_on_max_errors>: true/false, if true cancels after showing N errors");
    println!("  clean_invalid_lines: Genera una copia del CSV solo con líneas válidas (columnas correctas) y elimina las líneas corruptas.");
    println!("      Usage: csv_tool clean_invalid_lines <input_file> <output_file> <error_file>");
    println!("      - <input_file>: Archivo CSV de entrada");
    println!("      - <output_file>: Archivo CSV de salida solo con líneas válidas");
    println!("      - <error_file>: Archivo de texto con detalles de las líneas eliminadas");
    println!("  inspect_line: Inspecciona una línea específica en el CSV, mostrando contexto alrededor.");
    println!("      Usage: csv_tool inspect_line <input_file> <line_number> [context]");
    println!("      - <input_file>: Archivo CSV a inspeccionar");
    println!("      - <line_number>: Número de línea a inspeccionar (1-based)");
    println!("      - <context>: Número de líneas de contexto a mostrar antes y después (opcional, por defecto 5)");
    println!("  find_missing_key: Busca claves faltantes en una columna específica del CSV.");
    println!("      Usage: csv_tool find_missing_key <input_file> <key_column> [max_report]");
    println!("      - <input_file>: Archivo CSV a analizar");
    println!("      - <key_column>: Nombre de la columna clave a verificar");
    println!("      - <max_report>: Número máximo de reportes a mostrar (opcional, por defecto 100)");
    println!("  find_oversize: Encuentra líneas que exceden un tamaño umbral en bytes.");
    println!("      Usage: csv_tool find_oversize <input_file> [threshold_bytes] [max_report]");
    println!("      - <input_file>: Archivo CSV a analizar");
    println!("      - <threshold_bytes>: Tamaño umbral en bytes (opcional, por defecto 350000)");
    println!("      - <max_report>: Número máximo de reportes a mostrar (opcional, por defecto 10)");
    println!("  find_invalid_numeric: Busca valores numéricos inválidos en columnas específicas.");
    println!("      Usage: csv_tool find_invalid_numeric <input_file> <col1,col2,...> [max_report]");
    println!("      - <input_file>: Archivo CSV a analizar");
    println!("      - <col1,col2,...>: Nombres de las columnas a verificar, separadas por comas");
    println!("      - <max_report>: Número máximo de reportes a mostrar (opcional, por defecto 100)");
}

fn count_all_files(file_list_path: &str) -> Result<(), Box<dyn Error>> {
    // Obtener lista de archivos para estimación
    let file = File::open(file_list_path)?;
    let reader = BufReader::new(file);
    let file_names: Vec<String> = reader.lines().collect::<Result<Vec<_>, _>>()?;
    
    println!("📊 Estimando total de líneas para progress...");
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    println!("Estimación: ~{} líneas totales en {} archivos", estimated_total, file_names.len());
    
    let mut progress = ProgressTracker::new(estimated_total);
    let mut total = 0;
    let mut processed_lines = 0;

    for filename in file_names {
        let count = count_lines_with_progress(&filename, &mut progress, &mut processed_lines)?;
        println!("\n{}: {} líneas", filename, count);
        total += count;
    }

    progress.finish(&format!("📈 Total de líneas en todos los archivos: {}", total));
    Ok(())
}

fn merge_and_deduplicate(file_list_path: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    use std::collections::HashSet;

    println!("🔄 Estimando total de líneas para merge...");
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    println!("Estimación: ~{} líneas totales", estimated_total);
    
    let mut progress = ProgressTracker::new(estimated_total);
    let mut processed_lines = 0;

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
            processed_lines += 1;
            
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
            
            // Actualizar progreso cada 1000 líneas
            if processed_lines % 1000 == 0 {
                progress.update(processed_lines);
            }
        }
    }

    writer.flush()?;
    progress.finish(&format!("🔄 Merge completado, {} registros únicos guardados en {}", seen_lines.len(), output_file));
    Ok(())
}

fn count_lines_with_progress(input_file: &str, progress: &mut ProgressTracker, processed_lines: &mut usize) -> Result<usize, Box<dyn Error>> {
    let file = File::open(input_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;

    for _line in reader.lines() {
        line_count += 1;
        *processed_lines += 1;
        
        // Actualizar progreso cada 1000 líneas para mejor rendimiento
        if line_count % 1000 == 0 {
            progress.update(*processed_lines);
        }
    }
    
    progress.update(*processed_lines);
    Ok(line_count)
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

fn compare_first_n(file1: &str, file2: &str, num_rows: usize) -> Result<(), Box<dyn Error>> {
    let f1 = File::open(file1)?;
    let f2 = File::open(file2)?;
    let reader1 = BufReader::new(f1);
    let reader2 = BufReader::new(f2);

    let mut lines1 = reader1.lines();
    let mut lines2 = reader2.lines();

    let header1 = lines1.next().unwrap_or(Ok(String::new()))?;
    let header2 = lines2.next().unwrap_or(Ok(String::new()))?;

    if header1 != header2 {
        println!("⚠️ Header mismatch!");
        println!("File1 header: {}", header1);
        println!("File2 header: {}", header2);
    } else {
        println!("✅ Headers match.");
    }

    println!("Comparing first {} data rows...", num_rows);

    let mut differences = 0;

    for i in 1..=num_rows {
        let line1 = lines1.next().unwrap_or(Ok(String::new()))?;
        let line2 = lines2.next().unwrap_or(Ok(String::new()))?;

        if line1 != line2 {
            println!("❌ Difference at line {}:", i + 1);
            println!("File1: {}", line1);
            println!("File2: {}", line2);
            differences += 1;
        }
    }

    if differences == 0 {
        println!("🎉 No differences found in the first {} rows.", num_rows);
    } else {
        println!("🔍 Found {} differences in the first {} rows.", differences, num_rows);
    }

    Ok(())
}

fn tail_csv(input_file: &str, num_rows: usize) -> Result<(), Box<dyn Error>> {
    use std::collections::VecDeque;

    let file = File::open(input_file)?;
    let reader = BufReader::new(file);

    let mut lines = reader.lines();
    let header = lines.next().unwrap_or(Ok(String::new()))?;
    let mut buffer = VecDeque::with_capacity(num_rows);

    for line in lines {
        let line = line?;
        if buffer.len() == num_rows {
            buffer.pop_front();
        }
        buffer.push_back(line);
    }

    println!("{}", header);
    for line in buffer {
        println!("{}", line);
    }

    Ok(())
}

fn count_unique_records(file_list_path: &str) -> Result<(), Box<dyn Error>> {
    use std::collections::HashSet;

    println!("📊 Estimando total de líneas para conteo único...");
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    println!("Estimación: ~{} líneas totales", estimated_total);
    
    let mut progress = ProgressTracker::new(estimated_total);

    let file_list = File::open(file_list_path)?;
    let reader = BufReader::new(file_list);
    let mut seen_lines = HashSet::new();
    let mut total_lines = 0;
    let mut files_processed = 0;

    for line in reader.lines() {
        let filename = line?;
        let input = File::open(&filename)?;
        let file_reader = BufReader::new(input);
        
        let mut file_lines = 0;
        let mut file_unique = 0;

        for (i, file_line) in file_reader.lines().enumerate() {
            let line_content = file_line?;
            total_lines += 1;
            file_lines += 1;
            
            // Skip header line (first line of first file)
            if files_processed == 0 && i == 0 {
                seen_lines.insert(line_content);
                file_unique += 1;
                progress.update(total_lines);
                continue;
            }
            
            // Skip headers of subsequent files
            if files_processed > 0 && i == 0 {
                progress.update(total_lines);
                continue;
            }
            
            if seen_lines.insert(line_content) {
                file_unique += 1;
            }
            
            // Actualizar progreso cada 1000 líneas
            if total_lines % 1000 == 0 {
                progress.update(total_lines);
            }
        }
        
        println!("\n{}: {} líneas, {} únicas", filename, file_lines, file_unique);
        files_processed += 1;
    }

    let unique_count = seen_lines.len();
    let duplicates = total_lines - unique_count;
    
    progress.finish(&format!("🔍 Conteo único completado"));
    
    println!();
    println!("📊 RESUMEN:");
    println!("Total de líneas procesadas: {}", total_lines);
    println!("Registros únicos encontrados: {}", unique_count);
    println!("Archivos procesados: {}", files_processed);
    println!("Duplicados detectados: {}", duplicates);
    
    Ok(())
}

fn estimate_memory_usage(file_list_path: &str) -> Result<(), Box<dyn Error>> {
    println!("🧠 Estimando uso de memoria para deduplicación in-memory...");
    
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    
    // Estimar tamaño promedio de línea (basado en formato personalizado)
    let avg_line_size = 200; // bytes aproximados por línea CSV
    let overhead_factor = 1.5; // overhead de HashMap/HashSet
    
    let estimated_memory_bytes = (estimated_total as f64 * avg_line_size as f64 * overhead_factor) as u64;
    let memory_gb = estimated_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    
    println!("📊 ESTIMACIÓN DE MEMORIA:");
    println!("  Total de líneas estimadas: {}", estimated_total);
    println!("  Tamaño promedio por línea: {} bytes", avg_line_size);
    println!("  Memoria RAM estimada necesaria: {:.2} GB", memory_gb);
    
    if memory_gb > 16.0 {
        println!("⚠️  ADVERTENCIA: Memoria estimada muy alta!");
        println!("💡 Recomendación: Usar 'external_dedup' en lugar de 'count_unique' o 'merge_dedup'");
        println!("🚀 Comando sugerido: ./csv_tools.exe external_dedup {} output.csv", file_list_path);
    } else if memory_gb > 8.0 {
        println!("⚠️  CUIDADO: Memoria estimada alta, monitorear el sistema");
    } else {
        println!("✅ Memoria estimada dentro de límites razonables");
        println!("🚀 Puedes usar 'count_unique' o 'merge_dedup' sin problemas");
    }
    
    Ok(())
}

fn external_merge_dedup(file_list_path: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    use std::process::Command;
    use std::path::Path;
    
    println!("🔄 Iniciando deduplicación externa para archivos GIGANTES...");
    
    // Crear archivo temporal combinado
    let temp_merged = "temp_merged_all.csv";
    
    println!("📂 Paso 1: Combinando archivos...");
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    let mut progress = ProgressTracker::new(estimated_total);
    
    // Combinar todos los archivos en uno temporal
    let file_list = std::fs::File::open(file_list_path)?;
    let reader = std::io::BufReader::new(file_list);
    let mut writer = std::io::BufWriter::new(std::fs::File::create(temp_merged)?);
    let mut header_written = false;
    let mut processed_lines = 0;
    
    for line in std::io::BufRead::lines(reader) {
        let filename = line?;
        let input = std::fs::File::open(&filename)?;
        let file_reader = std::io::BufReader::new(input);
        
        for (i, file_line) in std::io::BufRead::lines(file_reader).enumerate() {
            let line_content = file_line?;
            processed_lines += 1;
            
            if i == 0 {
                if !header_written {
                    writeln!(writer, "{}", line_content)?;
                    header_written = true;
                }
            } else {
                writeln!(writer, "{}", line_content)?;
            }
            
            if processed_lines % 1000 == 0 {
                progress.update(processed_lines);
            }
        }
    }
    
    writer.flush()?;
    progress.finish("📂 Combinación completada");
    
    println!("🔄 Paso 2: Ordenando y deduplicando usando sort externo...");
    
    // Usar sort del sistema para ordenar y eliminar duplicados
    let sort_result = if cfg!(target_os = "windows") {
        // En Windows, usar PowerShell
        Command::new("powershell")
            .arg("-Command")
            .arg(&format!(
                "Get-Content '{}' | Sort-Object -Unique | Set-Content '{}'",
                temp_merged, output_file
            ))
            .status()?
    } else {
        // En Unix/Linux, usar sort nativo
        Command::new("sort")
            .arg("-u")  // unique
            .arg(temp_merged)
            .arg("-o")
            .arg(output_file)
            .status()?
    };
    
    if sort_result.success() {
        println!("✅ Deduplicación externa completada exitosamente!");
        
        // Limpiar archivo temporal
        if Path::new(temp_merged).exists() {
            std::fs::remove_file(temp_merged)?;
            println!("🗑️  Archivo temporal limpiado");
        }
        
        // Contar líneas en resultado final
        let final_count = count_lines(output_file)?;
        println!("📊 RESULTADO FINAL:");
        println!("  Archivo generado: {}", output_file);
        println!("  Registros únicos: {}", final_count - 1); // -1 por el header
        
    } else {
        eprintln!("❌ Error en el proceso de sort externo");
        return Err("Sort command failed".into());
    }
    
    Ok(())
}

/// Merge multiple CSV files (listed in file_list_path) into a single output_file, ignoring duplicates.
/// The output will contain the header from the first file only.
fn merge_files(file_list_path: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader, BufWriter, Write};

    println!("🔄 Estimando total de líneas para merge...");
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    println!("Estimación: ~{} líneas totales", estimated_total);

    let mut progress = ProgressTracker::new(estimated_total);
    let mut processed_lines = 0;

    let file_list = File::open(file_list_path)?;
    let reader = BufReader::new(file_list);
    let mut writer = BufWriter::new(File::create(output_file)?);

    let mut header_written = false;

    for line in reader.lines() {
        let filename = line?;
        let input = File::open(&filename)?;
        let file_reader = BufReader::new(input);

        for (i, file_line) in file_reader.lines().enumerate() {
            let line_content = file_line?;
            processed_lines += 1;

            if i == 0 {
                if !header_written {
                    writer.write_all(line_content.as_bytes())?;
                    writer.write_all(b"\n")?;
                    header_written = true;
                }
            } else {
                writer.write_all(line_content.as_bytes())?;
                writer.write_all(b"\n")?;
            }

            // Actualizar progreso cada 1000 líneas
            if processed_lines % 1000 == 0 {
                progress.update(processed_lines);
            }
        }
    }

    writer.flush()?;
    progress.finish(&format!("✅ Merge completado, {} líneas guardadas en {}", processed_lines, output_file));
    Ok(())
}

fn clean_invalid_lines(
    input_file: &str,
    output_file: &str,
    error_file: &str,
    expected_len: usize,
) -> Result<(), Box<dyn Error>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader, BufWriter, Write};
    use std::time::{Instant, Duration};

    let file = File::open(input_file)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut writer = BufWriter::new(File::create(output_file)?);
    let mut error_writer = BufWriter::new(File::create(error_file)?);

    // Escribir el header tal cual
    if let Some(Ok(header)) = lines.next() {
        writeln!(writer, "{}", header)?;
    }

    // Estimar total de líneas para la barra de progreso
    let total_lines = {
        let file = File::open(input_file)?;
        BufReader::new(file).lines().count().saturating_sub(1)
    };

    let mut line_number = 2;
    let mut error_count = 0;
    let mut cleaned_count = 0;
    let mut processed = 0;
    let start = Instant::now();

    for line in lines {
        let line = line?;
        let record_len = line.split(',').count();
        if record_len == expected_len {
            writeln!(writer, "{}", line)?;
            cleaned_count += 1;
        } else {
            let msg = format!(
                "Línea {} inválida: columnas {}, esperado: {}\n{}",
                line_number, record_len, expected_len, line
            );
            error_writer.write_all(msg.as_bytes())?;
            error_count += 1;
        }
        processed += 1;
        line_number += 1;

        // Actualiza la barra de progreso cada 1000 líneas o al final
        if processed % 1000 == 0 || processed == total_lines {
            let elapsed = start.elapsed();
            let percent = (processed as f64 / total_lines.max(1) as f64) * 100.0;
            let speed = processed as f64 / elapsed.as_secs_f64().max(0.01);
            let remaining = if speed > 0.0 {
                Duration::from_secs_f64((total_lines.saturating_sub(processed) as f64 / speed).max(0.0))
            } else {
                Duration::from_secs(0)
            };
            print!(
                "\r[{:6.2}%] {}/{} | válidas: {} | inválidas: {} | tiempo: {:.0?} | ETA: {:.0?}    ",
                percent,
                processed,
                total_lines,
                cleaned_count,
                error_count,
                elapsed,
                remaining
            );
            std::io::stdout().flush().ok();
        }
    }

    println!(
        "\nLimpieza completada. Líneas válidas: {}, líneas eliminadas: {}. Detalles en '{}'",
        cleaned_count, error_count, error_file
    );
    Ok(())
}

// ------------------- CSV inspection helpers -------------------

fn validate_moros_record(record: &csv::StringRecord, expected_cols: &[&str]) -> Vec<String> {
    let mut violations: Vec<String> = Vec::new();

    // column count
    if record.len() != expected_cols.len() {
        violations.push(format!("Column count mismatch: {} vs expected {}", record.len(), expected_cols.len()));
        // still attempt other checks when possible
    }

    // helper to get trimmed value by index
    let get = |i: usize| record.get(i).unwrap_or("").trim();

    // indexes according to expected_cols:
    // 0:Cuil,1:NroDoc,2:ApellidoNombre,3:IdCliente,4:IdRegion,5:RazonSocial,6:Telefono,
    // 7:NombreRegion,8:NombreCategoria,9:Periodo,10:IdEntidad,11:CreateDate,12:CreateUser

    // Cuil (not empty)
    if get(0).is_empty() {
        violations.push("Cuil is empty".to_string());
    }

    // numeric checks
    let numeric_indices = [(1, "NroDoc"), (3, "IdCliente"), (4, "IdRegion"), (9, "Periodo"), (10, "IdEntidad")];
    for (idx, name) in numeric_indices.iter() {
        let val = get(*idx);
        if val.is_empty() {
            violations.push(format!("{} is empty", name));
        } else if val.parse::<i128>().is_err() && val.parse::<f64>().is_err() {
            violations.push(format!("{} not numeric: '{}'", name, val));
        }
    }

    // CreateDate basic sanity (if present)
    let cd = get(11);
    if !cd.is_empty() {
        if !(cd.contains('-') && cd.contains(':')) {
            violations.push(format!("CreateDate not in expected format: '{}'", cd));
        } else {
            // optional: try parse with RFC style fallback
            if DateTime::parse_from_rfc3339(cd).is_err() && chrono::NaiveDateTime::parse_from_str(cd, "%Y-%m-%d %H:%M:%S").is_err() {
                // don't fail hard, just warn
                violations.push(format!("CreateDate parse warning: '{}'", cd));
            }
        }
    }

    violations
}

fn inspect_line_range(input_file: &str, target_line: usize, context: usize, validate_model: bool) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::VecDeque;
    use csv::ReaderBuilder;
    use std::io::Read;
    // expected schema for MorososTransmitDynamoDbModel
    let expected_cols = [
        "Cuil","NroDoc","ApellidoNombre","IdCliente","IdRegion","RazonSocial","Telefono",
        "NombreRegion","NombreCategoria","Periodo","IdEntidad","CreateDate","CreateUser"
    ];

    let file = File::open(input_file)?;
    let mut reader = BufReader::new(file);

    // read header line
    let mut header_line = String::new();
    reader.read_line(&mut header_line)?;
    let headers: Vec<String> = header_line.trim_end().split(',').map(|s| s.to_string()).collect();

    // keep window
    let mut deque: VecDeque<(usize, String)> = VecDeque::with_capacity(context * 2 + 1);
    let mut line_no = 1; // header already read

    for line in reader.lines() {
        line_no += 1;
        let l = line?;
        if deque.len() == deque.capacity() {
            deque.pop_front();
        }
        deque.push_back((line_no, l.clone()));

        if line_no == target_line + context {
            break;
        }
    }

    let start = target_line.saturating_sub(context);
    println!("--- Inspect lines {} .. {} (context {}) ---", start.max(2), target_line + context, context);

    for (ln, s) in deque {
        let prefix = if ln == target_line { ">>" } else { "  " };
        println!("{} {:>8}: {}", prefix, ln, s);

        if validate_model {
            // parse the single CSV line without headers
            let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(s.as_bytes());
            if let Some(result) = rdr.records().next() {
                match result {
                    Ok(record) => {
                        let violations = validate_moros_record(&record, &expected_cols);
                        if !violations.is_empty() {
                            println!("    => Model violations:");
                            for v in violations.iter() {
                                println!("       - {}", v);
                            }
                        } else {
                            println!("    => Model: OK");
                        }
                    }
                    Err(e) => {
                        println!("    => CSV parse error: {}", e);
                    }
                }
            } else {
                println!("    => Empty record");
            }
        }
    }

    Ok(())
}

// ---- Agregar funciones faltantes ----

fn find_missing_key(input_file: &str, key_column: &str, max_report: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_path(input_file)?;
    let headers = rdr.headers()?.clone();
    let idx = headers.iter().position(|h| h == key_column).ok_or("Key column not found")?;
    let mut line_no = 1; // header
    let mut found = 0;

    for result in rdr.records() {
        line_no += 1;
        let rec = result?;
        let val = rec.get(idx).unwrap_or("").trim();
        if val.is_empty() {
            println!("Missing key at line {}", line_no);
            found += 1;
            if found >= max_report { break; }
        }
    }
    if found == 0 { println!("No missing keys for column '{}'", key_column); }
    Ok(())
}

fn find_oversize_items(input_file: &str, threshold_bytes: usize, max_report: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_path(input_file)?;
    let headers = rdr.headers()?.clone();
    let mut line_no = 1; // header
    let mut reported = 0;

    for result in rdr.records() {
        line_no += 1;
        let rec = result?;
        // approximate size = sum of byte lengths of fields + small overhead
        let size: usize = rec.iter().map(|f| f.as_bytes().len()).sum::<usize>() + rec.len() * 2;
        if size >= threshold_bytes {
            println!("Oversize line {}: approx {} bytes (cols {})", line_no, size, rec.len());
            for (i, field) in rec.iter().enumerate().take(10) {
                println!("  {}: len={} preview='{}'", headers.get(i).unwrap_or(&format!("col{}", i)), field.len(), &field.chars().take(200).collect::<String>());
            }
            reported += 1;
            if reported >= max_report { break; }
        }
    }

    if reported == 0 { println!("No items >= {} bytes found", threshold_bytes); }
    Ok(())
}

fn find_invalid_numeric(input_file: &str, cols: &str, max_report: usize) -> Result<(), Box<dyn std::error::Error>> {
    let cols_vec: Vec<&str> = cols.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    let mut rdr = csv::Reader::from_path(input_file)?;
    let headers = rdr.headers()?.clone();
    let mut idxs = Vec::new();
    for c in &cols_vec {
        let pos = headers.iter().position(|h| h == *c).ok_or(format!("Column '{}' not found in header", c))?;
        idxs.push((c.to_string(), pos));
    }

    let mut line_no = 1;
    let mut reported = 0;
    for result in rdr.records() {
        line_no += 1;
        let rec = result?;
        for (colname, pos) in &idxs {
            let val = rec.get(*pos).unwrap_or("").trim();
            if !val.is_empty() {
                if val.parse::<i128>().is_err() && val.parse::<f64>().is_err() {
                    println!("Invalid numeric at line {} col '{}' value='{}'", line_no, colname, val);
                    reported += 1;
                    if reported >= max_report { return Ok(()); }
                }
            }
        }
    }

    if reported == 0 { println!("No invalid numeric values found in columns: {}", cols_vec.join(",")); }
    Ok(())
}
