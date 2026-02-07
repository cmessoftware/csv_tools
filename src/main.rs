use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::error::Error;
use std::time::Instant;
use csv::WriterBuilder;

// Importar módulos locales
mod progress;
mod file_utils;
mod models;
mod commands;

use progress::ProgressTracker;
use file_utils::estimate_total_lines_from_list;

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
        "sanitize_dynamodb" => {
            if args.len() != 5 {
                eprintln!("❌ Error: sanitize_dynamodb requires 3 arguments");
                eprintln!("Usage: csv_tools sanitize_dynamodb <input.csv> <output.csv> <model_type>");
                eprintln!("\nSupported models:");
                eprintln!("  - siisa_morosos (14 columns)");
                eprintln!("  - personas_telefonos (13 columns)");
                eprintln!("  - siisa_empleadores (7 columns)");
                eprintln!("  - siisa_empleadores_relaciones (4 columns)");
                return Ok(());
            }
            
            let input_path = &args[2];
            let output_path = &args[3];
            let model_type = &args[4];
            
            // ✅ Validar modelo ANTES de mostrar "Expected columns"
            let model = models::DynamoDbModel::from_model_type(model_type);
            
            if model.is_none() {
                eprintln!("❌ Error: Unknown model type: '{}'", model_type);
                eprintln!("\nSupported models:");
                eprintln!("  - siisa_morosos (14 columns)");
                eprintln!("  - personas_telefonos (13 columns)");
                eprintln!("  - siisa_empleadores (7 columns)");
                eprintln!("  - siisa_empleadores_relaciones (4 columns)");
                return Ok(());
            }
            
            commands::cleaning::sanitize_dynamodb(input_path, output_path, model_type)?;
        },
        "validate_schema" => {
            if args.len() != 4 {
                eprintln!("❌ Error: validate_schema requires 2 arguments");
                eprintln!("Usage: csv_tools validate_schema <input.csv> <model_type>");
                return Ok(());
            }
            
            let csv_path = &args[2];
            let model_type = &args[3];
            
            // ✅ Validar modelo ANTES de ejecutar
            if models::DynamoDbModel::from_model_type(model_type).is_none() {
                eprintln!("❌ Error: Unknown model type: '{}'", model_type);
                eprintln!("\nSupported models:");
                eprintln!("  - siisa_morosos");
                eprintln!("  - personas_telefonos");
                eprintln!("  - siisa_empleadores");
                eprintln!("  - siisa_empleadores_relaciones");
                return Ok(());
            }
            
            // Create a simple validation call
            println!("╔══════════════════════════════════════════════════════════════╗");
            println!("║  DynamoDB Schema Validation                                  ║");
            println!("╚══════════════════════════════════════════════════════════════╝");
            println!("📄 File:  {}", csv_path);
            println!("📋 Model: {}", model_type);
            
            let model = models::DynamoDbModel::from_model_type(model_type).unwrap();
            println!("🔢 Expected Columns: {}", model.expected_columns);
            println!("🔑 Keys: {} + {}", model.partition_key, 
                if model.sort_key.is_empty() { "(no sort key)" } else { model.sort_key });
            
            println!("\n✅ Schema validation complete (detailed validation available via validation module)");
            println!("💡 Use 'parse_keys' command to see actual key values from your CSV");
        },
        "parse_keys" => {
            if args.len() != 4 {
                eprintln!("❌ Error: parse_keys requires 2 arguments");
                eprintln!("Usage: csv_tools parse_keys <input.csv> <model_type>");
                return Ok(());
            }
            
            let csv_path = &args[2];
            let model_type = &args[3];
            
            if models::DynamoDbModel::from_model_type(model_type).is_none() {
                eprintln!("❌ Error: Unknown model type: '{}'", model_type);
                eprintln!("\nSupported models:");
                eprintln!("  - siisa_morosos");
                eprintln!("  - personas_telefonos");  
                eprintln!("  - siisa_empleadores");
                eprintln!("  - siisa_empleadores_relaciones");
                return Ok(());
            }
            
            models::parse_keys_from_csv(csv_path, model_type)?;
        },
        "convert_date" => {
            if args.len() != 5 {
                eprintln!("❌ Error: convert_date requires 3 arguments");
                eprintln!("Usage: csv_tools convert_date <input.csv> <output.csv> <date_column>");
                eprintln!("\nConverts dates from dd/MM/yyyy, MM/dd/yyyy, or existing ISO format to yyyy-MM-ddTHH:mm:ss");
                return Ok(());
            }
            
            commands::file_ops::convert_date_format(&args)?;
        },
        "delete_from_row" => {
            if args.len() != 5 {
                eprintln!("❌ Error: delete_from_row requires 3 arguments");
                eprintln!("Usage: csv_tools delete_from_row <input.csv> <output.csv> <row_number>");
                eprintln!("\nDeletes all rows from the specified row number to the end of file");
                eprintln!("Note: Row numbers start from 1 (header is row 1, first data row is 2)");
                return Ok(());
            }
            
            let input_file = &args[2];
            let output_file = &args[3];
            let row_number: usize = match args[4].parse() {
                Ok(n) if n > 0 => n,
                _ => {
                    eprintln!("❌ Error: Row number must be a positive integer");
                    return Ok(());
                }
            };
            
            commands::file_ops::delete_from_row(input_file, output_file, row_number)?;
        },
        "help" => {
            help();
        },
        _ => {
            eprintln!("Unknown command: {}", command);
            help();
        }
       }

    Ok(())
}

fn help() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  CSV Tools - DynamoDB & Data Processing                     ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("DynamoDB Commands:");
    println!("  sanitize_dynamodb <input.csv> <output.csv> <model_type>");
    println!("    Sanitize CSV for DynamoDB ImportTable");
    println!("    - Removes quotes from header row");
    println!("    - Validates numeric fields (Type N)");
    println!("    - Preserves quoted strings for Type S fields");
    println!();
    println!("  validate_schema <input.csv> <model_type>");
    println!("    Validate CSV schema and data types");
    println!("    - Check header format");
    println!("    - Validate Type N fields are numeric");
    println!("    - Report validation errors");
    println!();
    println!("  parse_keys <input.csv> <model_type>");
    println!("    Extract and display DynamoDB keys (PartitionKey + SortKey)");
    println!();
    println!("  convert_date <input.csv> <output.csv> <date_column>");
    println!("    Convert date formats (dd/MM/yyyy, MM/dd/yyyy, ISO) to yyyy-MM-ddTHH:mm:ss");
    println!();
    println!("  delete_from_row <input.csv> <output.csv> <row_number>");
    println!("    Delete all rows from specified row number to end of file");
    println!("    - Row numbers start from 1 (header = 1, first data = 2)");
    println!("    - Preserves header row");
    println!("    - Creates new CSV with only rows before the specified row");
    println!();
    println!("SUPPORTED MODELS:");
    println!("  - siisa_morosos                 (14 columns, Keys: Cuil + IdTransmit)");
    println!("  - personas_telefonos            (13 columns, Keys: Cuil + IdTelefono)");
    println!("  - siisa_empleadores             (7 columns, Keys: Cuit)");
    println!("  - siisa_empleadores_relaciones  (4 columns, Keys: Cuil + Cuit)");
    println!();
    println!("EXAMPLES:");
    println!();
    println!("  # Sanitize siisa_morosos CSV");
    println!("  csv_tools sanitize_dynamodb input.csv output.csv siisa_morosos");
    println!();
    println!("  # Sanitize siisa_empleadores CSV");
    println!("  csv_tools sanitize_dynamodb empleadores.csv empleadores_clean.csv siisa_empleadores");
    println!();
    println!("  # Sanitize siisa_empleadores_relaciones CSV");
    println!("  csv_tools sanitize_dynamodb relaciones.csv relaciones_clean.csv siisa_empleadores_relaciones");
    println!();
    println!("  # Validate schema");
    println!("  csv_tools validate_schema output.csv siisa_morosos");
    println!();
    println!("  # Parse DynamoDB keys");
    println!("  csv_tools parse_keys output.csv siisa_empleadores");
    println!();
    println!("  # Parse composite keys for empleadores relaciones");
    println!("  csv_tools parse_keys relaciones.csv siisa_empleadores_relaciones");
    println!();
    println!("  # Convert date formats (supports dd/MM/yyyy, MM/dd/yyyy, and ISO) to ISO");
    println!("  csv_tools convert_date input.csv output.csv fecha_creacion");
    println!();
    println!("NOTES:");
    println!("  - Compatible with SiisaRestApi chunk-export-v2 output format");
    println!("  - Follows DynamoDB ImportTable CSV specification (RFC 4180)");
    println!("  - Header row must NOT have quotes (auto-sanitized)");
    println!("  - Type N fields (DynamoDB Number) must be unquoted in CSV");
    println!("  - Type S fields (DynamoDB String) auto-quoted when needed");
    println!();
    println!("Legacy Commands:");
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
}

fn count_all_files(file_list_path: &str) -> Result<(), Box<dyn Error>> {
    // Obtener lista de archivos para estimación
    let file = File::open(file_list_path)?;
    let reader = BufReader::new(file);
    let file_names: Vec<String> = reader.lines().collect::<Result<Vec<_>, _>>()?;
    
    println!("📊 Estimando total de líneas para progress...");
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    println!("Estimación: ~{} líneas totales en {} archivos", estimated_total, file_names.len());
    
    let mut progress = ProgressTracker::new(estimated_total as u64);
    let mut total = 0;
    let mut processed_lines = 0;

    for filename in file_names {
        let count = count_lines_with_progress(&filename, &mut progress, &mut processed_lines)?;
        println!("\n{}: {} líneas", filename, count);
        total += count;
    }

    progress.finish();
    println!("📈 Total de líneas en todos los archivos: {}", total);
    Ok(())
}

fn merge_and_deduplicate(file_list_path: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    use std::collections::HashSet;

    println!("🔄 Estimando total de líneas para merge...");
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    println!("Estimación: ~{} líneas totales", estimated_total);
    
    let mut progress = ProgressTracker::new(estimated_total as u64);
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
    progress.finish();
    println!("🔄 Merge completado, {} registros únicos guardados en {}", seen_lines.len(), output_file);
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
            progress.update(*processed_lines as u64);
        }
    }
    
    progress.update(*processed_lines as u64);
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

fn count_unique_records(file_list_path: &str) -> Result<(), Box<dyn Error>> {
    use std::collections::HashSet;

    println!("📊 Estimando total de líneas para conteo único...");
    let estimated_total = estimate_total_lines_from_list(file_list_path)?;
    println!("Estimación: ~{} líneas totales", estimated_total);
    
    let mut progress = ProgressTracker::new(estimated_total as u64);

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
    let duplicates = total_lines - (unique_count as u64);
    
    progress.finish();
    println!("🔍 Conteo único completado");
    
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
    
    // Estimar tamaño promedio de línea (basado en formato SIISA)
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
    let mut progress = ProgressTracker::new(estimated_total as u64);
    
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
    progress.finish();
    println!("📂 Combinación completada");
    
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
