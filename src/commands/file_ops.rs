use csv::{Reader, ReaderBuilder, WriterBuilder, StringRecord, Writer};
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write, BufRead};
use std::collections::{HashMap, HashSet};
use regex::Regex;
use lazy_static::lazy_static;
use chrono::NaiveDateTime;

use crate::models::{
    get_dynamodb_key_columns
};

// Constantes
const EXPECTED_COLS: usize = 14; // siisa_morosos default

// ✅ FUNCIONES ACTIVAS (exportadas en commands/mod.rs)

/// Convierte fechas de múltiples formatos a formato ISO yyyy-MM-ddTHH:mm:ss
/// Soporta tanto HH:mm como HH:mm:ss (los segundos son opcionales)
/// Preserva fechas que ya están en formato ISO válido
/// Soporta formato europeo (dd/MM/yyyy), estadounidense (MM/dd/yyyy) e ISO existente
/// Sigue convenciones SiisaRestApi: CsvHelper-based parsing + structured error reporting
pub fn convert_date_format(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 5 {
        eprintln!("❌ Usage: csv_tools convert_date <input.csv> <output.csv> <date_column>");
        eprintln!("💡 Converts multiple date formats to ISO format yyyy-MM-ddTHH:mm:ss");
        eprintln!("💡 Supports European format: 'dd/MM/yyyy HH:mm[:ss]'");
        eprintln!("💡 Supports US format: 'MM/dd/yyyy HH:mm[:ss]'");
        eprintln!("💡 Preserves ISO format: 'yyyy-MM-ddTHH:mm[:ss]'");
        std::process::exit(1);
    }

    let input_file = &args[2];
    let output_file = &args[3];
    let date_column = &args[4];

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Date Format Converter (Multi-format → ISO)                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📄 Input CSV: {}", input_file);
    println!("📝 Output CSV: {}", output_file);
    println!("📅 Date column: {}", date_column);
    println!("🔄 European: dd/MM/yyyy HH:mm[:ss] → yyyy-MM-ddTHH:mm:ss");
    println!("🔄 US Format: MM/dd/yyyy HH:mm[:ss] → yyyy-MM-ddTHH:mm:ss");
    println!("✅ ISO Format: yyyy-MM-ddTHH:mm[:ss] → preserved");
    println!();

    let error_log_path = format!("{}.date_conversion_errors.log", output_file);
    let mut log = File::create(&error_log_path)?;

    writeln!(log, "# Date Format Conversion Error Log")?;
    writeln!(log, "# Input: {}", input_file)?;
    writeln!(log, "# Output: {}", output_file)?;
    writeln!(log, "# Date column: {}", date_column)?;
    writeln!(log, "# Source formats: dd/MM/yyyy, MM/dd/yyyy, yyyy-MM-ddT HH:mm[:ss]")?;
    writeln!(log, "# Target format: yyyy-MM-ddTHH:mm:ss")?;
    writeln!(log, "#")?;
    writeln!(log, "# Format: [LINE] STATUS | Details")?;
    writeln!(log, "# -------------------------------------------------------")?;

    let mut rdr = ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(input_file)?;

    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Necessary)
        .from_path(output_file)?;

    let headers = rdr.headers()?.clone();
    
    let date_col_idx = headers.iter()
        .position(|h| h.trim() == date_column)
        .ok_or_else(|| format!("Column '{}' not found in CSV", date_column))?;

    println!("📊 Column analysis:");
    println!("   Date column '{}' found at index {}", date_column, date_col_idx);
    println!();

    wtr.write_record(&headers)?;

    let mut total_processed = 0usize;
    let mut conversion_errors = 0usize;
    let mut successful_conversions = 0usize;
    let mut line_num = 2usize; // header is line 1

    println!("🔍 Processing records...");
    println!();

    for result in rdr.records() {
        total_processed += 1;
        
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                conversion_errors += 1;
                writeln!(
                    log,
                    "[LINE {}] ❌ PARSE_ERROR | CSV parsing failed: {}",
                    line_num, e
                )?;
                line_num += 1;
                continue;
            }
        };

        let original_date = record.get(date_col_idx).unwrap_or("").trim();
        
        if original_date.is_empty() {
            // Keep empty dates as is
            wtr.write_record(&record)?;
            successful_conversions += 1;
        } else {
            match convert_date_dd_mm_yyyy_to_iso(original_date) {
                Ok(iso_date) => {
                    // Create new record with converted date
                    let mut new_record_vec: Vec<String> = record.iter().map(|s| s.to_string()).collect();
                    new_record_vec[date_col_idx] = iso_date;
                    
                    let string_record = StringRecord::from(new_record_vec);
                    wtr.write_record(&string_record)?;
                    successful_conversions += 1;
                },
                Err(e) => {
                    conversion_errors += 1;
                    writeln!(
                        log,
                        "[LINE {}] ❌ DATE_CONVERSION_ERROR | Original='{}' | Error: {}",
                        line_num, original_date, e
                    )?;
                    writeln!(log, "  CSV: {}", serialize_record_for_log(&record))?;
                    writeln!(log, "")?;
                }
            }
        }

        if total_processed % 10_000 == 0 {
            print!("\r📊 Processed: {} | Converted: {} | Errors: {}", 
                total_processed, successful_conversions, conversion_errors);
            std::io::stdout().flush().ok();
        }

        line_num += 1;
    }

    wtr.flush()?;
    log.flush()?;

    println!("\r📊 Processed: {} | Converted: {} | Errors: {}", 
        total_processed, successful_conversions, conversion_errors);
    println!();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Date Conversion Summary                                     ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Input CSV:");
    println!("   Total records processed: {}", total_processed);
    println!();
    println!("📊 Output CSV:");
    println!("   Successfully converted: {} ✅", successful_conversions);
    println!("   Date conversion errors: {} ❌", conversion_errors);
    
    if conversion_errors > 0 {
        println!("   Error rate: {:.2}%", 
            (conversion_errors as f64 / total_processed as f64) * 100.0);
    }
    
    println!();
    println!("📝 Files created:");
    println!("   Converted CSV: {}", output_file);
    if conversion_errors > 0 {
        println!("   Error log: {}", error_log_path);
    }
    
    if conversion_errors > 0 {
        println!();
        println!("⚠️  WARNING: {} records had date conversion errors", conversion_errors);
        println!("   Review error log: {}", error_log_path);
        println!("   These records were SKIPPED in the output");
    } else {
        println!();
        println!("🎯 All dates successfully converted to ISO format ✅");
    }

    Ok(())
}

/// Convierte fecha de dd/MM/yyyy o MM/dd/yyyy HH:mm:ss o HH:mm a yyyy-MM-ddTHH:mm:ss
/// También preserva fechas que ya están en formato ISO válido
/// Soporta tanto formatos europeos (dd/MM/yyyy) como estadounidenses (MM/dd/yyyy)
fn convert_date_dd_mm_yyyy_to_iso(date_str: &str) -> Result<String, Box<dyn Error>> {
    // First, check if it's already in ISO format (yyyy-MM-ddTHH:mm:ss or yyyy-MM-ddTHH:mm)
    if let Ok(parsed_date) = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S") {
        return Ok(parsed_date.format("%Y-%m-%dT%H:%M:%S").to_string());
    }
    
    if let Ok(parsed_date) = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M") {
        return Ok(parsed_date.format("%Y-%m-%dT%H:%M:00").to_string()); // Add :00 for seconds
    }
    
    // Try European format with seconds: dd/MM/yyyy HH:mm:ss
    if let Ok(parsed_date) = NaiveDateTime::parse_from_str(date_str, "%d/%m/%Y %H:%M:%S") {
        return Ok(parsed_date.format("%Y-%m-%dT%H:%M:%S").to_string());
    }
    
    // Try European format without seconds: dd/MM/yyyy HH:mm
    if let Ok(parsed_date) = NaiveDateTime::parse_from_str(date_str, "%d/%m/%Y %H:%M") {
        return Ok(parsed_date.format("%Y-%m-%dT%H:%M:00").to_string()); // Add :00 for seconds
    }
    
    // Try US format with seconds: MM/dd/yyyy HH:mm:ss
    if let Ok(parsed_date) = NaiveDateTime::parse_from_str(date_str, "%m/%d/%Y %H:%M:%S") {
        return Ok(parsed_date.format("%Y-%m-%dT%H:%M:%S").to_string());
    }
    
    // Try US format without seconds: MM/dd/yyyy HH:mm
    if let Ok(parsed_date) = NaiveDateTime::parse_from_str(date_str, "%m/%d/%Y %H:%M") {
        return Ok(parsed_date.format("%Y-%m-%dT%H:%M:00").to_string()); // Add :00 for seconds
    }
    
    // If all formats fail, return error with helpful message including all supported formats
    Err(format!("Invalid date format '{}'. Expected formats: 'yyyy-MM-ddTHH:mm:ss', 'yyyy-MM-ddTHH:mm', 'dd/MM/yyyy HH:mm:ss', 'dd/MM/yyyy HH:mm', 'MM/dd/yyyy HH:mm:ss', or 'MM/dd/yyyy HH:mm'", date_str).into())
}

/// Sanitizador automático para DynamoDB con validación de schema
/// Sigue convenciones SiisaRestApi: CsvHelper-based parsing + structured error reporting
/// 
/// ✅ FIX: Preserva header original sin sanitización
pub fn sanitize_for_dynamodb_auto(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let output_file = &args[3];
    let model_type = args.get(4).map(String::as_str).unwrap_or("siisa_morosos");
    
    let expected_cols = args.get(5)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| {
            match model_type {
                "siisa_morosos" => 14,
                "personas_telefonos" => 13,
                _ => EXPECTED_COLS
            }
        });

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  DynamoDB Auto-Sanitizer (SiisaRestApi Compatible)          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📋 Model type: {}", model_type);
    println!("📋 Expected columns: {}", expected_cols);
    println!("📄 Input CSV: {}", input_file);
    println!("📝 Output CSV: {}", output_file);
    println!("🔧 Strategy: CsvHelper-based parsing + validate numeric fields");
    println!();

    let numeric_fields = get_numeric_fields_local(model_type)?;
    
    println!("🔑 DynamoDB Numeric Fields (Type: N):");
    for field in &numeric_fields {
        println!("   - {}", field);
    }
    println!();

    let error_log_path = format!("{}.sanitization_errors.log", output_file);
    let mut log = File::create(&error_log_path)?;

    writeln!(log, "# DynamoDB Auto-Sanitization Error Log")?;
    writeln!(log, "# Input: {}", input_file)?;
    writeln!(log, "# Output: {}", output_file)?;
    writeln!(log, "# Model: {}", model_type)?;
    writeln!(log, "# Expected columns: {}", expected_cols)?;
    writeln!(log, "# Strategy: CsvHelper parsing + validate numeric fields")?;
    writeln!(log, "#")?;
    writeln!(log, "# Numeric Fields (Type: N): {:?}", numeric_fields)?;
    writeln!(log, "#")?;
    writeln!(log, "# Format: [LINE] STATUS | Details")?;
    writeln!(log, "# -------------------------------------------------------")?;

    let mut rdr = ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(input_file)?;

    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::NonNumeric)
        .from_path(output_file)?;

    let mut total_processed = 0usize;
    let mut invalid_numeric_count = 0usize;
    let mut irreparable_count = 0usize;
    let mut line_num = 1usize;

    let headers = rdr.headers()?.clone();
    
    if headers.len() != expected_cols {
        writeln!(
            log,
            "[LINE {}] ⚠️  WARNING: Header has {} columns, expected {}",
            line_num, headers.len(), expected_cols
        )?;
    }
    
    wtr.write_record(&headers)?;
    
    let numeric_indices: Vec<(usize, String)> = numeric_fields
        .iter()
        .filter_map(|field| {
            headers.iter()
                .position(|h| h.trim() == field)
                .map(|idx| (idx, field.clone()))
        })
        .collect();

    println!("📊 Numeric field positions:");
    for (idx, field) in &numeric_indices {
        println!("   {} at index {}", field, idx);
    }
    println!();
    
    line_num += 1;

    println!("🔍 Processing records...");
    println!();

    for result in rdr.records() {
        total_processed += 1;
        
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                irreparable_count += 1;
                writeln!(
                    log,
                    "[LINE {}] ❌ PARSE_ERROR | CSV parsing failed: {}",
                    line_num, e
                )?;
                line_num += 1;
                continue;
            }
        };

        if record.len() != expected_cols {
            irreparable_count += 1;
            
            writeln!(
                log,
                "[LINE {}] ❌ IRREPARABLE_STRUCTURE | Columns: {} (expected {})",
                line_num,
                record.len(),
                expected_cols
            )?;
            writeln!(log, "  CSV: {}", serialize_record_for_log(&record))?;
            writeln!(log, "")?;
            
            line_num += 1;
            
            if total_processed % 10_000 == 0 {
                print!("\r📊 Processed: {} | Invalid Numeric: {} | Irreparable: {}", 
                    total_processed, invalid_numeric_count, irreparable_count);
                std::io::stdout().flush().ok();
            }
            
            continue;
        }

        let mut has_invalid_numeric = false;
        
        for (idx, field_name) in &numeric_indices {
            let value = record.get(*idx).unwrap_or("");
            
            if !is_valid_dynamodb_number_local(value) {
                has_invalid_numeric = true;
                
                writeln!(
                    log,
                    "[LINE {}] ❌ INVALID_NUMERIC | Field='{}' | Value='{}' | Expected: Type N (numeric)",
                    line_num,
                    field_name,
                    value
                )?;
            }
        }

        if has_invalid_numeric {
            invalid_numeric_count += 1;
            
            writeln!(
                log,
                "[LINE {}] ❌ SKIPPED | Record has non-numeric values in numeric fields",
                line_num
            )?;
            writeln!(log, "  CSV: {}", serialize_record_for_log(&record))?;
            writeln!(log, "")?;
            
            line_num += 1;
            
            if total_processed % 10_000 == 0 {
                print!("\r📊 Processed: {} | Invalid Numeric: {} | Irreparable: {}", 
                    total_processed, invalid_numeric_count, irreparable_count);
                std::io::stdout().flush().ok();
            }
            
            continue;
        }

        wtr.write_record(&record)?;

        if total_processed % 10_000 == 0 {
            print!("\r📊 Processed: {} | Invalid Numeric: {} | Irreparable: {}", 
                total_processed, invalid_numeric_count, irreparable_count);
            std::io::stdout().flush().ok();
        }

        line_num += 1;
    }

    wtr.flush()?;
    log.flush()?;

    println!("\r📊 Processed: {} | Invalid Numeric: {} | Irreparable: {}", 
        total_processed, invalid_numeric_count, irreparable_count);
    println!();

    let total_written = total_processed - invalid_numeric_count - irreparable_count;
    let total_removed = invalid_numeric_count + irreparable_count;

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Auto-Sanitization Summary                                   ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Input CSV:");
    println!("   Total records processed: {}", total_processed);
    println!();
    println!("📊 Output CSV:");
    println!("   Records written: {} ✅", total_written);
    println!("   Header preserved: ✅ (no modifications)");
    
    println!();
    println!("📊 Rejected Records:");
    
    if invalid_numeric_count > 0 {
        println!("   ❌ Invalid numeric fields: {} ({:.2}%)", 
            invalid_numeric_count, 
            (invalid_numeric_count as f64 / total_processed as f64) * 100.0);
    }
    
    if irreparable_count > 0 {
        println!("   ❌ Irreparable structure: {} ({:.2}%)", 
            irreparable_count, 
            (irreparable_count as f64 / total_processed as f64) * 100.0);
    }
    
    println!("   Total removed: {} ({:.2}%)", 
        total_removed, 
        (total_removed as f64 / total_processed as f64) * 100.0);
    
    println!();
    println!("📝 Files created:");
    println!("   Clean CSV: {}", output_file);
    println!("   Error log: {}", error_log_path);
    
    println!();
    println!("🎯 DynamoDB Import Ready:");
    println!("   Expected records in DynamoDB: {}", total_written);
    println!("   Expected columns per record: {}", expected_cols);
    println!("   All numeric fields validated ✅");
    
    if total_removed > 0 {
        println!();
        println!("⚠️  WARNING: {} rows were removed:", total_removed);
        
        if invalid_numeric_count > 0 {
            println!("   - {} records with non-numeric values in Type N fields", invalid_numeric_count);
        }
        
        if irreparable_count > 0 {
            println!("   - {} records with irreparable structure or parsing errors", irreparable_count);
        }
        
        println!("   Review error log: {}", error_log_path);
        println!("   These records will NOT be imported to DynamoDB");
    }

    Ok(())
}

/// Serializa un StringRecord para logging SIN re-quotar
fn serialize_record_for_log(record: &csv::StringRecord) -> String {
    use std::io::Cursor;
    
    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Necessary)
        .from_writer(Cursor::new(Vec::new()));
    
    if let Err(_) = wtr.write_record(record) {
        return format!("{:?}", record.as_slice());
    }
    
    if let Err(_) = wtr.flush() {
        return format!("{:?}", record.as_slice());
    }
    
    let inner = wtr.into_inner()
        .ok()
        .and_then(|cursor| String::from_utf8(cursor.into_inner()).ok())
        .unwrap_or_else(|| format!("{:?}", record.as_slice()));
    
    inner.trim_end().to_string()
}

/// Retorna lista de campos numéricos según modelo DynamoDB (LOCAL)
fn get_numeric_fields_local(model_type: &str) -> Result<Vec<String>, Box<dyn Error>> {
    match model_type {
        "siisa_morosos" => Ok(vec![
            "Cuil".to_string(),
            "IdTransmit".to_string(),
            "NroDoc".to_string(),
            "IdCliente".to_string(),
            "IdRegion".to_string(),
            "Periodo".to_string(),
            "IdEntidad".to_string(),
        ]),
        _ => Err(format!("Unknown model type: {}", model_type).into())
    }
}

/// Validación estricta compatible con DynamoDB Number (LOCAL)
fn is_valid_dynamodb_number_local(value: &str) -> bool {
    let v = value.trim();

    if v.is_empty() {
        return false;
    }

    lazy_static! {
        static ref RE: Regex = Regex::new(
            r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?$"
        ).unwrap();
    }

    if !RE.is_match(v) {
        return false;
    }

    let significant = v.replace('.', "").replace('-', "");
    significant.len() <= 38
}

// ✅ FUNCIONES LEGACY COMENTADAS (evitar duplicación)

/*
/// ❌ DUPLICADO - Comentado para evitar error E0428
#[allow(dead_code)]
pub fn deduplicate_by_dynamodb_keys(args: &[String]) -> Result<(), Box<dyn Error>> {
    // ...código legacy...
}
*/

// ❌ FUNCIONES LEGACY (suprimir warnings hasta implementación futura)

#[allow(dead_code)]
pub fn deduplicate_by_dynamodb_keys(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let output_file = &args[3];
    let model_type = args.get(4).map(String::as_str).unwrap_or("siisa_morosos");
    let duplicates_log = args.get(5).map(String::as_str).unwrap_or("duplicates_removed.log");
    
    let error_log = format!("{}_errors.log", output_file.trim_end_matches(".csv"));

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  DynamoDB Key Deduplication (SiisaRestApi Compatible)       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📋 DynamoDB Model: {}", model_type);
    println!("📄 Input CSV: {}", input_file);
    println!("📝 Output CSV: {}", output_file);
    println!("📝 Duplicates log: {}", duplicates_log);
    println!("📝 Errors log: {}", error_log);
    println!("🔄 Strategy: Keep LAST occurrence (PutItem behavior)");
    println!();

    // Obtener columnas de clave DynamoDB según modelo
    let (pk_name, sk_name_opt) = get_dynamodb_key_columns(model_type)?;
    
    println!("🔑 DynamoDB Primary Key Schema:");
    println!("   PartitionKey: {} (Type: N)", pk_name);
    match &sk_name_opt {
        Some(sk) => println!("   SortKey: {} (Type: N)", sk),
        None => println!("   SortKey: (none)")
    }
    println!();

    // Paso 1: Validar schema
    println!("🔍 Step 1/3: Validating CSV schema...");
    
    let mut rdr = Reader::from_path(input_file)?;
    let headers = rdr.headers()?.clone();
    
    let pk_idx = headers.iter()
        .position(|h| h == pk_name)
        .ok_or_else(|| format!("Column '{}' not found in CSV", pk_name))?;
    
    let sk_idx = match &sk_name_opt {
        Some(sk_name) => Some(headers.iter()
            .position(|h| h == sk_name)
            .ok_or_else(|| format!("Column '{}' not found in CSV", sk_name))?),
        None => None
    };

    println!("✅ Schema matches {}DynamoDbModel", model_type);
    println!();
    println!("📊 Column positions:");
    println!("   {} at index {}", pk_name, pk_idx);
    match (sk_name_opt.as_ref(), sk_idx) {
        (Some(sk_name), Some(idx)) => println!("   {} at index {}", sk_name, idx),
        _ => println!("   (no sort key)")
    }
    println!();

    // Paso 2: Cargar en memoria con error recovery
    println!("🔍 Step 2/3: Loading records into memory (required for deduplication)...");

    let mut records_map: HashMap<String, StringRecord> = HashMap::new();
    let mut total_processed = 0;
    let mut total_errors = 0;
    let mut duplicate_count = 0;

    // ✅ Writer para errores (siguiendo patrón ChunkStateManager)
    let error_file_handle = File::create(&error_log)?;
    let mut error_writer = BufWriter::new(error_file_handle);
    
    writeln!(error_writer, "# CSV Deduplication Error Log")?;
    writeln!(error_writer, "# Input: {}", input_file)?;
    writeln!(error_writer, "# Model: {}", model_type)?;
    writeln!(error_writer, "# Strategy: Skip malformed records + continue processing")?;
    writeln!(error_writer, "#")?;
    writeln!(error_writer, "# Format: [LINE] ERROR_TYPE | Details")?;
    writeln!(error_writer, "# -------------------------------------------------------")?;

    // Reset reader para leer datos
    let mut rdr = Reader::from_path(input_file)?;
    let expected_len = rdr.headers()?.len();

    for result in rdr.records() {
        total_processed += 1;

        // ✅ CRITICAL: Error recovery pattern (SiisaRestApi convention)
        let record = match result {
            Ok(rec) => {
                // Validar longitud de columnas
                if rec.len() != expected_len {
                    total_errors += 1;
                    
                    // Log error detallado
                    writeln!(error_writer, 
                        "[LINE {}] LENGTH_MISMATCH | Expected {} columns, found {} | Position: {:?}",
                        total_processed, expected_len, rec.len(), rec.position())?;
                    
                    writeln!(error_writer, "  Raw data: {:?}", rec.as_slice())?;
                    writeln!(error_writer, "")?;
                    
                    // ⚠️ SKIP este registro y continuar (graceful degradation)
                    if total_processed % 10_000 == 0 {
                        print!("\r📊 Processed: {} | Errors: {} | Unique: {} | Duplicates: {}", 
                            total_processed, total_errors, records_map.len(), duplicate_count);
                        std::io::stdout().flush().ok();
                    }
                    continue;
                }
                rec
            },
            Err(e) => {
                total_errors += 1;
                
                // Log error detallado
                writeln!(error_writer, 
                    "[LINE {}] CSV_PARSE_ERROR | {}",
                    total_processed, e)?;
                writeln!(error_writer, "")?;
                
                // ⚠️ SKIP este registro y continuar
                if total_processed % 10_000 == 0 {
                    print!("\r📊 Processed: {} | Errors: {} | Unique: {} | Duplicates: {}", 
                        total_processed, total_errors, records_map.len(), duplicate_count);
                    std::io::stdout().flush().ok();
                }
                continue;
            }
        };

        // Extraer claves DynamoDB
        let pk_value = &record[pk_idx];
        let sk_value = sk_idx.map(|idx| &record[idx]);

        // ✅ Validar que las claves no estén vacías
        if pk_value.is_empty() || (sk_value.is_some() && sk_value.unwrap().is_empty()) {
            total_errors += 1;
            
            writeln!(error_writer, 
                "[LINE {}] EMPTY_KEY | PartitionKey='{}', SortKey='{}'",
                total_processed, pk_value, sk_value.unwrap_or("(none)"))?;
            writeln!(error_writer, "  Raw data: {:?}", record.as_slice())?;
            writeln!(error_writer, "")?;
            
            continue;
        }

        // Crear clave compuesta (PartitionKey + SortKey)
        // Sigue patrón CompositePrimaryKey de SiisaRestApi
        let composite_key = match sk_value {
            Some(sk) => format!("{}#{}", pk_value, sk),
            None => pk_value.to_string()
        };

        // ✅ STRATEGY: Keep LAST occurrence (matches DynamoDB PutItem behavior)
        if records_map.contains_key(&composite_key) {
            duplicate_count += 1;
        }
        
        records_map.insert(composite_key, record);

        // Mostrar progreso cada 10,000 registros (siguiendo convención SiisaRestApi)
        if total_processed % 10_000 == 0 {
            print!("\r📊 Processed: {} | Errors: {} | Unique: {} | Duplicates: {}", 
                total_processed, total_errors, records_map.len(), duplicate_count);
            std::io::stdout().flush().ok();
        }
    }

    error_writer.flush()?;

    println!("\r📊 Processed: {} | Errors: {} | Unique: {} | Duplicates: {}", 
        total_processed, total_errors, records_map.len(), duplicate_count);
    println!();
    println!("✅ Complete: {} records in memory", records_map.len());
    println!();

    // Paso 3: Escribir registros únicos
    println!("🔍 Step 3/3: Writing deduplicated records...");
    println!();
    
    // Log de duplicados removidos (structured logging pattern)
    let dup_file = File::create(duplicates_log)?;
    let mut dup_writer = BufWriter::new(dup_file);
    
    writeln!(dup_writer, "# DynamoDB Key Deduplication Summary")?;
    writeln!(dup_writer, "# Input: {}", input_file)?;
    writeln!(dup_writer, "# Output: {}", output_file)?;
    writeln!(dup_writer, "# Model: {}", model_type)?;
    writeln!(dup_writer, "# Strategy: Keep LAST occurrence (PutItem behavior)")?;
    writeln!(dup_writer, "#")?;
    writeln!(dup_writer, "# Total processed: {}", total_processed)?;
    writeln!(dup_writer, "# Unique records: {}", records_map.len())?;
    writeln!(dup_writer, "# Duplicates removed: {}", duplicate_count)?;
    writeln!(dup_writer, "# Errors (skipped): {}", total_errors)?;
    writeln!(dup_writer, "# -------------------------------------------------------")?;
    dup_writer.flush()?;

    println!("💾 Writing deduplicated records to: {}", output_file);
    println!();

    let mut wtr = Writer::from_path(output_file)?;
    wtr.write_record(&headers)?;

    let mut written = 0;
    for record in records_map.values() {
        wtr.write_record(record)?;
        written += 1;

        if written % 10_000 == 0 {
            print!("\r📊 Written: {} records", written);
            std::io::stdout().flush().ok();
        }
    }

    wtr.flush()?;

    println!("\r📊 Written: {} records", written);
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Deduplication Summary                                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Input CSV:");
    println!("   Total records processed: {}", total_processed);
    if total_errors > 0 {
        println!("   ⚠️  Malformed records (skipped): {} ({:.2}%)", 
            total_errors, (total_errors as f64 / total_processed as f64) * 100.0);
    }
    println!();
    println!("📊 Output CSV:");
    println!("   Unique records: {} ✅", records_map.len());
    println!("   Duplicates removed: {} ({:.2}%)", 
        duplicate_count, (duplicate_count as f64 / total_processed as f64) * 100.0);
    if total_errors > 0 {
        println!("   Errors skipped: {} ({:.2}%)", 
            total_errors, (total_errors as f64 / total_processed as f64) * 100.0);
    }
    
    let total_removed = duplicate_count + total_errors;
    println!("   Total removed: {} ({:.2}%)", 
        total_removed, (total_removed as f64 / total_processed as f64) * 100.0);
    println!();
    println!("📝 Files created:");
    println!("   Clean CSV: {}", output_file);
    println!("   Duplicates log: {}", duplicates_log);
    if total_errors > 0 {
        println!("   ⚠️  Errors log: {} ({} malformed records)", error_log, total_errors);
    }
    println!();
    println!("🎯 DynamoDB Import Ready:");
    println!("   Expected records in DynamoDB: {}", records_map.len());
    println!("   No overwrites will occur (all keys unique)");
    
    if total_errors > 0 {
        println!();
        println!("⚠️  WARNING: {} malformed records were skipped", total_errors);
        println!("   Review error log for details: {}", error_log);
        println!("   These records will NOT be imported to DynamoDB");
    }

    Ok(())
}

/// Clean duplicate headers from CSV file
pub fn clean_headers(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let output_file = &args[3];
    
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
    println!("✅ Header cleanup complete: {}", output_file);
    Ok(())
}

/// Filter CSV rows by column value
pub fn filter_rows(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let output_file = &args[3];
    let column_name = &args[4];
    let value = &args[5];
    
    let input = File::open(input_file)?;
    let mut rdr = Reader::from_reader(input);
    let headers = rdr.headers()?.clone();
    
    let column_index = headers.iter()
        .position(|h| h == column_name)
        .ok_or_else(|| format!("Column '{}' not found", column_name))?;

    let mut wtr = Writer::from_path(output_file)?;
    wtr.write_record(&headers)?;

    for result in rdr.records() {
        let record = result?;
        if record.get(column_index).unwrap_or("") == value {
            wtr.write_record(&record)?;
        }
    }

    wtr.flush()?;
    println!("✅ Filtering complete: {}", output_file);
    Ok(())
}

/// Count lines in a single CSV file
pub fn count_lines(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    
    let file = File::open(input_file)?;
    let reader = BufReader::new(file);
    let line_count = reader.lines().count();
    
    println!("📊 Total lines in {}: {}", input_file, line_count);
    Ok(())
}

/// Count lines across multiple CSV files
pub fn count_all_files(args: &[String]) -> Result<(), Box<dyn Error>> {
    let file_list = &args[2];
    
    let file = File::open(file_list)?;
    let reader = BufReader::new(file);
    let mut total = 0;

    for line in reader.lines() {
        let filename = line?;
        let f = File::open(&filename)?;
        let r = BufReader::new(f);
        let count = r.lines().count();
        println!("{}: {} lines", filename, count);
        total += count;
    }

    println!("\n📊 Total lines across all files: {}", total);
    Ok(())
}

/// Count unique records across multiple files (in-memory)
pub fn count_unique_records(args: &[String]) -> Result<(), Box<dyn Error>> {
    let file_list = &args[2];
    
    let file = File::open(file_list)?;
    let reader = BufReader::new(file);
    let mut seen_lines = HashSet::new();

    for line in reader.lines() {
        let filename = line?;
        let f = File::open(&filename)?;
        let r = BufReader::new(f);
        
        for (i, file_line) in r.lines().enumerate() {
            if i == 0 { continue; } // Skip header
            seen_lines.insert(file_line?);
        }
    }

    println!("📊 Unique records: {}", seen_lines.len());
    Ok(())
}

/// Merge multiple CSV files without deduplication
pub fn merge_files(args: &[String]) -> Result<(), Box<dyn Error>> {
    let file_list = &args[2];
    let output_file = &args[3];
    
    let file = File::open(file_list)?;
    let reader = BufReader::new(file);
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
                writer.write_all(line_content.as_bytes())?;
                writer.write_all(b"\n")?;
            }
        }
    }

    writer.flush()?;
    println!("✅ Merge complete: {}", output_file);
    Ok(())
}

/// Merge and deduplicate CSV files (in-memory)
pub fn merge_and_deduplicate(args: &[String]) -> Result<(), Box<dyn Error>> {
    let file_list = &args[2];
    let output_file = &args[3];
    
    let file = File::open(file_list)?;
    let reader = BufReader::new(file);
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
    println!("✅ Merge + dedup complete: {} unique records", seen_lines.len());
    Ok(())
}

/// External merge sort for large CSV files
pub fn external_merge_dedup(_args: &[String]) -> Result<(), Box<dyn Error>> {
    println!("⚠️  External merge/dedup not yet implemented");
    println!("   Use merge_dedup for files that fit in RAM");
    Ok(())
}

/// Estimate memory required for in-memory deduplication
pub fn estimate_memory_usage(args: &[String]) -> Result<(), Box<dyn Error>> {
    let file_list = &args[2];
    
    let file = File::open(file_list)?;
    let reader = BufReader::new(file);
    let mut total_size = 0u64;

    for line in reader.lines() {
        let filename = line?;
        let metadata = std::fs::metadata(&filename)?;
        total_size += metadata.len();
    }

    let estimated_ram = (total_size as f64 * 1.5) / (1024.0 * 1024.0 * 1024.0);
    
    println!("📊 Total CSV size: {:.2} GB", total_size as f64 / (1024.0 * 1024.0 * 1024.0));
    println!("📊 Estimated RAM needed: {:.2} GB", estimated_ram);
    
    if estimated_ram > 16.0 {
        println!("⚠️  WARNING: May require external sort");
    }
    
    Ok(())
}


/// Compare first N rows of two CSV files
pub fn compare_first_n(args: &[String]) -> Result<(), Box<dyn Error>> {
    let file1 = &args[2];
    let file2 = &args[3];
    let num_rows: usize = args[4].parse()?;
    
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
    } else {
        println!("✅ Headers match");
    }

    let mut differences = 0;
    for i in 1..=num_rows {
        let line1 = lines1.next().unwrap_or(Ok(String::new()))?;
        let line2 = lines2.next().unwrap_or(Ok(String::new()))?;

        if line1 != line2 {
            println!("❌ Difference at line {}", i + 1);
            differences += 1;
        }
    }

    if differences == 0 {
        println!("✅ No differences in first {} rows", num_rows);
    }
    
    Ok(())
}

/// Show last N rows of CSV file
pub fn tail_csv(args: &[String]) -> Result<(), Box<dyn Error>> {
    use std::collections::VecDeque;
    
    let input_file = &args[2];
    let num_rows: usize = args[3].parse()?;
    
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

/// Show first N rows of CSV file
pub fn head_csv(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let num_rows: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
    
    let file = File::open(input_file)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    
    // Show header
    if let Some(Ok(header)) = lines.next() {
        println!("{}", header);
    }
    
    // Show N data rows
    let mut count = 0;
    for line in lines {
        if count >= num_rows { break; }
        println!("{}", line?);
        count += 1;
    }
    
    println!("\n📊 Showing {} rows", count);
    Ok(())
}

/// Validate CSV against DynamoDB schema
/// Valida estructura y campos numéricos
pub fn validate_dynamodb_schema(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let model_type = args.get(3).map(String::as_str).unwrap_or("siisa_morosos");

    println!("🔍 Validating {} against DynamoDB schema...", input_file);

    // ✅ FIX 1: Usar función local
    let numeric_fields = get_numeric_fields_local(model_type)?;

    // Archivo log de errores
    let mut error_log = File::create(format!("{}.schema_errors.log", input_file))?;

    // Reader en modo flexible para capturar errores de estructura
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)               // Permite detectar filas con menos/más columnas
        .from_path(input_file)?;

    let headers = rdr.headers()?.clone();
    let header_len = headers.len();

    // Ubicar columnas numéricas
    let numeric_indices: Vec<(usize, String)> = numeric_fields
        .iter()
        .filter_map(|field| {
            headers
                .iter()
                .position(|h| h == field)
                .map(|idx| (idx, field.clone()))
        })
        .collect();

    writeln!(error_log, "HEADER COUNT: {}", header_len)?;
    println!("📌 Column count: {}", header_len);

    let mut line_num = 2usize; // header es línea 1
    let mut total_errors = 0;

    for result in rdr.records() {
        let record = match result {
            Ok(x) => x,
            Err(e) => {
                writeln!(
                    error_log,
                    "[LINE {}] **CSV PARSE ERROR**: {}",
                    line_num, e
                )?;
                total_errors += 1;
                line_num += 1;
                continue;
            }
        };

        // 🔥 VALIDAR CANTIDAD DE COLUMNAS
        if record.len() != header_len {
            writeln!(
                error_log,
                "[LINE {}] COLUMN COUNT MISMATCH: got {} expected {} | RECORD={:?}",
                line_num,
                record.len(),
                header_len,
                record
            )?;
            total_errors += 1;
        }

        // 🔥 VALIDAR CAMPOS NUMÉRICOS
        for (idx, field_name) in &numeric_indices {
            let value = record.get(*idx).unwrap_or("");

            // ✅ FIX 2: Usar función local
            if !is_valid_dynamodb_number_local(value) {
                writeln!(
                    error_log,
                    "[LINE {}] INVALID NUMERIC {}='{}'",
                    line_num,
                    field_name,
                    value
                )?;
                total_errors += 1;
            }
        }

        line_num += 1;
    }

    println!("-----------------------------------------");
    println!("🔎 VALIDATION SUMMARY");
    println!("-----------------------------------------");
    println!("❌ Errors found: {}", total_errors);
    println!("📝 Log file    : {}.schema_errors.log", input_file);

    if total_errors == 0 {
        println!("✅ CSV is fully DynamoDB-Compatible (structure + numbers)");
    } else {
        println!("⚠ CSV has issues that WILL cause ImportTable to fail.");
    }

    Ok(())
}

/// Deduplicación simple por todas las columnas
pub fn deduplicate_csv(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        eprintln!("Usage: csv_tools deduplicate <input.csv> <output.csv>");
        std::process::exit(1);
    }
    
    let input_file = &args[2];
    let output_file = &args[3];
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  CSV Deduplication (All Columns)                            ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📄 Input: {}", input_file);
    println!("📝 Output: {}", output_file);
    println!();
    
    let mut rdr = Reader::from_path(input_file)?;
    let headers = rdr.headers()?.clone();
    
    let mut seen = HashSet::new();
    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Necessary)
        .from_path(output_file)?;
    
    wtr.write_record(&headers)?;
    
    let mut total = 0usize;
    let mut unique = 0usize;
    
    for result in rdr.records() {
        total += 1;
        let record = result?;
        
        let key = record.iter().collect::<Vec<_>>().join(",");
        
        if seen.insert(key) {
            unique += 1;
            wtr.write_record(&record)?;
        }
        
        if total % 10_000 == 0 {
            print!("\r📊 Processed: {} | Unique: {}", total, unique);
            std::io::stdout().flush().ok();
        }
    }
    
    wtr.flush()?;
    
    println!("\r📊 Processed: {} | Unique: {} | Duplicates: {}", 
        total, unique, total - unique);
    println!("✅ Deduplication complete");
    
    Ok(())
}

/// Deduplicación por claves DynamoDB compuestas
pub fn deduplicate_dynamodb(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 5 {
        eprintln!("Usage: csv_tools deduplicate_dynamodb <input.csv> <output.csv> <model_type>");
        eprintln!("Model types: siisa_morosos, personas_telefonos");
        std::process::exit(1);
    }

    let input_file = &args[2];
    let output_file = &args[3];
    let model_type = &args[4];

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  DynamoDB Deduplication (Composite Keys)                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📋 Model: {}", model_type);
    println!("📄 Input: {}", input_file);
    println!("📝 Output: {}", output_file);
    println!();

    let (pk_name, sk_name_opt) = get_dynamodb_key_columns(model_type)?;

    println!("🔑 DynamoDB Composite Key:");
    println!("   Partition Key: {}", pk_name);
    match &sk_name_opt {
        Some(sk) => println!("   Sort Key: {}", sk),
        None => println!("   Sort Key: (none)")
    }
    println!();

    let mut rdr = Reader::from_path(input_file)?;
    let headers = rdr.headers()?.clone();

    let pk_idx = headers.iter().position(|h| h == pk_name)
        .ok_or(format!("Partition key '{}' not found in CSV headers", pk_name))?;
    let sk_idx = match &sk_name_opt {
        Some(sk_name) => Some(headers.iter().position(|h| h == sk_name)
            .ok_or(format!("Sort key '{}' not found in CSV headers", sk_name))?),
        None => None
    };

    let mut records_map: HashMap<String, StringRecord> = HashMap::new();

    println!("🔍 Processing records...");
    println!();

    let mut total = 0usize;

    for result in rdr.records() {
        total += 1;
        let record = result?;

        let pk_value = record.get(pk_idx).unwrap_or("");
        let composite_key = match sk_idx {
            Some(idx) => {
                let sk_value = record.get(idx).unwrap_or("");
                format!("{}|{}", pk_value, sk_value)
            },
            None => pk_value.to_string()
        };

        records_map.insert(composite_key, record);

        if total % 10_000 == 0 {
            print!("\r📊 Processed: {} | Unique: {}", total, records_map.len());
            std::io::stdout().flush().ok();
        }
    }

    println!("\r📊 Processed: {} | Unique: {}", total, records_map.len());
    println!();

    println!("💾 Writing deduplicated output...");

    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Necessary)
        .from_path(output_file)?;

    wtr.write_record(&headers)?;

    for record in records_map.values() {
        wtr.write_record(record)?;
    }

    wtr.flush()?;

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Deduplication Summary                                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Total records processed: {}", total);
    println!("📊 Unique records written: {}", records_map.len());
    println!("📊 Duplicates removed: {}", total - records_map.len());
    println!("✅ Deduplication complete");

    Ok(())
}

/// Merge de múltiples CSV files con deduplicación
pub fn merge_csv_files(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        eprintln!("Usage: csv_tools merge <output.csv> <file1.csv> <file2.csv> [file3.csv...]");
        std::process::exit(1);
    }
    
    let output_file = &args[2];
    let input_files: Vec<&String> = args[3..].iter().collect();
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  CSV Files Merge with Deduplication                         ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📝 Output: {}", output_file);
    println!("📄 Input files: {}", input_files.len());
    println!();
    
    let mut all_records = HashMap::new();
    let mut headers: Option<csv::StringRecord> = None;
    let mut total_processed = 0usize;
    
    for (idx, input_file) in input_files.iter().enumerate() {
        println!("📖 Reading file {}/{}: {}", idx + 1, input_files.len(), input_file);
        
        let mut rdr = Reader::from_path(input_file)?;
        
        if headers.is_none() {
            headers = Some(rdr.headers()?.clone());
        }
        
        for result in rdr.records() {
            total_processed += 1;
            let record = result?;
            
            let key = record.iter().collect::<Vec<_>>().join(",");
            
            all_records.insert(key, record);
            
            if total_processed % 10_000 == 0 {
                print!("\r   📊 Processed: {} | Unique: {}", total_processed, all_records.len());
                std::io::stdout().flush().ok();
            }
        }
        
        println!("\r   ✅ File {} complete", idx + 1);
    }
    
    println!();
    println!("💾 Writing merged output...");
    
    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Necessary)
        .from_path(output_file)?;
    
    if let Some(header) = headers {
        wtr.write_record(&header)?;
    }
    
    for record in all_records.values() {
        wtr.write_record(record)?;
    }
    
    wtr.flush()?;
    
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Merge Summary                                               ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Total records processed: {}", total_processed);
    println!("📊 Unique records written: {}", all_records.len());
    println!("📊 Duplicates removed: {}", total_processed - all_records.len());
    println!("✅ Merge complete");
    
    Ok(())
}

/// Split CSV en chunks de tamaño específico
pub fn split_csv(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 5 {
        eprintln!("Usage: csv_tools split <input.csv> <output_prefix> <chunk_size>");
        std::process::exit(1);
    }
    
    let input_file = &args[2];
    let output_prefix = &args[3];
    let chunk_size: usize = args[4].parse()
        .expect("chunk_size must be a positive integer");
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  CSV File Splitter                                          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📄 Input: {}", input_file);
    println!("📦 Chunk size: {} records", chunk_size);
    println!();
    
    let mut rdr = Reader::from_path(input_file)?;
    let headers = rdr.headers()?.clone();
    
    let mut chunk_num = 1usize;
    let mut current_chunk_size = 0usize;
    let mut total_processed = 0usize;
    
    let chunk_file = format!("{}_{:03}.csv", output_prefix, chunk_num);
    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Necessary)
        .from_path(&chunk_file)?;
    
    wtr.write_record(&headers)?;
    
    println!("📝 Writing chunk {}: {}", chunk_num, chunk_file);
    
    for result in rdr.records() {
        let record = result?;
        total_processed += 1;
        current_chunk_size += 1;
        
        wtr.write_record(&record)?;
        
        if current_chunk_size >= chunk_size {
            wtr.flush()?;
            println!("   ✅ Chunk {} complete ({} records)", chunk_num, current_chunk_size);
            
            chunk_num += 1;
            current_chunk_size = 0;
            
            let chunk_file = format!("{}_{:03}.csv", output_prefix, chunk_num);
            wtr = WriterBuilder::new()
                .quote_style(csv::QuoteStyle::Necessary)
                .from_path(&chunk_file)?;
            
            wtr.write_record(&headers)?;
            println!("📝 Writing chunk {}: {}", chunk_num, chunk_file);
        }
        
        if total_processed % 10_000 == 0 {
            print!("\r   📊 Processed: {}", total_processed);
            std::io::stdout().flush().ok();
        }
    }
    
    if current_chunk_size > 0 {
        wtr.flush()?;
        println!("\r   ✅ Chunk {} complete ({} records)", chunk_num, current_chunk_size);
    }
    
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Split Summary                                               ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Total records processed: {}", total_processed);
    println!("📊 Chunks created: {}", chunk_num);
    println!("✅ Split complete");
    
    Ok(())
}

/// Agrega newline final si falta (in-place modification)
/// Sigue convenciones POSIX y DynamoDB ImportTable requirements
pub fn add_trailing_newline(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 3 {
        eprintln!("❌ Usage: csv_tools add-trailing-newline <file.csv>");
        eprintln!("💡 Adds newline at end if missing (modifies file in-place)");
        std::process::exit(1);
    }

    let file_path = &args[2];
    
    println!("🔧 Checking trailing newline: {}", file_path);
    
    // Leer archivo completo
    let mut content = std::fs::read(file_path)?;
    
    if content.is_empty() {
        eprintln!("⚠️  File is empty, skipping");
        return Ok(());
    }
    
    // Verificar si termina en newline (0x0A)
    let last_byte = content[content.len() - 1];
    
    if last_byte == b'\n' {
        println!("✅ File already has trailing newline");
        return Ok(());
    }
    
    // Agregar newline
    content.push(b'\n');
    std::fs::write(file_path, &content)?;
    
    println!("✅ Trailing newline added");
    println!("   Old size: {} bytes", content.len() - 1);
    println!("   New size: {} bytes", content.len());
    
    Ok(())
}

/// Elimina líneas vacías del CSV (in-place modification)
/// Preserva solo header + datos válidos
/// Sigue convenciones SiisaRestApi: CSV Schema Compliance
pub fn remove_empty_lines(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 3 {
        eprintln!("❌ Usage: csv_tools remove_empty_lines <file.csv>");
        eprintln!("💡 Removes empty lines (modifies file in-place)");
        std::process::exit(1);
    }

    let file_path = &args[2];
    
    println!("🧹 Removing empty lines from: {}", file_path);
    
    // Leer archivo completo
    let content = fs::read_to_string(file_path)?;
    let lines: Vec<&str> = content.lines().collect();
    
    println!("   Original lines: {}", lines.len());
    
    // Filtrar líneas vacías o solo con comas/espacios
    let cleaned_lines: Vec<&str> = lines
        .into_iter()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && trimmed.trim_matches(',').len() > 0
        })
        .collect();
    
    let removed_count = content.lines().count() - cleaned_lines.len();
    
    println!("   Cleaned lines: {}", cleaned_lines.len());
    println!("   Removed: {} empty line(s)", removed_count);
    
    if removed_count == 0 {
        println!("✅ No empty lines found");
        return Ok(());
    }
    
    // Reconstruir CSV con newline final
    let mut cleaned_content = cleaned_lines.join("\n");
    cleaned_content.push('\n'); // ✅ Agregar newline POSIX-compliant
    
    // ✅ SOLUCIÓN 1: Calcular tamaño ANTES de mover el ownership
    let new_size = cleaned_content.len();  // Capturar valor necesario
    fs::write(file_path, cleaned_content)?;  // Mover ownership
    println!("   New size: {} bytes", new_size);  // Usar valor capturado

    // ✅ SOLUCIÓN 2 (alternativa): Pasar referencia en lugar de ownership
    // fs::write(file_path, &cleaned_content)?;  // Pasa &String en lugar de String
    // println!("   New size: {} bytes", cleaned_content.len());  // Aún disponible
    
    println!("✅ Empty lines removed successfully");
    println!("   New size: {} bytes", new_size);
    
    Ok(())
}

/// Sanitiza CSV completo para DynamoDB ImportTable
/// Elimina BOM + líneas vacías + agrega newline final
pub fn sanitize_csv_complete(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        eprintln!("❌ Usage: csv_tools sanitize_csv <input.csv> <output.csv>");
        eprintln!("💡 Full sanitization: BOM + empty lines + trailing newline");
        std::process::exit(1);
    }

    let input_file = &args[2];
    let output_file = &args[3];
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  CSV Complete Sanitization for DynamoDB ImportTable         ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("📦 Input: {}", input_file);
    println!("📝 Output: {}", output_file);
    println!();
    
    // Leer archivo como bytes (para detectar BOM)
    let mut bytes = fs::read(input_file)?;
    let original_size = bytes.len();
    
    // 1. Eliminar BOM si existe
    let mut bom_removed = false;
    if bytes.len() >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF {
        println!("🔧 Removing UTF-8 BOM...");
        bytes = bytes[3..].to_vec();
        bom_removed = true;
    }
    
    // 2. Convertir a string y eliminar líneas vacías
    let content = String::from_utf8(bytes)?;
    let lines: Vec<&str> = content.lines().collect();
    
    println!("📋 Line analysis:");
    println!("   Total lines: {}", lines.len());
    
    let cleaned_lines: Vec<&str> = lines
        .into_iter()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && trimmed.trim_matches(',').len() > 0
        })
        .collect();
    
    let empty_lines_removed = content.lines().count() - cleaned_lines.len();
    
    if empty_lines_removed > 0 {
        println!("🧹 Removed {} empty line(s)", empty_lines_removed);
    }
    
    // 3. Reconstruir CSV con newline final
    let mut final_content = cleaned_lines.join("\n");
    final_content.push('\n'); // ✅ POSIX-compliant trailing newline
    
    // ✅ SOLUCIÓN 1: Calcular tamaño ANTES de mover el ownership
    let new_size = final_content.len();
    let final_line_count = cleaned_lines.len();
    let size_diff = new_size as i64 - original_size as i64;
    
    // 4. Escribir archivo sanitizado (mueve ownership de final_content)
    fs::write(output_file, final_content.as_bytes())?;
    
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Sanitization Summary                                        ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    if bom_removed {
        println!("✅ BOM removed (saved 3 bytes)");
    } else {
        println!("✅ No BOM detected");
    }
    
    if empty_lines_removed > 0 {
        println!("✅ {} empty line(s) removed", empty_lines_removed);
    } else {
        println!("✅ No empty lines detected");
    }
    
    println!("✅ Trailing newline added");
    println!();
    println!("📊 Size change: {} → {} bytes ({:+} bytes)", 
             original_size, new_size, size_diff);
    println!("📋 Final structure: {} lines (header + {} data rows)", 
             final_line_count, final_line_count - 1);
    println!();
    println!("📝 Files:");
    println!("   Input (original): {}", input_file);
    println!("   Output (sanitized): {}", output_file);
    println!();
    println!("🎯 Sanitized CSV is ready for DynamoDB ImportTable");
    
    Ok(())
}

/// Elimina registros desde una fila específica hasta el final del archivo
/// Mantiene el header y solo preserva las filas antes de la fila especificada
/// Sigue convenciones SiisaRestApi: CsvHelper-based parsing + structured error reporting
pub fn delete_from_row(input_file: &str, output_file: &str, from_row: usize) -> Result<(), Box<dyn Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Delete Rows from Specific Line to End                      ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📄 Input CSV: {}", input_file);
    println!("📝 Output CSV: {}", output_file);
    println!("✂️  Delete from row: {} (to end of file)", from_row);
    println!("📋 Note: Row 1 = header, Row 2 = first data row");
    println!();

    // Validar que from_row sea válido
    if from_row <= 1 {
        eprintln!("❌ Error: Row number must be >= 2 (row 1 is header)");
        std::process::exit(1);
    }

    let mut rdr = ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(input_file)?;

    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Necessary)
        .from_path(output_file)?;

    let headers = rdr.headers()?.clone();
    wtr.write_record(&headers)?;

    let mut current_row = 2usize; // La primera fila de datos es la fila 2
    let mut total_processed = 0usize;
    let mut rows_kept = 0usize;
    let mut rows_deleted = 0usize;

    println!("🔍 Processing records...");
    println!();

    for result in rdr.records() {
        total_processed += 1;
        
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("⚠️  Warning: Skipping malformed record at line {}: {}", current_row, e);
                current_row += 1;
                continue;
            }
        };

        if current_row < from_row {
            // Mantener este registro (está antes de la fila de corte)
            wtr.write_record(&record)?;
            rows_kept += 1;
        } else {
            // Eliminar este registro (está en o después de la fila de corte)
            rows_deleted += 1;
        }

        if total_processed % 10_000 == 0 {
            print!("\r📊 Processed: {} | Kept: {} | Deleted: {}", 
                total_processed, rows_kept, rows_deleted);
            std::io::stdout().flush().ok();
        }

        current_row += 1;
    }

    wtr.flush()?;

    println!("\r📊 Processed: {} | Kept: {} | Deleted: {}", 
        total_processed, rows_kept, rows_deleted);
    println!();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Delete Operation Summary                                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Input CSV:");
    println!("   Total data rows processed: {}", total_processed);
    println!("   Cut-off point: Row {} (inclusive)", from_row);
    println!();
    println!("📊 Output CSV:");
    println!("   Rows preserved: {} ✅", rows_kept);
    println!("   Rows deleted: {} ❌", rows_deleted);
    println!("   Header preserved: ✅");
    
    if rows_deleted > 0 {
        println!("   Deletion rate: {:.2}%", 
            (rows_deleted as f64 / total_processed as f64) * 100.0);
    }
    
    println!();
    println!("📝 Files:");
    println!("   Original CSV: {}", input_file);
    println!("   Truncated CSV: {}", output_file);
    
    println!();
    if rows_deleted > 0 {
        println!("🎯 Operation completed successfully:");
        println!("   {} records removed from row {} onwards", rows_deleted, from_row);
        println!("   Output contains header + {} data rows", rows_kept);
    } else {
        println!("📋 No records were deleted:");
        println!("   Cut-off row {} is beyond the end of the file", from_row);
        println!("   Output is identical to input");
    }

    Ok(())
}
