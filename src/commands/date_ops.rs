use std::error::Error;
use std::io::Write;
use chrono::{NaiveDateTime, Datelike, NaiveDate};
use csv::{Reader, WriterBuilder};

/// Conversión de fechas DD/MM/YYYY a YYYY-MM-DD
/// Sigue patrón SiisaRestApi: stream-based processing + progress tracking
pub fn convert_dates(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        eprintln!("Usage: csv_tools convert_dates <input.csv> <output.csv>");
        std::process::exit(1);
    }
    
    let input_file = &args[2];
    let output_file = &args[3];
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Date Format Converter (DD/MM/YYYY → YYYY-MM-DD)           ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📄 Input: {}", input_file);
    println!("📝 Output: {}", output_file);
    println!();
    
    let mut rdr = Reader::from_path(input_file)?;
    let headers = rdr.headers()?.clone();
    
    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Necessary)
        .from_path(output_file)?;
    
    wtr.write_record(&headers)?;
    
    let mut total = 0usize;
    let mut converted = 0usize;
    
    for result in rdr.records() {
        total += 1;
        let record = result?;
        
        // ✅ RECONSTRUIR REGISTRO con campos convertidos (StringRecord es inmutable)
        let mut new_record = csv::StringRecord::new();
        
        for field in record.iter() {
            if let Some(new_date) = try_convert_date(field) {
                new_record.push_field(&new_date);
                converted += 1;
            } else {
                new_record.push_field(field);
            }
        }
        
        wtr.write_record(&new_record)?;
        
        if total % 10_000 == 0 {
            print!("\r📊 Processed: {} | Converted: {}", total, converted);
            std::io::stdout().flush()?;
        }
    }
    
    wtr.flush()?;
    
    println!("\r📊 Processed: {} | Converted: {}", total, converted);
    println!("✅ Date conversion complete");
    
    Ok(())
}

/// Intenta convertir una fecha de DD/MM/YYYY a YYYY-MM-DD
fn try_convert_date(value: &str) -> Option<String> {
    // Patrón DD/MM/YYYY
    if value.len() == 10 && value.chars().nth(2)? == '/' && value.chars().nth(5)? == '/' {
        let parts: Vec<&str> = value.split('/').collect();
        if parts.len() == 3 {
            if let (Ok(day), Ok(month), Ok(year)) = (
                parts[0].parse::<u32>(),
                parts[1].parse::<u32>(),
                parts[2].parse::<i32>()
            ) {
                if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
                    return Some(date.format("%Y-%m-%d").to_string());
                }
            }
        }
    }
    None
}

/// Parse US datetime format: MM/dd/yyyy hh:mm:ss AM/PM
pub fn parse_us_datetime(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%m/%d/%Y %I:%M:%S %p").ok()
}

pub fn find_oldest_date(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        eprintln!("Usage: csv_tools find_oldest_date <input_file> <date_column>");
        return Ok(());
    }
    find_extreme_date(&args[2], &args[3], true)
}

pub fn find_newest_date(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        eprintln!("Usage: csv_tools find_newest_date <input_file> <date_column>");
        return Ok(());
    }
    find_extreme_date(&args[2], &args[3], false)
}

fn find_extreme_date(
    input_file: &str,
    date_column: &str,
    find_oldest: bool,
) -> Result<(), Box<dyn Error>> {
    println!("🔍 Buscando fecha {} en columna '{}'", 
             if find_oldest { "más antigua" } else { "más reciente" }, 
             date_column);
    
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(input_file)?;

    let headers = reader.headers()?.clone();
    let date_col_idx = headers.iter()
        .position(|h| h == date_column)
        .ok_or(format!("Columna '{}' no encontrada", date_column))?;

    let mut extreme_date: Option<NaiveDateTime> = None;
    let mut extreme_record: Option<String> = None;
    let mut processed = 0u64;
    let mut valid_dates = 0u64;
    let mut invalid_dates = 0u64;
    let mut format_errors = 0u64;
    let start = std::time::Instant::now();

    for (_line_num, result) in reader.records().enumerate() {  // ← Prefijo con _
        match result {
            Ok(record) => {
                if record.len() < date_col_idx + 1 {
                    format_errors += 1;
                    continue;
                }
                
                if let Some(date_str) = record.get(date_col_idx) {
                    if let Some(date) = parse_us_datetime(date_str) {
                        valid_dates += 1;
                        
                        if extreme_date.is_none() || 
                           (find_oldest && date < extreme_date.unwrap()) ||
                           (!find_oldest && date > extreme_date.unwrap()) {
                            extreme_date = Some(date);
                            extreme_record = Some(record.iter().take(5).collect::<Vec<_>>().join(" | "));
                        }
                    } else {
                        invalid_dates += 1;
                    }
                }
                processed += 1;
                
                if processed % 100_000 == 0 {
                    print!("\r📊 Procesados: {} | Válidos: {} | Errores: {} | Tiempo: {:.1}s", 
                           processed, valid_dates, invalid_dates + format_errors, start.elapsed().as_secs_f32());
                    std::io::stdout().flush()?;
                }
            }
            Err(_) => {
                format_errors += 1;
                continue;
            }
        }
    }

    println!("\n\n📊 RESUMEN:");
    println!("  Registros procesados: {}", processed);
    println!("  Fechas válidas: {}", valid_dates);
    println!("  Fechas inválidas: {}", invalid_dates);
    println!("  Errores de formato: {}", format_errors);
    
    if let (Some(date), Some(record)) = (extreme_date, extreme_record) {
        println!("\n✅ Fecha {} encontrada:", if find_oldest { "MÁS ANTIGUA" } else { "MÁS RECIENTE" });
        println!("   📅 {}", date.format("%m/%d/%Y %I:%M:%S %p"));
        println!("   📝 Registro: {}", record);
    } else {
        println!("❌ No se encontraron fechas válidas");
    }

    Ok(())
}

pub fn find_last_by_month(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 6 {
        eprintln!("Usage: csv_tools find_last_by_month <input_file> <date_column> <year> <month>");
        return Ok(());
    }
    
    let input_file = &args[2];
    let date_column = &args[3];
    let year: i32 = args[4].parse()
        .map_err(|_| "Invalid year format")?;
    let month: u32 = args[5].parse()
        .map_err(|_| "Invalid month format")?;
    
    if month < 1 || month > 12 {
        return Err("Month must be between 1 and 12".into());
    }
    
    find_last_record_by_month_impl(input_file, date_column, year, month)
}

fn find_last_record_by_month_impl(
    input_file: &str,
    date_column: &str,
    target_year: i32,
    target_month: u32,
) -> Result<(), Box<dyn Error>> {
    println!("🔍 Buscando último registro de {}/{} en columna '{}'", target_month, target_year, date_column);
    
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(input_file)?;

    let headers = reader.headers()?.clone();
    let date_col_idx = headers.iter()
        .position(|h| h == date_column)
        .ok_or(format!("Columna '{}' no encontrada", date_column))?;

    // Crear fecha objetivo (último día del mes)
    let target_date = NaiveDate::from_ymd_opt(
        target_year,
        target_month,
        match target_month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => if target_year % 4 == 0 && (target_year % 100 != 0 || target_year % 400 == 0) { 29 } else { 28 },
            _ => 31
        }
    ).ok_or("Invalid target date")?
    .and_hms_opt(23, 59, 59)
    .ok_or("Invalid time")?;

    let mut last_date: Option<NaiveDateTime> = None;
    let mut last_record_line: Option<usize> = None;
    let mut last_record_data: Option<csv::StringRecord> = None;
    
    let mut closest_date: Option<NaiveDateTime> = None;
    let mut closest_record_line: Option<usize> = None;
    let mut closest_record_data: Option<csv::StringRecord> = None;
    let mut min_distance: Option<i64> = None;
    
    let mut processed = 0u64;
    let mut matched_records = 0u64;
    let mut valid_dates = 0u64;
    let start = std::time::Instant::now();

    for (line_num, result) in reader.records().enumerate() {
        if let Ok(record) = result {
            if record.len() > date_col_idx {
                if let Some(date_str) = record.get(date_col_idx) {
                    if let Some(date) = parse_us_datetime(date_str) {
                        valid_dates += 1;
                        
                        let distance = (date.and_utc().timestamp() - target_date.and_utc().timestamp()).abs();
                        
                        if date.year() == target_year && date.month() == target_month {
                            matched_records += 1;
                            if last_date.is_none() || date > last_date.unwrap() {
                                last_date = Some(date);
                                last_record_line = Some(line_num + 2);
                                last_record_data = Some(record.clone());
                            }
                        }
                        
                        if min_distance.is_none() || distance < min_distance.unwrap() {
                            min_distance = Some(distance);
                            closest_date = Some(date);
                            closest_record_line = Some(line_num + 2);
                            closest_record_data = Some(record.clone());
                        }
                    }
                }
            }
            processed += 1;
            
            if processed % 100_000 == 0 {
                print!("\r📊 Procesados: {} | Válidos: {} | Coincidencias: {} | Tiempo: {:.1}s", 
                       processed, valid_dates, matched_records, start.elapsed().as_secs_f32());
                std::io::stdout().flush()?;
            }
        }
    }

    println!("\n\n📊 RESUMEN:");
    println!("  Registros procesados: {}", processed);
    println!("  Fechas válidas: {}", valid_dates);
    println!("  Registros del mes {}/{}: {}", target_month, target_year, matched_records);
    
    if let (Some(date), Some(line), Some(record)) = (last_date, last_record_line, last_record_data) {
        println!("\n✅ ÚLTIMO REGISTRO DE {}/{}:", target_month, target_year);
        println!("   📅 Fecha: {}", date.format("%m/%d/%Y %I:%M:%S %p"));
        println!("   📍 Línea: {}", line);
        println!("   📝 Registro completo:");
        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                println!("      {}: {}", header, field);
            }
        }
    } else if let (Some(date), Some(line), Some(record)) = (closest_date, closest_record_line, closest_record_data) {
        println!("\n❌ No se encontraron registros EXACTOS para {}/{}", target_month, target_year);
        println!("\n🔍 REGISTRO MÁS CERCANO ENCONTRADO:");
        println!("   📅 Fecha: {}", date.format("%m/%d/%Y %I:%M:%S %p"));
        println!("   📍 Línea: {}", line);
        
        let diff_days = min_distance.unwrap_or(0) / (24 * 3600);
        if date < target_date {
            println!("   ⏱️  {} días ANTES del mes objetivo", diff_days);
        } else {
            println!("   ⏱️  {} días DESPUÉS del mes objetivo", diff_days);
        }
        
        println!("   📝 Registro completo:");
        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                println!("      {}: {}", header, field);
            }
        }
    } else {
        println!("❌ No se encontraron registros válidos en el archivo");
    }

    Ok(())
}

pub fn sort_csv_by_date(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 5 {
        eprintln!("Usage: csv_tools sort_by_date <input> <output> <date_column> [asc|desc]");
        return Ok(());
    }
    
    let _input_file = &args[2];   // ← Prefijo con _
    let _output_file = &args[3];  // ← Prefijo con _
    let date_column = &args[4];
    let order = args.get(5).map(|s| s.as_str()).unwrap_or("desc");
    
    println!("🔄 Sorting CSV by date column '{}' in {} order", date_column, order);
    println!("⚠️  This operation uses external sort for memory efficiency");
    println!("❌ sort_by_date not yet implemented in modular structure");
    
    Ok(())
}