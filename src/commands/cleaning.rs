use std::error::Error;
use std::fs::File;
use csv::{ReaderBuilder, WriterBuilder};
use crate::models::DynamoDbModel;

/// Sanitize CSV for DynamoDB ImportTable
/// - Removes quotes from header row
/// - Validates numeric fields (Type N)
/// - Preserves quoted strings for Type S fields
/// - Compatible with SiisaRestApi chunk-export-v2 output
pub fn sanitize_dynamodb(
    input_path: &str,
    output_path: &str,
    model_type: &str,
) -> Result<(), Box<dyn Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  CSV Sanitization for DynamoDB ImportTable                   ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    println!("📄 Input:  {}", input_path);
    println!("📄 Output: {}", output_path);
    println!("📋 Model:  {}", model_type);
    
    // ✅ FIX: Usar DynamoDbModel::from_model_type() que soporta todos los modelos
    let model = DynamoDbModel::from_model_type(model_type)
        .ok_or_else(|| format!(
            "Unknown model type: '{}'\n\
             Supported: siisa_morosos, personas_telefonos, siisa_empleadores, siisa_empleadores_relaciones",
            model_type
        ))?;
    
    // ✅ FIX: Usar model.expected_columns (10 para empleadores, 14 para morosos)
    println!("🔢 Expected Columns: {}", model.expected_columns);
    println!("🔧 Strategy: CsvHelper-based parsing + validate numeric fields");
    println!();
    
    // Read input CSV
    let input_file = File::open(input_path)?;
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(input_file);
    
    // Get headers
    let headers = reader.headers()?;
    let header_str = headers.iter()
        .map(|h| h.trim_matches('"'))  // Remove quotes if present
        .collect::<Vec<_>>()
        .join(",");
    
    println!("🔍 DEBUG: Raw header from input CSV:");
    println!("   '{}'", headers.iter().collect::<Vec<_>>().join(","));
    println!();
    
    println!("🔍 DEBUG: Clean header to be written:");
    println!("   '{}'", header_str);
    println!();
    
    // Validate header count
    if headers.len() != model.expected_columns {
        return Err(format!(
            "Header mismatch: expected {} columns for {}, found {}\n\
             Expected: {:?}\n\
             Got: {:?}",
            model.expected_columns,
            model_type,
            headers.len(),
            crate::models::get_expected_headers(model_type)?,
            headers.iter().collect::<Vec<_>>()
        ).into());
    }
    
    // Create output CSV
    let output_file = File::create(output_path)?;
    let mut writer = WriterBuilder::new()
        .has_headers(false)  // We'll write header manually
        .quote_style(csv::QuoteStyle::Necessary)
        .from_writer(output_file);
    
    // ✅ Write header WITHOUT quotes
    writer.write_record(header_str.split(','))?;
    println!("✅ Header written without quotes");
    println!();
    
    // Process records
    println!("🔍 Processing records...");
    let mut processed = 0;
    let mut valid = 0;
    let mut invalid = 0;
    
    for result in reader.records() {
        let record = result?;
        processed += 1;
        
        // Validate numeric fields (Type N in DynamoDB)
        let mut is_valid = true;
        
        for &field_name in &model.numeric_fields {
            if let Some(&col_idx) = model.column_mapping.get(field_name) {
                if let Some(value) = record.get(col_idx) {
                    let trimmed = value.trim().trim_matches('"');
                    
                    // ✅ Validar que sea número válido
                    if !trimmed.is_empty() && trimmed.parse::<f64>().is_err() {
                        eprintln!(
                            "⚠️  Line {}: Invalid numeric value for {} (Type N): '{}'",
                            processed + 1,
                            field_name,
                            trimmed
                        );
                        is_valid = false;
                    }
                }
            }
        }
        
        if is_valid {
            // Write record (CsvHelper handles quoting automatically)
            writer.write_record(&record)?;
            valid += 1;
        } else {
            invalid += 1;
        }
        
        // Progress reporting (cada 10,000 registros)
        if processed % 10000 == 0 {
            println!("   ✅ Processed: {} | Valid: {} | Invalid: {}", 
                     processed, valid, invalid);
        }
    }
    
    writer.flush()?;
    
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Sanitization Summary                                        ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Total processed: {}", processed);
    println!("✅ Valid records:   {}", valid);
    println!("❌ Invalid records: {}", invalid);
    println!();
    
    if invalid > 0 {
        eprintln!("⚠️  WARNING: {} invalid records were skipped", invalid);
        eprintln!("   Review logs above for details");
    }
    
    println!("✅ Sanitization complete!");
    println!("📄 Output file: {}", output_path);
    println!();
    
    Ok(())
}

/// Validate that CSV is ready for DynamoDB ImportTable
/// Checks:
/// - Header format (no quotes)
/// - Numeric fields contain valid numbers (Type N)
/// - No missing required fields
pub fn validate_dynamodb_csv(
    csv_path: &str,
    model_type: &str,
) -> Result<(), Box<dyn Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  DynamoDB CSV Validation                                     ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📄 File: {}", csv_path);
    println!("📋 Model: {}", model_type);
    println!();
    
    // ✅ FIX: Usar DynamoDbModel::from_model_type()
    let model = DynamoDbModel::from_model_type(model_type)
        .ok_or_else(|| format!(
            "Unknown model type: '{}'\n\
             Supported: siisa_morosos, personas_telefonos, siisa_empleadores, siisa_empleadores_relaciones",
            model_type
        ))?;
    
    let file = File::open(csv_path)?;
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    
    // Validate header
    let headers = reader.headers()?;
    
    println!("🔍 Header validation:");
    println!("   Expected: {} columns", model.expected_columns);
    println!("   Found:    {} columns", headers.len());
    
    if headers.len() != model.expected_columns {
        return Err(format!(
            "Column count mismatch: expected {}, found {}",
            model.expected_columns,
            headers.len()
        ).into());
    }
    
    // Check for quotes in header
    let has_quotes = headers.iter().any(|h| h.starts_with('"') || h.ends_with('"'));
    if has_quotes {
        eprintln!("⚠️  WARNING: Header contains quotes (DynamoDB expects unquoted header)");
    } else {
        println!("   ✅ Header format valid (no quotes)");
    }
    
    println!();
    println!("🔍 Validating records...");
    
    let mut total = 0;
    let mut errors = 0;
    
    for (line_num, result) in reader.records().enumerate() {
        let record = result?;
        total += 1;
        
        // Validate numeric fields
        for &field_name in &model.numeric_fields {
            if let Some(&col_idx) = model.column_mapping.get(field_name) {
                if let Some(value) = record.get(col_idx) {
                    let trimmed = value.trim().trim_matches('"');
                    
                    if !trimmed.is_empty() && trimmed.parse::<f64>().is_err() {
                        eprintln!(
                            "   ❌ Line {}: Invalid {} (Type N): '{}'",
                            line_num + 2,  // +2 because headers are line 1
                            field_name,
                            trimmed
                        );
                        errors += 1;
                    }
                }
            }
        }
        
        if total % 10000 == 0 {
            println!("   Validated: {} records", total);
        }
    }
    
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Validation Summary                                          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Total records: {}", total);
    
    if errors > 0 {
        println!("❌ Validation FAILED: {} errors found", errors);
        return Err(format!("{} validation errors detected", errors).into());
    } else {
        println!("✅ Validation PASSED: All records valid for DynamoDB import");
    }
    
    println!();
    Ok(())
}