use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use csv::{Reader, Writer};
use crate::models::{
    get_expected_headers, 
    validate_headers, 
    parse_dynamodb_key,           // ✅ Corrected: was parse_composite_key
    parse_sql_composite_key,      // ✅ Added: for resume functionality
    format_sql_composite_key,     // ✅ Corrected: was format_composite_key
    validate_field_type
};

/// Enhanced CSV header validation (compatible con chunk-export-v2)
pub fn enhanced_check(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let model_type = args.get(3).map(|s| s.as_str());
    
    println!("🔍 Checking CSV file: {}", input_file);
    
    let mut reader = Reader::from_path(input_file)?;
    let headers = reader.headers()?;
    
    // Check for duplicate headers
    let mut header_vec: Vec<&str> = headers.iter().collect();
    let original_len = header_vec.len();
    header_vec.sort();
    header_vec.dedup();
    let has_duplicates = header_vec.len() != original_len;
    
    if has_duplicates {
        println!("❌ Duplicate headers detected!");
        println!("💡 Use 'clean' command to remove duplicates");
        return Ok(());
    }
    
    // DynamoDB model-specific validation
    if let Some(model) = model_type {
        let expected_headers = get_expected_headers(model)?;
        let actual_headers: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
        
        println!("\n📋 DynamoDB Schema Validation");
        println!("   Model: {}", model);
        println!("   Expected columns: {}", expected_headers.len());
        println!("   Found columns: {}", actual_headers.len());
        
        // Validación estricta del modelo DynamoDB
        match validate_headers(&actual_headers, model) {
            Ok(_) => {
                println!("\n✅ Headers match DynamoDB model schema perfectly");
                println!("\n📊 Schema details (EfficientDynamoDb attributes):");
                for (i, header) in expected_headers.iter().enumerate() {
                    let key_type = match (i, model) {
                        (0, "siisa_morosos") => " [PartitionKey: Cuil]",
                        (1, "siisa_morosos") => " [SortKey: IdTransmit]",
                        _ => ""
                    };
                    println!("   [{:2}] {}{}", i + 1, header, key_type);
                }
                
                println!("\n🔗 C# Model: SiisaRestApi.Common/Models/DynamoModels/MorososTransmitDynamoDbModel.cs");
            }
            Err(e) => {
                println!("\n❌ Schema validation failed:");
                println!("{}", e);
                return Ok(());
            }
        }
    } else {
        println!("✅ No duplicate headers found");
        println!("   Columns: {}", headers.len());
        println!("\n📋 Headers found:");
        for (i, header) in headers.iter().enumerate() {
            println!("   [{:2}] {}", i + 1, header);
        }
    }
    
    // Count records (compatible con ChunkSize config)
    let record_count = reader.records().count();
    println!("\n📊 Total data records: {}", record_count);
    
    Ok(())
}

/// Validate CSV against DynamoDB model schema
/// ⚠️ Uses DynamoDB PartitionKey+SortKey, not SQL CompositePrimaryKey
pub fn validate_csv_schema(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let error_file = &args[3];
    let table_name = &args[4];
    let max_show: usize = args[5].parse().unwrap_or(10);
    let cancel_on_max: bool = args[6].parse().unwrap_or(false);
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  DynamoDB Schema Validation - SiisaRestApi Compatible        ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📋 DynamoDB Table: {}", table_name);
    println!("📄 Input CSV: {}", input_file);
    println!("📝 Error Log: {}", error_file);
    println!("⚠️  Max errors to display: {}", max_show);
    println!("🛑 Cancel on max errors: {}\n", cancel_on_max);
    
    let mut reader = Reader::from_path(input_file)?;
    let headers = reader.headers()?.clone();
    let actual_headers: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
    
    // Validate header structure against DynamoDB model (MorososTransmitDynamoDbModel)
    let expected_headers = get_expected_headers(table_name)?;
    
    println!("🔍 Validating header schema...");
    match validate_headers(&actual_headers, table_name) {
        Ok(_) => {
            println!("✅ Header schema matches DynamoDB model ({} columns)", headers.len());
            println!("   PartitionKey: {} (Cuil)", expected_headers[0]);
            println!("   SortKey: {} (IdTransmit)\n", expected_headers[1]);
        }
        Err(e) => {
            println!("❌ Header validation failed:");
            println!("{}\n", e);
            return Err("Schema mismatch - cannot proceed with DynamoDB validation".into());
        }
    }
    
    let mut error_writer = BufWriter::new(File::create(error_file)?);
    writeln!(error_writer, "Line,ErrorType,Details,DynamoDbKey,SqlCompositeKey")?;
    
    let mut error_count = 0;
    let mut processed = 0;
    
    println!("🔍 Validating data records for DynamoDB import...\n");
    
    for (idx, result) in reader.records().enumerate() {
        let line_num = idx + 2; // +1 for 0-index, +1 for header
        
        match result {
            Ok(record) => {
                // Validate record length
                if record.len() != expected_headers.len() {
                    error_count += 1;
                    
                    // Extract both keys for comprehensive error reporting
                    let dynamo_key = parse_dynamodb_key(&record, table_name)
                        .unwrap_or_else(|_| "INVALID_DYNAMO_KEY".to_string());
                    
                    let sql_key = parse_sql_composite_key(&record)
                        .map(|(c, t, n)| format_sql_composite_key(c, t, &n))
                        .unwrap_or_else(|_| "INVALID_SQL_KEY".to_string());
                    
                    let error_msg = format!(
                        "Column count mismatch: expected {} but found {}",
                        expected_headers.len(), record.len()
                    );
                    
                    writeln!(error_writer, "{},ColumnCount,{},{},{}",
                             line_num, error_msg, dynamo_key, sql_key)?;
                    
                    if error_count <= max_show {
                        eprintln!("❌ Line {}: {}", line_num, error_msg);
                        eprintln!("   DynamoDB Key: {}", dynamo_key);
                        eprintln!("   SQL Key (resume): {}", sql_key);
                    }
                    
                    if cancel_on_max && error_count >= max_show {
                        println!("\n⚠️  Max errors ({}) reached. Stopping validation.", max_show);
                        break;
                    }
                }
                
                // Validate field types (DynamoDB attribute types)
                for (i, value) in record.iter().enumerate() {
                    if let Some(field_name) = expected_headers.get(i) {
                        if let Err(e) = validate_field_type(value, field_name, table_name) {
                            error_count += 1;
                            
                            let dynamo_key = parse_dynamodb_key(&record, table_name)
                                .unwrap_or_else(|_| "INVALID_DYNAMO_KEY".to_string());
                            
                            let sql_key = parse_sql_composite_key(&record)
                                .map(|(c, t, n)| format_sql_composite_key(c, t, &n))
                                .unwrap_or_else(|_| "INVALID_SQL_KEY".to_string());
                            
                            writeln!(error_writer, "{},TypeError,{},{},{}",
                                     line_num, e, dynamo_key, sql_key)?;
                            
                            if error_count <= max_show {
                                eprintln!("❌ Line {}: {}", line_num, e);
                                eprintln!("   DynamoDB Key: {}", dynamo_key);
                            }
                        }
                    }
                }
                
                processed += 1;
                if processed % 10_000 == 0 {
                    print!("\r📊 Processed: {} | Errors: {}", processed, error_count);
                    std::io::stdout().flush()?;
                }
            }
            Err(e) => {
                error_count += 1;
                writeln!(error_writer, "{},ParseError,{},UNKNOWN_DYNAMO_KEY,UNKNOWN_SQL_KEY", 
                         line_num, e)?;
                
                if error_count <= max_show {
                    eprintln!("❌ Line {}: Parse error - {}", line_num, e);
                }
            }
        }
    }
    
    error_writer.flush()?;
    
    let error_rate = if processed > 0 {
        (error_count as f64 / processed as f64) * 100.0
    } else {
        0.0
    };
    
    println!("\n\n╔══════════════════════════════════════════════════════════════╗");
    println!("║  DynamoDB Validation Summary                                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Processed: {} records", processed);
    println!("❌ Errors: {} ({:.2}%)", error_count, error_rate);
    println!("📝 Error log: {}", error_file);
    
    if error_count == 0 {
        println!("\n🎉 All records valid for DynamoDB import!");
        println!("✅ Ready for EfficientDynamoDb batch write operation");
        println!("🔗 See: SiisaRestApi.Process chunk-export-v2 → DynamoDB import");
    } else {
        println!("\n⚠️  Review error file before DynamoDB import");
        println!("💡 Use 'clean_invalid_lines' command to filter invalid records");
    }
    
    Ok(())
}

/// Remove invalid lines from CSV (DynamoDB-ready cleaning)
pub fn clean_invalid_lines(args: &[String]) -> Result<(), Box<dyn Error>> {
    let input_file = &args[2];
    let output_file = &args[3];
    let error_file = &args[4];
    
    println!("🧹 Cleaning invalid lines for DynamoDB import: {}", input_file);
    
    let mut reader = Reader::from_path(input_file)?;
    let headers = reader.headers()?.clone();
    let expected_cols = headers.len();
    
    let mut writer = Writer::from_path(output_file)?;
    writer.write_record(&headers)?;
    
    let mut error_writer = BufWriter::new(File::create(error_file)?);
    writeln!(error_writer, "Line,Issue,Details,DynamoDbKey,SqlCompositeKey")?;
    
    let mut valid_count = 0u64;
    let mut invalid_count = 0u64;
    
    // Determine model type from headers for key extraction
    let model_type = if headers.len() == 14 && headers.get(0) == Some("Cuil") {
        "siisa_morosos"
    } else {
        "unknown"
    };
    
    for (idx, result) in reader.records().enumerate() {
        let line_num = idx + 2;
        
        match result {
            Ok(record) => {
                if record.len() == expected_cols {
                    writer.write_record(&record)?;
                    valid_count += 1;
                } else {
                    invalid_count += 1;
                    
                    let dynamo_key = parse_dynamodb_key(&record, model_type)
                        .unwrap_or_else(|_| "INVALID_DYNAMO_KEY".to_string());
                    
                    let sql_key = parse_sql_composite_key(&record)
                        .map(|(c, t, n)| format_sql_composite_key(c, t, &n))
                        .unwrap_or_else(|_| "INVALID_SQL_KEY".to_string());
                    
                    writeln!(
                        error_writer,
                        "{},ColumnMismatch,Expected {} but found {},{},{}",
                        line_num, expected_cols, record.len(), dynamo_key, sql_key
                    )?;
                }
            }
            Err(e) => {
                invalid_count += 1;
                writeln!(error_writer, "{},ParseError,{},UNKNOWN_DYNAMO_KEY,UNKNOWN_SQL_KEY", 
                         line_num, e)?;
            }
        }
        
        if (valid_count + invalid_count) % 10_000 == 0 {
            print!("\r📊 Valid: {} | Invalid: {}", valid_count, invalid_count);
            std::io::stdout().flush()?;
        }
    }
    
    writer.flush()?;
    error_writer.flush()?;
    
    let total = valid_count + invalid_count;
    let invalid_rate = (invalid_count as f64 / total as f64) * 100.0;
    
    println!("\n\n✅ Cleaning complete (DynamoDB-ready):");
    println!("   Valid records: {} ({:.2}%)", valid_count, 100.0 - invalid_rate);
    println!("   Invalid records removed: {} ({:.2}%)", invalid_count, invalid_rate);
    println!("📝 Clean output: {}", output_file);
    println!("📝 Error log: {}", error_file);
    println!("\n💡 Clean CSV is ready for DynamoDB batch write via EfficientDynamoDb");
    
    Ok(())
}