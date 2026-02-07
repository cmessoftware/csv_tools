use std::error::Error;
use std::fs::File;
use csv::ReaderBuilder;
use crate::models::DynamoDbModel;

pub fn validate_schema(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 4 {
        eprintln!("Usage: csv_tools validate_schema <input.csv> <model_type>");
        eprintln!("Model types: siisa_morosos, personas_telefonos");
        std::process::exit(1);
    }

    let input_path = &args[2];
    let model_type = &args[3];

    let model = DynamoDbModel::from_model_type(model_type)
        .ok_or_else(|| format!("Unknown model type: {}", model_type))?;

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  DynamoDB Schema Validation (Complete)                      ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📋 Model: {}", model.table_name);
    println!("📄 Input: {}", input_path);
    println!();

    // ✅ NUEVO: Mostrar todos los campos numéricos a validar
    println!("🔢 DynamoDB Type N Fields (all will be validated):");
    println!("   Partition Key: {} (Type N)", model.partition_key);
    println!("   Sort Key: {} (Type N)", model.sort_key);
    for field in &model.numeric_fields {
        if *field != model.partition_key && *field != model.sort_key {
            println!("   Additional: {} (Type N)", field);
        }
    }
    println!();

    // Abrir CSV
    let file = File::open(input_path)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let headers = rdr.headers()?.clone();
    
    // Verificar que el CSV tiene las columnas esperadas
    if headers.len() != model.expected_columns {
        eprintln!("❌ Column count mismatch!");
        eprintln!("   Expected: {} columns", model.expected_columns);
        eprintln!("   Found: {} columns", headers.len());
        eprintln!();
        eprintln!("📋 CSV headers:");
        for (i, h) in headers.iter().enumerate() {
            eprintln!("   [{}] {}", i, h);
        }
        std::process::exit(1);
    }

    println!("🔍 Validating records...");
    println!();

    let mut total_records = 0;
    let mut invalid_records = 0;
    let mut field_errors: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    // ✅ NUEVO: Validar TODOS los campos numéricos (no solo PK/SK)
    for (line_idx, result) in rdr.records().enumerate() {
        let record = result?;
        total_records += 1;

        let mut record_has_errors = false;

        // Validar cada campo numérico según el modelo
        for field_name in &model.numeric_fields {
            if let Some(&col_idx) = model.column_mapping.get(field_name) {
                if col_idx < record.len() {
                    let value = record[col_idx].trim();

                    // Validación estricta de campos numéricos
                    if !is_valid_dynamodb_number(value) {
                        record_has_errors = true;
                        
                        *field_errors.entry(field_name.to_string()).or_insert(0) += 1;

                        // Reportar error detallado (máximo 20 errores por campo)
                        if field_errors[*field_name] <= 20 {
                            eprintln!(
                                "   ❌ Line {}, Field [{}] {}: INVALID '{}' (expected numeric)",
                                line_idx + 2, // +2 porque línea 1 es header
                                col_idx,
                                field_name,
                                value
                            );
                        }
                    }
                }
            }
        }

        if record_has_errors {
            invalid_records += 1;
        }

        // Progreso cada 1000 registros
        if total_records % 1000 == 0 {
            print!("\r📊 Processed: {} | Invalid: {}", total_records, invalid_records);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }

    println!();
    println!("\r📊 Processed: {} | Invalid: {}", total_records, invalid_records);
    println!();

    // ✅ NUEVO: Resumen detallado por campo
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Validation Summary                                          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("📊 Total records validated: {}", total_records);
    println!("✅ Valid records: {}", total_records - invalid_records);
    println!("❌ Invalid records: {}", invalid_records);
    println!();

    if !field_errors.is_empty() {
        println!("📋 Errors by field (Type N validation failed):");
        let mut sorted_errors: Vec<_> = field_errors.iter().collect();
        sorted_errors.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
        
        for (field_name, count) in sorted_errors {
            println!("   ❌ {}: {} invalid value(s)", field_name, count);
        }
        println!();

        println!("💡 To fix these issues:");
        println!("   csv_tools sanitize_dynamodb \"{}\" \"output.csv\" {}", input_path, model_type);
        println!();
        
        std::process::exit(1);
    } else {
        println!("✅ All records valid for DynamoDB import");
        println!("   All Type N fields contain valid numeric values");
    }

    Ok(())
}

/// ✅ NUEVO: Validación estricta de números para DynamoDB Type N
/// Mismas reglas que DynamoDB ImportTable
fn is_valid_dynamodb_number(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }

    // Debe parsear como número decimal válido
    if value.parse::<f64>().is_err() {
        return false;
    }

    // No puede tener espacios en blanco antes/después
    if value != value.trim() {
        return false;
    }

    // Validación carácter por carácter (más estricta)
    let mut has_decimal_point = false;
    let mut has_e = false;

    for (i, c) in value.chars().enumerate() {
        match c {
            '0'..='9' => continue,
            '-' | '+' if i == 0 => continue, // Signo solo al inicio
            '.' if !has_decimal_point => {
                has_decimal_point = true;
                continue;
            }
            'e' | 'E' if !has_e => {
                has_e = true;
                continue;
            }
            '-' | '+' if has_e && value.chars().nth(i - 1) == Some('e') || value.chars().nth(i - 1) == Some('E') => {
                continue; // Signo después de 'e' en notación científica
            }
            _ => return false, // Cualquier otro carácter es inválido
        }
    }

    // No puede ser solo '-', '+', '.' o 'e'
    if value == "-" || value == "+" || value == "." || value == "e" || value == "E" {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_dynamodb_number() {
        // Casos válidos
        assert!(is_valid_dynamodb_number("123"));
        assert!(is_valid_dynamodb_number("-456"));
        assert!(is_valid_dynamodb_number("123.456"));
        assert!(is_valid_dynamodb_number("-789.012"));
        assert!(is_valid_dynamodb_number("1.23e10"));
        assert!(is_valid_dynamodb_number("1.23E-5"));

        // Casos inválidos
        assert!(!is_valid_dynamodb_number(""));
        assert!(!is_valid_dynamodb_number(" 123"));      // Espacio antes
        assert!(!is_valid_dynamodb_number("123 "));      // Espacio después
        assert!(!is_valid_dynamodb_number("1,234"));     // Separador de miles
        assert!(!is_valid_dynamodb_number("+123"));      // Signo positivo (DynamoDB rechaza)
        assert!(!is_valid_dynamodb_number("abc"));       // No numérico
        assert!(!is_valid_dynamodb_number("12.34.56")); // Múltiples puntos
        assert!(!is_valid_dynamodb_number("--123"));     // Doble signo
        assert!(!is_valid_dynamodb_number("."));         // Solo punto
        assert!(!is_valid_dynamodb_number("-"));         // Solo signo
    }
}