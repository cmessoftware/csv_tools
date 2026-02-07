use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

/// DynamoDB model schemas compatible with SiisaRestApi.Common
/// Based on SiisaRestApi.Common/Models/DynamoModels/
/// 
/// ⚠️ CRITICAL: DynamoDB uses Cuil (PartitionKey) + IdTransmit (SortKey)
/// This differs from SQL CompositePrimaryKey used in chunk-export-v2

/// Modelo para siisa_morosos
/// Based on SiisaRestApi.Common/Models/DynamoModels/MorososTransmitDynamoDbModel.cs
/// 
/// ⚠️ CRITICAL: DynamoDB uses Cuil (PartitionKey) + IdTransmit (SortKey)
/// This differs from SQL CompositePrimaryKey used in chunk-export-v2
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MorososTransmitDynamoDbModel {
    // DynamoDB Primary Key (EfficientDynamoDb attributes)
    #[serde(rename = "Cuil")]
    pub cuil: f64,  // decimal in C# = f64 in Rust (DynamoDB Number type)
    
    #[serde(rename = "IdTransmit")]
    pub id_transmit: i32,  // SortKey
    
    // Business fields
    #[serde(rename = "NroDoc")]
    pub nro_doc: String,
    
    #[serde(rename = "ApellidoNombre")]
    pub apellido_nombre: String,
    
    #[serde(rename = "IdCliente")]
    pub id_cliente: i32,
    
    #[serde(rename = "IdRegion")]
    pub id_region: i32,
    
    #[serde(rename = "RazonSocial")]
    pub razon_social: String,
    
    #[serde(rename = "Telefono")]
    pub telefono: String,
    
    #[serde(rename = "NombreRegion")]
    pub nombre_region: String,
    
    #[serde(rename = "NombreCategoria")]
    pub nombre_categoria: String,
    
    #[serde(rename = "Periodo")]
    pub periodo: String,
    
    #[serde(rename = "IdEntidad")]
    pub id_entidad: i32,
    
    // Audit fields (SiisaRestApi standard)
    #[serde(rename = "CreateDate")]
    pub create_date: String,
    
    #[serde(rename = "CreateUser")]
    pub create_user: String,
}

/// Modelo para siisa_personas_telefonos (si existe en SiisaRestApi.Common)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonasTelefonosDynamoDbModel {
    #[serde(rename = "Cuil")]
    pub cuil: String,
    
    #[serde(rename = "IdTelefono")]
    pub id_telefono: i32,
    
    #[serde(rename = "Telefono")]
    pub telefono: String,
    
    #[serde(rename = "Prefijo")]
    pub prefijo: String,
    
    #[serde(rename = "CreateUser")]
    pub create_user: String,
    
    #[serde(rename = "CreateDate")]
    pub create_date: String,
}

/// Modelo para siisa_empleadores
/// Based on SiisaRestApi.Common/Models/DynamoModels/EmpleadorDynamosDbModel.cs
/// 
/// DynamoDB Schema:
/// - PartitionKey: Cuit (Type: N)
/// - No SortKey (simple primary key)
/// - Table: siisa_empleadores
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmpleadorDynamoDbModel {
    // DynamoDB Primary Key (EfficientDynamoDb attributes)
    #[serde(rename = "Cuit")]
    pub cuit: f64,  // PartitionKey (decimal in C# = f64 in Rust for DynamoDB Number type)
    
    // Business fields
    #[serde(rename = "RazonSocial")]
    pub razon_social: String,
    
    #[serde(rename = "Domicilio")]
    pub domicilio: String,
    
    #[serde(rename = "CodPostal")]
    pub cod_postal: String,
    
    #[serde(rename = "Localidad")]
    pub localidad: String,
    
    #[serde(rename = "NombreProvincia")]
    pub nombre_provincia: String,
    
    #[serde(rename = "Telefono")]
    pub telefono: String,
}

/// Expected CSV headers for each DynamoDB table
/// ⚠️ MATCHES: chunk-export-v2 output from SiisaRestApi.Process
pub fn get_expected_headers(model_type: &str) -> Result<Vec<&'static str>, String> {
    match model_type {
        "siisa_morosos" | "MorososTransmitDynamoDbModel" => Ok(vec![
            "Cuil", "IdTransmit", "NroDoc", "ApellidoNombre", "IdCliente", "IdRegion",
            "RazonSocial", "Telefono", "NombreRegion", "NombreCategoria", "Periodo",
            "IdEntidad", "CreateDate", "CreateUser"
        ]),
        "siisa_personas_telefonos" | "PersonasTelefonosDynamoDbModel" => Ok(vec![
            "Cuil", "IdTelefono", "Telefono", "Prefijo", "CreateUser", "CreateDate"
        ]),
        "siisa_empleadores" | "EmpleadorDynamoDbModel" => Ok(vec![
            "Cuit",              // PartitionKey (Type N)
            "RazonSocial",
            "Domicilio",
            "CodPostal",
            "Localidad",
            "NombreProvincia",
            "Telefono"
        ]),
        "siisa_empleadores_relaciones" | "EmpleadorRelacionDynamoDbModel" => Ok(vec![
            "Cuil",              // PartitionKey (Type N - long)
            "Cuit",              // SortKey (Type N - long)
            "FechaIngreso",      // String
            "FechaBaja"          // String
        ]),
        _ => Err(format!(
            "Unknown DynamoDB model: '{}'\n\
             Supported: siisa_morosos, siisa_personas_telefonos, siisa_empleadores", 
            model_type
        ))
    }
}

/// Validate CSV header against expected DynamoDB schema
/// Compatible with SiisaRestApi.Process chunk-export-v2 output
pub fn validate_headers(actual_headers: &[String], model_type: &str) -> Result<(), String> {
    let expected = match model_type {
        "siisa_morosos" => vec![
            "Cuil", "NroDoc", "ApellidoNombre", "IdCliente", "IdRegion",
            "RazonSocial", "Telefono", "NombreRegion", "NombreCategoria",
            "Periodo", "IdEntidad", "CreateDate", "CreateUser", "IdTransmit"
        ],
        "personas_telefonos" => vec![
            "IdCliente", "NroTelefono", "NombreTitular", "Tipo",
            "CodigoPais", "CodigoArea", "Numero", "CreateDate",
            "CreateUser", "Observaciones", "Activo"
        ],
        "siisa_empleadores" => vec![
            "Cuit", "RazonSocial", "Domicilio", "CodPostal",
            "Localidad", "NombreProvincia", "Telefono"
        ],
        _ => return Err(format!("Unknown model type : {}", model_type))
    };

    let missing: Vec<_> = expected.iter()
        .filter(|&col| !actual_headers.contains(&col.to_string()))
        .collect();

    let extra: Vec<_> = actual_headers.iter()
        .filter(|col| !expected.contains(&col.as_str()))
        .collect();

    if missing.is_empty() && extra.is_empty() {
        Ok(())
    } else {
        let mut errors = vec![];
        if !missing.is_empty() {
            errors.push(format!("Missing columns: {:?}", missing));
        }
        if !extra.is_empty() {
            errors.push(format!("Extra columns: {:?}", extra));
        }
        Err(errors.join("; "))
    }
}

/// Parse DynamoDB PartitionKey + SortKey from CSV record
/// Extended to support siisa_empleadores
pub fn parse_dynamodb_key(record: &csv::StringRecord, model_type: &str) -> Result<String, String> {
    match model_type {
        "siisa_morosos" | "MorososTransmitDynamoDbModel" => {
            if record.len() < 2 {
                return Err("Record too short to contain DynamoDB key".to_string());
            }
            
            let cuil = record.get(0).ok_or("Missing Cuil (PartitionKey)")?;
            let id_transmit = record.get(1).ok_or("Missing IdTransmit (SortKey)")?;
            
            Ok(format!("{{Cuil={},IdTransmit={}}}", cuil, id_transmit))
        }
        "siisa_personas_telefonos" => {
            if record.len() < 2 {
                return Err("Record too short to contain DynamoDB key".to_string());
            }
            
            let cuil = record.get(0).ok_or("Missing Cuil")?;
            let id_telefono = record.get(1).ok_or("Missing IdTelefono")?;
            
            Ok(format!("{{Cuil={},IdTelefono={}}}", cuil, id_telefono))
        }
        "siisa_empleadores" | "EmpleadorDynamoDbModel" => {
            if record.is_empty() {
                return Err("Record is empty".to_string());
            }
            
            let cuit = record.get(0).ok_or("Missing Cuit (PartitionKey)")?;
            
            Ok(format!("{{Cuit={}}}", cuit))
        }
        "siisa_empleadores_relaciones" | "EmpleadorRelacionDynamoDbModel" => {
            if record.len() < 2 {
                return Err("Record too short to contain DynamoDB composite key".to_string());
            }
            
            let cuil = record.get(0).ok_or("Missing Cuil (PartitionKey)")?;
            let cuit = record.get(1).ok_or("Missing Cuit (SortKey)")?;
            
            Ok(format!("{{Cuil={},Cuit={}}}", cuil, cuit))
        }
        _ => Err(format!("Unknown model type: {}", model_type))
    }
}

/// Parse SQL CompositePrimaryKey from CSV (for resume functionality)
/// Pattern: {IdCliente, IdTransmit, NroDoc}
/// 
/// ⚠️ Used by chunk-export-v2 for resume points, NOT for DynamoDB import
/// DynamoDB uses parse_dynamodb_key() instead
pub fn parse_sql_composite_key(record: &csv::StringRecord) -> Result<(i32, i32, String), String> {
    // En chunk-export-v2, el orden es:
    // Cuil, IdTransmit, NroDoc, ApellidoNombre, IdCliente, IdRegion, ...
    // Necesitamos extraer IdCliente (pos 4), IdTransmit (pos 1), NroDoc (pos 2)
    
    if record.len() < 6 {
        return Err("Record too short for SQL composite key extraction".to_string());
    }
    
    let id_transmit: i32 = record.get(1)
        .ok_or("Missing IdTransmit")?
        .parse()
        .map_err(|e| format!("Invalid IdTransmit: {}", e))?;
    
    let nro_doc = record.get(2)
        .ok_or("Missing NroDoc")?
        .to_string();
    
    let id_cliente: i32 = record.get(4)
        .ok_or("Missing IdCliente")?
        .parse()
        .map_err(|e| format!("Invalid IdCliente: {}", e))?;
    
    Ok((id_cliente, id_transmit, nro_doc))
}

/// Format DynamoDB key for display (compatible with EfficientDynamoDb)
pub fn format_dynamodb_key(cuil: f64, id_transmit: i32) -> String {
    format!("{{Cuil={},IdTransmit={}}}", cuil, id_transmit)
}

/// Format SQL CompositePrimaryKey for display (compatible with C# ToString())
pub fn format_sql_composite_key(id_cliente: i32, id_transmit: i32, nro_doc: &str) -> String {
    format!("{{IdCliente={},IdTransmit={},NroDoc={}}}", 
            id_cliente, id_transmit, nro_doc)
}

/// Get column index for a specific field (useful for validation)
pub fn get_column_index(field_name: &str, model_type: &str) -> Result<usize, String> {
    let headers = get_expected_headers(model_type)?;
    
    headers.iter()
        .position(|&h| h == field_name)
        .ok_or(format!("Field '{}' not found in model '{}'", field_name, model_type))
}

/// Validate DynamoDB attribute types (basic type checking)
pub fn validate_field_type(value: &str, field_name: &str, model_type: &str) -> Result<(), String> {
    match model_type {
        "siisa_morosos" | "MorososTransmitDynamoDbModel" => {
            match field_name {
                "Cuil" => {
                    value.parse::<f64>()
                        .map_err(|_| format!("Invalid Cuil (must be decimal): {}", value))?;
                }
                "IdTransmit" | "IdCliente" | "IdRegion" | "IdEntidad" => {
                    value.parse::<i32>()
                        .map_err(|_| format!("Invalid {} (must be integer): {}", field_name, value))?;
                }
                "NroDoc" | "ApellidoNombre" | "RazonSocial" | "Telefono" | 
                "NombreRegion" | "NombreCategoria" | "Periodo" | 
                "CreateDate" | "CreateUser" => {
                    // String fields - just check not empty for required fields
                    if value.is_empty() && field_name != "Telefono" {
                        return Err(format!("Field {} cannot be empty", field_name));
                    }
                }
                _ => {
                    return Err(format!("Unknown field: {}", field_name));
                }
            }
        }
        "siisa_empleadores" | "EmpleadorDynamoDbModel" => {
            match field_name {
                "Cuit" => {
                    value.parse::<f64>()
                        .map_err(|_| format!("Invalid Cuit (must be decimal): {}", value))?;
                }
                "RazonSocial" | "Domicilio" | "CodPostal" | "Localidad" | "NombreProvincia" | "Telefono" => {
                    // String fields - allow empty for Telefono
                    if value.is_empty() && field_name != "Telefono" {
                        return Err(format!("Field {} cannot be empty", field_name));
                    }
                }
                _ => {
                    return Err(format!("Unknown field: {}", field_name));
                }
            }
        }
        "siisa_empleadores_relaciones" | "EmpleadorRelacionDynamoDbModel" => {
            match field_name {
                "Cuil" | "Cuit" => {
                    value.parse::<i64>()
                        .map_err(|_| format!("Invalid {} (must be long integer): {}", field_name, value))?;
                }
                "FechaIngreso" | "FechaBaja" => {
                    // String fields for dates - can be empty (nullable)
                    // Validation: if not empty, should be valid date format
                    if !value.is_empty() && !is_valid_date_format(value) {
                        return Err(format!("Invalid date format for {}: {} (expected yyyy-MM-dd or yyyy-MM-dd HH:mm:ss)", field_name, value));
                    }
                }
                _ => {
                    return Err(format!("Unknown field: {}", field_name));
                }
            }
        }
        _ => {
            return Err(format!("Validation not implemented for model: {}", model_type));
        }
    }
    
    Ok(())
}

/// Validate that a string contains only digits (DynamoDB Number type validation)
/// Compatible with C# IsDigitsOnly() pattern
pub fn is_digits_only(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_digit())
}

/// Validate date format for EmpleadorRelacionDynamoDbModel dates
/// Accepts yyyy-MM-dd or yyyy-MM-dd HH:mm:ss formats
pub fn is_valid_date_format(value: &str) -> bool {
    // Simple regex-like validation for common date formats
    let parts: Vec<&str> = value.split(' ').collect();
    
    // Check date part (yyyy-MM-dd)
    let date_part = parts[0];
    let date_components: Vec<&str> = date_part.split('-').collect();
    
    if date_components.len() != 3 {
        return false;
    }
    
    // Validate year (4 digits), month (2 digits), day (2 digits)
    if date_components[0].len() != 4 || !is_digits_only(date_components[0]) ||
       date_components[1].len() != 2 || !is_digits_only(date_components[1]) ||
       date_components[2].len() != 2 || !is_digits_only(date_components[2]) {
        return false;
    }
    
    // If there's a time part, validate HH:mm:ss
    if parts.len() == 2 {
        let time_part = parts[1];
        let time_components: Vec<&str> = time_part.split(':').collect();
        
        if time_components.len() != 3 {
            return false;
        }
        
        // Validate hour, minute, second (all 2 digits)
        for component in time_components {
            if component.len() != 2 || !is_digits_only(component) {
                return false;
            }
        }
    } else if parts.len() > 2 {
        return false;
    }
    
    true
}

/// Validate DynamoDB numeric keys (PartitionKey + SortKey type N)
/// Returns Ok(()) if valid, Err with details if invalid
pub fn validate_numeric_key(
    value: &str, 
    key_name: &str, 
    line_num: usize
) -> Result<(), String> {
    let trimmed = value.trim();
    
    if trimmed.is_empty() {
        return Err(format!(
            "Line {}: {} is empty (required for DynamoDB key)",
            line_num, key_name
        ));
    }
    
    if !is_digits_only(trimmed) {
        return Err(format!(
            "Line {}: {} contains non-numeric characters: '{}' (DynamoDB type N requires digits only)",
            line_num, key_name, trimmed
        ));
    }
    
    Ok(())
}

/// Retorna las columnas de clave primaria DynamoDB según el modelo
/// Sigue schema de MorososTransmitDynamoDbModel y PersonasTelefonoDynamoDbModel
pub fn get_dynamodb_key_columns(model_type: &str) -> Result<(String, Option<String>), Box<dyn Error>> {
    match model_type {
        "siisa_morosos" => {
            Ok(("Cuil".to_string(), Some("IdTransmit".to_string())))
        },
        "personas_telefonos" => {
            Ok(("IdCliente".to_string(), Some("NroTelefono".to_string())))
        },
        "siisa_empleadores" => {
            // Solo PartitionKey, sin SortKey
            Ok(("Cuit".to_string(), None))
        },
        "siisa_empleadores_relaciones" => {
            // Composite key: Cuil (PartitionKey) + Cuit (SortKey)
            Ok(("Cuil".to_string(), Some("Cuit".to_string())))
        },
        _ => Err(format!("Unknown DynamoDB model type: {}", model_type).into())
    }
}

#[derive(Debug, Clone)]
pub struct DynamoDbModel {
    pub table_name: &'static str,
    pub partition_key: &'static str,
    pub sort_key: &'static str,
    pub numeric_fields: Vec<&'static str>,  // Todos los campos Type N (DynamoDB Number)
    pub expected_columns: usize,
    pub column_mapping: HashMap<&'static str, usize>,
}

impl DynamoDbModel {
    pub fn siisa_morosos() -> Self {
        let mut mapping = HashMap::new();
        mapping.insert("Cuil", 0);
        mapping.insert("IdTransmit", 1);
        mapping.insert("NroDoc", 2);
        mapping.insert("ApellidoNombre", 3);
        mapping.insert("IdCliente", 4);
        mapping.insert("IdRegion", 5);
        mapping.insert("RazonSocial", 6);
        mapping.insert("Telefono", 7);
        mapping.insert("NombreRegion", 8);
        mapping.insert("NombreCategoria", 9);
        mapping.insert("Periodo", 10);
        mapping.insert("IdEntidad", 11);
        mapping.insert("CreateDate", 12);
        mapping.insert("CreateUser", 13);

        DynamoDbModel {
            table_name: "siisa_morosos",
            partition_key: "Cuil",
            sort_key: "IdTransmit",
            numeric_fields: vec![
                "Cuil", "IdTransmit", "NroDoc", "IdCliente", 
                "IdRegion", "Periodo", "IdEntidad"
            ],
            expected_columns: 14,
            column_mapping: mapping,
        }
    }

    pub fn personas_telefonos() -> Self {
        let mut mapping = HashMap::new();
        mapping.insert("IdCliente", 0);
        mapping.insert("IdTransmit", 1);
        mapping.insert("NroDoc", 2);
        mapping.insert("NroTelefono", 3);
        mapping.insert("ApellidoNombre", 4);
        mapping.insert("RazonSocial", 5);
        mapping.insert("NombreRegion", 6);
        mapping.insert("Direccion", 7);
        mapping.insert("DireccionAfip", 8);
        mapping.insert("Mail", 9);
        mapping.insert("IdEntidad", 10);
        mapping.insert("CreateDate", 11);
        mapping.insert("CreateUser", 12);

        DynamoDbModel {
            table_name: "personas_telefonos",
            partition_key: "IdCliente",
            sort_key: "IdTransmit",
            numeric_fields: vec![
                "IdCliente", "IdTransmit", "NroDoc", 
                "NroTelefono", "IdEntidad"
            ],
            expected_columns: 13,
            column_mapping: mapping,
        }
    }

    /// ✅ NUEVO: Modelo para siisa_empleadores
    /// Compatible con EmpleadorDynamosDbModel.cs
    pub fn siisa_empleadores() -> Self {
        let mut mapping = HashMap::new();
        mapping.insert("Cuit", 0);
        mapping.insert("RazonSocial", 1);
        mapping.insert("Domicilio", 2);
        mapping.insert("CodPostal", 3);
        mapping.insert("Localidad", 4);
        mapping.insert("NombreProvincia", 5);
        mapping.insert("Telefono", 6);

        DynamoDbModel {
            table_name: "siisa_empleadores",
            partition_key: "Cuit",
            sort_key: "",  // No SortKey
            numeric_fields: vec![
                "Cuit"  // Solo PartitionKey es Type N
            ],
            expected_columns: 7,
            column_mapping: mapping,
        }
    }

    /// ✅ NUEVO: Modelo para siisa_empleadores_relaciones
    /// Compatible con EmpleadorRelacionDynamoDbModel.cs (4 campos)
    pub fn siisa_empleadores_relaciones() -> Self {
        let mut mapping = HashMap::new();
        mapping.insert("Cuil", 0);           // PartitionKey (long)
        mapping.insert("Cuit", 1);           // SortKey (long)
        mapping.insert("FechaIngreso", 2);   // String
        mapping.insert("FechaBaja", 3);      // String

        DynamoDbModel {
            table_name: "siisa_empleadores_relaciones",
            partition_key: "Cuil",
            sort_key: "Cuit",  // Composite key: Cuil + Cuit
            numeric_fields: vec![
                "Cuil", "Cuit"  // Both keys are Type N (long)
            ],
            expected_columns: 4,  // Solo 4 campos según EmpleadorRelacionDynamoDbModel
            column_mapping: mapping,
        }
    }

    pub fn from_model_type(model_type: &str) -> Option<Self> {
        match model_type.to_lowercase().as_str() {
            "siisa_morosos" => Some(Self::siisa_morosos()),
            "personas_telefonos" => Some(Self::personas_telefonos()),
            "siisa_empleadores" => Some(Self::siisa_empleadores()),
            "siisa_empleadores_relaciones" => Some(Self::siisa_empleadores_relaciones()),  // ✅ NUEVO
            _ => None,
        }
    }
}

/// Parse and display DynamoDB keys from CSV records
/// Compatible with all supported models
pub fn parse_keys_from_csv(csv_path: &str, model_type: &str) -> Result<(), Box<dyn std::error::Error>> {
    use csv::ReaderBuilder;
    use std::fs::File;
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  DynamoDB Key Parser                                         ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    println!("📄 File:  {}", csv_path);
    println!("📋 Model: {}", model_type);
    
    let model = DynamoDbModel::from_model_type(model_type)
        .ok_or_else(|| format!("Unknown model type: {}", model_type))?;
    
    println!("🔑 Keys:  {} + {}", model.partition_key, 
        if model.sort_key.is_empty() { "(no sort key)" } else { model.sort_key });
    println!();
    
    let file = File::open(csv_path)?;
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    
    let mut count = 0;
    const MAX_DISPLAY: usize = 10;
    
    for (i, result) in reader.records().enumerate() {
        let record = result?;
        
        if i >= MAX_DISPLAY {
            println!("... (showing first {} records)", MAX_DISPLAY);
            break;
        }
        
        match parse_dynamodb_key_from_record(&record, model_type) {
            Ok(key) => {
                println!("Record {}: {}", i + 1, key);
                count += 1;
            }
            Err(e) => {
                eprintln!("❌ Record {}: Error - {}", i + 1, e);
            }
        }
    }
    
    println!("\n✅ Processed {} records successfully", count);
    Ok(())
}

/// Parse DynamoDB key from CSV record
/// Returns formatted key string for different model types
pub fn parse_dynamodb_key_from_record(record: &csv::StringRecord, model_type: &str) -> Result<String, String> {
    match model_type.to_lowercase().as_str() {
        "siisa_morosos" | "morosos_transmit_dynamodb_model" => {
            if record.len() < 3 {
                return Err("Record too short to contain DynamoDB composite key".to_string());
            }
            
            let cuil = record.get(0).ok_or("Missing Cuil (PartitionKey)")?;
            let id_transmit = record.get(1).ok_or("Missing IdTransmit (SortKey)")?;
            
            Ok(format!("{{Cuil={},IdTransmit={}}}", cuil, id_transmit))
        }
        "siisa_personas_telefonos" => {
            if record.len() < 2 {
                return Err("Record too short to contain DynamoDB key".to_string());
            }
            
            let cuil = record.get(0).ok_or("Missing Cuil")?;
            let id_telefono = record.get(1).ok_or("Missing IdTelefono")?;
            
            Ok(format!("{{Cuil={},IdTelefono={}}}", cuil, id_telefono))
        }
        "siisa_empleadores" | "empleador_dynamodb_model" => {
            if record.is_empty() {
                return Err("Record is empty".to_string());
            }
            
            let cuit = record.get(0).ok_or("Missing Cuit (PartitionKey)")?;
            
            Ok(format!("{{Cuit={}}}", cuit))
        }
        "siisa_empleadores_relaciones" | "empleador_relacion_dynamodb_model" => {
            if record.len() < 2 {
                return Err("Record too short to contain DynamoDB composite key".to_string());
            }
            
            let cuil = record.get(0).ok_or("Missing Cuil (PartitionKey)")?;
            let cuit = record.get(1).ok_or("Missing Cuit (SortKey)")?;
            
            Ok(format!("{{Cuil={},Cuit={}}}", cuil, cuit))
        }
        _ => Err(format!("Unknown model type: {}", model_type))
    }
}