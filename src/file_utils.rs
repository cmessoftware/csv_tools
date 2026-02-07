use std::fs::File;
use std::io::{BufReader, BufRead};
use std::error::Error;

/// Lee un archivo de lista de archivos y devuelve las rutas
pub fn read_file_list(file_list_path: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let file = File::open(file_list_path)?;
    let reader = BufReader::new(file);
    
    let mut files = Vec::new();
    for line in reader.lines() {
        let path = line?.trim().to_string();
        if !path.is_empty() && !path.starts_with('#') {
            files.push(path);
        }
    }
    
    Ok(files)
}

/// Calcula el tamaño de un archivo en bytes
pub fn get_file_size(path: &str) -> Result<u64, Box<dyn Error>> {
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.len())
}

/// Formatea bytes en formato legible (KB, MB, GB)
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Valida que un archivo exista
pub fn validate_file_exists(path: &str) -> Result<(), Box<dyn Error>> {
    if !std::path::Path::new(path).exists() {
        return Err(format!("File not found: {}", path).into());
    }
    Ok(())
}

/// Crea un directorio si no existe
pub fn ensure_directory_exists(path: &str) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(path)?;
    Ok(())
}

/// Obtiene el número total de líneas en un archivo (para estimar progreso)
pub fn estimate_file_lines(file_path: &str) -> Result<usize, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().count())
}

/// Obtiene el total de líneas en múltiples archivos listados en un archivo de texto
pub fn estimate_total_lines_from_list(file_list_path: &str) -> Result<usize, Box<dyn Error>> {
    let file = File::open(file_list_path)?;
    let reader = BufReader::new(file);
    let mut total = 0;

    for line in reader.lines() {
        let filename = line?;
        total += estimate_file_lines(&filename)?;
    }

    Ok(total)
}
