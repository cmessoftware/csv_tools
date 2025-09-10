use std::fs::File;
use std::io::{BufRead, BufReader};
use std::error::Error;

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
