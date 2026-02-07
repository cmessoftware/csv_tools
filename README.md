# CSV Tools - Herramientas de Procesamiento CSV

🛠️ Suite de utilidades para inspección, limpieza y análisis de CSV a gran escala (orientado a SIISA).

## Resumen rápido
- Implementado en Rust (alto rendimiento).
- Diseñado para trabajar con archivos de millones de filas en streaming (sin cargar todo en memoria).
- Integración directa con SiisaRestApi.Process para depuración de migraciones.

## Instalación / Build

Requisitos:
- Rust toolchain

Compilar (optimizado):
```powershell
# desde el directorio csv_tools
cargo build --release
# ejecutable: target\release\csv_tools.exe
```

Recomendado en Cargo.toml:
```toml
[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
strip = true
```

## Comandos principales (Rust)

- clean <input> <output>
- filter <input> <output> <col> <value>
- check <input> <model>
- count <input>
- count_all <file_list.txt>
- count_unique <file_list.txt>
- merge_dedup <file_list.txt> <output.csv>
- external_dedup <file_list.txt> <output.csv>
  - Recomendado para archivos gigantes (decenas de GB), usa herramientas externas para ordenar/deduplicar con poco uso de RAM.

Nuevos comandos útiles
- tail <input> <num_rows>
  - Muestra las últimas N filas del CSV (conserva el header).
  - Ejemplo: .\target\release\csv_tools.exe tail "C:\data\siisa.csv" 50
- find_latest_date <input> <date_column> [mdy|iso]
  - Encuentra la fecha más reciente en una columna dada.
  - Formatos soportados:
    - mdy (default): "8/13/2025 11:00:00", "8/13/2025", y variantes tipo "1,8/13/2025 11:00:00" (elimina coma).
    - iso: "2025-08-13 11:00:00", "2025-08-13T11:00:00", "2025-08-13".
  - Ejemplo: .\target\release\csv_tools.exe find_latest_date ".\siisa.csv" CreateDate mdy
- find_latest_date_batch <file_list.txt> <date_column> [mdy|iso]
  - Busca la fecha más reciente entre múltiples CSV listados (uno por línea) en file_list.txt.
  - Ejemplo: .\target\release\csv_tools.exe find_latest_date_batch ".\file_list.txt" CreateDate mdy
- find_earliest_date_batch <file_list.txt> <date_column> [mdy|iso]
  - Busca la fecha más antigua por archivo y la más antigua global en el conjunto.
  - Ejemplo: .\target\release\csv_tools.exe find_earliest_date_batch ".\file_list.txt" CreateDate mdy
- merge_dedup_mixed <file_list.txt> <output.csv> [expected_header]
  - Une múltiples CSV con y sin header manteniendo un único header en la salida y eliminando duplicados.
  - Si pasas expected_header, se usará como header único; si no, detecta el primero y lo conserva.
  - Ejemplo:
    .\target\release\csv_tools.exe merge_dedup_mixed ".\file_list.txt" ".\merged.csv" "Cuil,NroDoc,ApellidoNombre,IdCliente,IdRegion,RazonSocial,Telefono,NombreRegion,NombreCategoria,Periodo,IdEntidad,CreateDate,CreateUser"
- sort_by_date <input> <output> <date_column> [asc|desc]
  - Ordena un CSV gigante por fecha usando external sort (bajo uso de RAM).
  - Formato soportado: MM/dd/yyyy hh:mm:ss AM/PM (ej. 8/13/2025 11:00:00 AM).
  - También soporta formatos ISO y variantes.
  - Ejemplo ASC: .\target\release\csv_tools.exe sort_by_date ".\siisa.csv" ".\siisa_sorted.csv" CreateDate asc
  - Ejemplo DESC: .\target\release\csv_tools.exe sort_by_date ".\siisa.csv" ".\siisa_sorted_desc.csv" CreateDate desc

## Ejemplos (PowerShell)

```powershell
# Contar filas
.\target\release\csv_tools.exe count ".\data\siisa.csv"

# Merge con deduplicación (archivos muy grandes: usa external_dedup)
.\target\release\csv_tools.exe external_dedup ".\file_list.txt" ".\merged.csv"

# Últimas 100 filas
.\target\release\csv_tools.exe tail ".\data\siisa.csv" 100

# Fecha más reciente en un archivo
.\target\release\csv_tools.exe find_latest_date ".\data\siisa.csv" CreateDate mdy

# Fecha más reciente entre muchos archivos
.\target\release\csv_tools.exe find_latest_date_batch ".\file_list.txt" CreateDate mdy

# Fecha más antigua por archivo y global
.\target\release\csv_tools.exe find_earliest_date_batch ".\file_list.txt" CreateDate mdy

# Merge de archivos con/sin header (mismo modelo)
.\target\release\csv_tools.exe merge_dedup_mixed ".\file_list.txt" ".\out.csv" "Cuil,NroDoc,ApellidoNombre,IdCliente,IdRegion,RazonSocial,Telefono,NombreRegion,NombreCategoria,Periodo,IdEntidad,CreateDate,CreateUser"

# Ordenar por fecha (ascendente)
.\target\release\csv_tools.exe sort_by_date ".\data\siisa.csv" ".\siisa_sorted.csv" CreateDate asc

# Ordenar por fecha (descendente)
.\target\release\csv_tools.exe sort_by_date ".\data\siisa.csv" ".\siisa_sorted_desc.csv" CreateDate desc
```

## Consejos de rendimiento (50M+ filas)
- Siempre compilar en release: cargo run --release -- <comando> ...
- Coloca los CSV en SSD NVMe para maximizar I/O.
- Usa external_dedup para uniones enormes (bajo uso de memoria).
- Evita imprimir demasiado en consola (los comandos ya limitan logs).
- Usa rutas absolutas en file_list.txt para evitar “NotFound”.
- Si usas formato mdy/iso, especifica uno para reducir intentos de parseo.

## Solución de problemas

- “The system cannot find the file specified.” al usar find_*_batch:
  - Ejecuta desde el mismo directorio donde está file_list.txt, o usa ruta absoluta.
  - Asegúrate de que cada línea de file_list.txt apunte a un archivo existente (puedes probar con Get-Item en PowerShell).
- Archivos sin header en merge:
  - Usa merge_dedup_mixed y, si es posible, pasa expected_header para evitar que la primera línea de un archivo sin header sea tomada como header.

## Integración con SiisaRestApi.Process
- Use csv_tools para preparar/validar el CSV antes de subir a S3.
- Flujo recomendado antes de migrate:
  1. Verificar que el archivo .gz esté en S3 (aws s3 ls).
  2. Ejecutar los checks sobre el CSV local.
  3. Subir archivo limpio a S3 y luego lanzar SiisaRestApi.Process.exe --step=migrate.

## Contacto / Repo
- Equipo SiisaRestApi Development
- Repo: https://github.com/SIISAPosta/SiisaRestApi

---

**⚡ Optimizado para el procesamiento de millones de registros del sistema SIISA**
