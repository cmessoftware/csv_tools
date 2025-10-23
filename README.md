# CSV Tools - Herramientas de Procesamiento CSV

🛠️ **Suite de herramientas para procesamiento, análisis y conversión de archivos CSV**

## 📋 Descripción

CSV Tools es una colección de herramientas desarrolladas en **Rust** y **Python** para el procesamiento eficiente de archivos CSV de gran tamaño.

## 🚀 Características Principales

- ✅ **Alto rendimiento** con Rust para archivos de millones de registros
- ✅ **Procesamiento por chunks** para archivos que no caben en memoria
- ✅ **Deduplicación inteligente** preservando headers
- ✅ **Análisis de integridad** y detección de headers duplicados
- ✅ **Comparación de archivos** con reportes detallados
- ✅ **Filtrado y limpieza** de datos
- ✅ **Conteo rápido** de registros múltiples archivos

## 📁 Estructura del Proyecto

```
csv_tools/
├── src/
│   ├── main.rs                    # Herramienta principal en Rust
│   ├── decoder.rs                 # Decodificador de formatos personalizados.
│   └── analyzer.rs                # Analizador de estructuras de datos
├── csv_tools.py                   # Herramienta Python con Polars
├── Cargo.toml                     # Configuración Rust
└── README.md                      # Este archivo
```

## 🔧 Instalación

### Prerrequisitos

- **Rust** (para herramientas de alto rendimiento)
- **Python 3.8+** (para herramientas de análisis)
- **Visual Studio Build Tools** (Windows, para Rust)

### Instalación de Rust

```bash
# Instalar Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# En Windows, también instalar Visual Studio Build Tools
winget install Microsoft.VisualStudio.2022.BuildTools

# En el caso de ya tener el Visual Studio 2022 en el mismo ambiente el isual Studio Build Tools ya estaria instalado
```

### Instalación de Python

```bash
# Windows
winget install Python.Python.3.12

# Instalar dependencias Python
pip install polars
```

### Compilación de Herramientas Rust

```bash
# Compilar versión optimizada
cargo build --release

# El ejecutable estará en target/release/csv_tools.exe
```

## 📚 Herramientas Disponibles

### 1. 🦀 **CSV Tools (Rust)** - Alto Rendimiento

Herramienta principal para procesamiento intensivo de archivos CSV grandes.

#### Comandos Disponibles:

```bash
# Limpiar headers duplicados
./target/release/csv_tools.exe clean input.csv output.csv

# Filtrar registros por columna
./target/release/csv_tools.exe filter input.csv output.csv "NombreColumna" "valor"

# Verificar headers duplicados
./target/release/csv_tools.exe check input.csv

# Contar líneas rápidamente
./target/release/csv_tools.exe count input.csv

# Contar líneas en múltiples archivos (TOTAL, incluye duplicados)
./target/release/csv_tools.exe count_all file_list.txt

# Contar registros únicos en múltiples archivos (RÁPIDO, pero requiere RAM)
./target/release/csv_tools.exe count_unique file_list.txt

# Merge y deduplicación in-memory (RÁPIDO, pero limitado por RAM)
./target/release/csv_tools.exe merge_dedup file_list.txt output.csv

# 🆕 ESTIMAR memoria necesaria antes de procesar (RECOMENDADO)
./target/release/csv_tools.exe estimate_memory file_list.txt

# 🆕 DEDUPLICACIÓN EXTERNA para archivos GIGANTES (sin límite de RAM)
./target/release/csv_tools.exe external_dedup file_list.txt output.csv

# Comparar archivos
./target/release/csv_tools.exe compare file1.csv file2.csv 100

# Ayuda
./target/release/csv_tools.exe help
```

#### 🚨 **Guía de Comandos según Tamaño de Datos:**

```bash
# Para archivos < 10M registros únicos (< 2GB RAM)
./target/release/csv_tools.exe count_unique file_list.txt
./target/release/csv_tools.exe merge_dedup file_list.txt output.csv

# Para archivos 10M-50M registros únicos (2-10GB RAM)
./target/release/csv_tools.exe estimate_memory file_list.txt  # ⚠️ Verificar primero
./target/release/csv_tools.exe count_unique file_list.txt     # Si RAM es suficiente

# Para archivos 50M+ registros únicos (>10GB RAM requerida)
./target/release/csv_tools.exe external_dedup file_list.txt output.csv  # 🔥 Siempre funciona
```

#### Ejemplo de Uso:

```bash
# Procesar archivo de 1M de registros
./target/release/csv_tools.exe count big_file.csv
# Output: Counting lines in file: big_file.csv...
#         Time taken to count 1000000 lines: 0.45 seconds

# Merge multiple files with deduplication
./target/release/csv_tools.exe merge_dedup chunks_list.txt merged_output.csv
# Output: Merge completado, duplicados eliminados.
```

### 2. 🐍 **CSV Tools (Python)** - Análisis Avanzado

Herramienta Python con Polars para análisis más flexibles.

```bash
# Usar con Python
python csv_tools.py clean input.csv output.csv
python csv_tools.py filter input.csv output.csv "columna" "valor"
python csv_tools.py count input.csv
python csv_tools.py merge_dedup file_list.txt output.csv
```

## 🎯 Casos de Uso Específicos

### Análisis de Calidad de Datos

```bash
# Verificar headers duplicados (problema común en exports)
./target/release/csv_tools.exe check exported_data.csv

# Comparar archivos para detectar diferencias
./target/release/csv_tools.exe compare original.csv processed.csv 1000

# Filtrar registros específicos
./target/release/csv_tools.exe filter data.csv filtered.csv "IdCliente" "123"
```

## 📊 Rendimiento

### Benchmarks Típicos

| Operación | Archivo | Tiempo (Rust) | Tiempo (Python) | Notas |
|-----------|---------|---------------|-----------------|-------|
| Count | 1M registros | 0.45s | 2.3s | Archivo individual |
| Count All | 5 archivos x 200K | 1.8s | 5.2s | **Total con duplicados** |
| Count Unique | 5 archivos x 200K | 2.4s | N/A | **Solo únicos, rápido** |
| Count Unique | 2.25M registros | 1.8s | N/A | **Límite práctico in-memory** |
| **External Dedup** | **358M registros** | **~2-5 min** | **N/A** | **🔥 Sin límite de RAM** |
| Clean Headers | 500K registros | 1.2s | 3.8s | Limpieza de headers |
| Merge + Dedup | 5 archivos x 200K | 3.4s | 12.1s | **Genera archivo output** |
| Filter | 1M registros | 2.1s | 4.7s | Filtrado por columna |

### 🧠 **Límites de Memoria**

| Registros Únicos | RAM Estimada | Comando Recomendado | Notas |
|------------------|--------------|---------------------|-------|
| < 1M | < 500MB | `count_unique` | ✅ Súper rápido |
| 1M - 5M | 500MB - 2GB | `count_unique` | ✅ Rápido |
| 5M - 25M | 2GB - 10GB | `estimate_memory` + `count_unique` | ⚠️ Verificar RAM |
| 25M - 100M+ | 10GB - 40GB+ | `external_dedup` | 🔥 **Usar siempre** |
| 100M+ | 40GB+ | `external_dedup` | 🚀 **Unlimited scale** |

**💡 Recomendación**: Usar herramientas Rust para archivos > 100K registros

## 🔍 Formatos Soportados

### CSV Estándar
- Headers automáticos
- Separadores: coma, punto y coma, tab
- Encoding: UTF-8, UTF-16, ISO-8859-1


## 🛡️ Validaciones y Controles

- ✅ **Verificación de headers** duplicados
- ✅ **Validación de estructura** de archivos
- ✅ **Detección de encoding** automática
- ✅ **Manejo de errores** robusto
- ✅ **Reportes detallados** de procesamiento
- ✅ **Preservación de datos** originales

## 🚨 Limitaciones Conocidas

1. **Memoria**: Archivos > 10GB requieren procesamiento por chunks
2. **Encoding**: Algunos archivos legacy pueden requerir conversión manual
3. **Headers**: Se asume que la primera línea es header
4. **Python**: Requiere instalación de Polars para funcionalidad completa

## 🆘 Soporte y Troubleshooting

### Problemas Comunes

**Error: "linker link.exe not found"**
```bash
# Instalar Visual Studio Build Tools
winget install Microsoft.VisualStudio.2022.BuildTools
```

**Error: "Python module polars not found"**
```bash
pip install polars
```

**Archivos muy grandes causing memory issues**
```bash
# Usar herramientas Rust en lugar de Python
./target/release/csv_tools.exe count huge_file.csv
```
---

**⚡ Optimizado para el procesamiento de millones de registros**
