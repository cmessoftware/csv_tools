#!/usr/bin/env python3
"""
Generador de archivos CSV grandes para probar progress bars
"""
import csv
import random
from datetime import datetime, timedelta

def generate_large_csv(filename, num_records=100000):
    """Genera un archivo CSV grande con datos simulados SIISA"""
    
    # Headers del formato SIISA
    headers = [
        'Cuil', 'NroDoc', 'ApellidoNombre', 'IdCliente', 'IdRegion', 
        'RazonSocial', 'Telefono', 'NombreRegion', 'NombreCategoria', 
        'Periodo', 'IdEntidad', 'CreateDate', 'CreateUser'
    ]
    
    # Datos base para generar variaciones
    nombres = [
        'GARCIA MARIA', 'RODRIGUEZ JUAN', 'MARTINEZ LUCIA', 'LOPEZ CARLOS',
        'GONZALEZ ANA', 'FERNANDEZ PEDRO', 'DIAZ SOFIA', 'RUIZ MIGUEL',
        'HERNANDEZ LAURA', 'JIMENEZ ANTONIO', 'ALVAREZ CARLA', 'MORENO LUIS',
        'MUÑOZ ELENA', 'BLANCO JORGE', 'SANZ PATRICIA', 'CASTRO RAMON',
        'ORTEGA SILVIA', 'RUBIO FERNANDO', 'MARIN TERESA', 'SOTO DANIEL'
    ]
    
    regiones = [
        'Capital y GBA', 'Interior Buenos Aires', 'Córdoba', 'Santa Fe',
        'Mendoza', 'Tucumán', 'Entre Ríos', 'Misiones'
    ]
    
    categorias = [
        'entre 31 y 60', 'entre 61 y 90', 'entre 91 y 120', 'superior a 120'
    ]
    
    entidades = ['FLEX FIDEICOMISO FINANCIERO', 'BANCO NACION', 'BANCO PROVINCIA']
    users = ['celesteferrarese', 'admin', 'migrator', 'system']
    
    print(f"🔄 Generando {filename} con {num_records:,} registros...")
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        for i in range(num_records):
            # Generar datos aleatorios pero realistas
            doc_base = 20000000 + i
            cuil = f"{random.choice([20, 23, 24, 27])}{doc_base}{random.randint(0, 9)}"
            
            nombre = random.choice(nombres)
            id_cliente = random.randint(1, 5)
            id_region = random.randint(1, 8)
            razon_social = random.choice(entidades)
            telefono = f"11 {random.randint(2000, 9999)}-{random.randint(1000, 9999)}"
            nombre_region = random.choice(regiones)
            categoria = random.choice(categorias)
            periodo = random.choice(['202201', '202202', '202203', '202301'])
            id_entidad = random.randint(400, 500)
            
            # Fecha aleatoria
            base_date = datetime(2022, 1, 1)
            random_days = random.randint(0, 730)
            create_date = (base_date + timedelta(days=random_days)).strftime('%m/%d/%Y %I:%M:%S %p')
            
            create_user = random.choice(users)
            
            row = [
                cuil, doc_base, nombre, id_cliente, id_region,
                razon_social, telefono, nombre_region, categoria,
                periodo, id_entidad, create_date, create_user
            ]
            
            writer.writerow(row)
            
            # Progress cada 10K registros
            if (i + 1) % 10000 == 0:
                print(f"  ✅ {i + 1:,} registros generados...")
    
    print(f"🎉 {filename} generado exitosamente con {num_records:,} registros!")

def main():
    """Genera archivos de diferentes tamaños para testing"""
    
    # Archivos REALMENTE grandes para desafiar a Rust 🦀💪
    test_files = [
        ('massive_file1.csv', 500000),   # 500K registros
        ('massive_file2.csv', 750000),   # 750K registros  
        ('massive_file3.csv', 1000000),  # 1M registros (¡1 MILLÓN!)
    ]
    
    print("🚀 Generando archivos MASIVOS para desafiar a Rust...")
    print("⚡ Rust es tan rápido que necesitamos archivos de MILLONES de registros!")
    print()
    
    for filename, size in test_files:
        print(f"🎯 Preparando {size:,} registros - esto debería hacer sudar a Rust...")
        generate_large_csv(filename, size)
        print()
    
    # Crear lista de archivos masivos
    with open('massive_files.txt', 'w') as f:
        for filename, _ in test_files:
            f.write(f"{filename}\n")
    
    print("📝 Lista de archivos masivos creada: massive_files.txt")
    print(f"🔥 Total: {sum(size for _, size in test_files):,} registros combinados!")
    print()
    print("🎯 Ahora SÍ deberías ver el progress bar:")
    print("  ./target/release/csv_tools.exe count_all massive_files.txt")
    print("  ./target/release/csv_tools.exe count_unique massive_files.txt")
    print("  ./target/release/csv_tools.exe merge_dedup massive_files.txt merged_massive.csv")
    print()
    print("💡 Si aún es muy rápido, Rust oficialmente ganó. 🏆")

if __name__ == "__main__":
    main()
