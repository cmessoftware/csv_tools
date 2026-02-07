use std::env;
use std::fs;
use std::path::Path;

fn main() {
    // ✅ Incrementar número de build
    let build_number = increment_build_number();
    
    // ✅ Timestamp de compilación usando chrono
    let build_time = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    
    // ✅ Versión desde Cargo.toml
    let version = env!("CARGO_PKG_VERSION");
    
    // ✅ Target platform (e.g., x86_64-pc-windows-msvc)
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    
    println!("cargo:warning=Building CSV Tools v{}.{} (Build #{})", version, build_number, build_number);
    
    // ✅ Pasar variables al código Rust usando rustc-env
    println!("cargo:rustc-env=CSV_TOOLS_VERSION={}", version);
    println!("cargo:rustc-env=BUILD_NUMBER={}", build_number);
    println!("cargo:rustc-env=BUILD_DATE={}", build_time);
    println!("cargo:rustc-env=BUILD_TIMESTAMP={}", build_time);
    println!("cargo:rustc-env=TARGET={}", target);
}

fn increment_build_number() -> u32 {
    let build_file = "build_number.txt";
    let current = if Path::new(build_file).exists() {
        fs::read_to_string(build_file)
            .unwrap_or_else(|_| "0".to_string())
            .trim()
            .parse::<u32>()
            .unwrap_or(0)
    } else {
        0
    };
    
    let next = current + 1;
    fs::write(build_file, next.to_string()).unwrap();
    next
}