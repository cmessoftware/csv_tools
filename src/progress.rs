use std::time::{Instant, Duration};
use std::io::{self, Write};

/// Estructura para manejar el progreso de operaciones largas con barra visual
pub struct ProgressTracker {
    start_time: Instant,
    total_items: usize,
    current_item: usize,
    last_update: Instant,
}

impl ProgressTracker {
    /// Crea un nuevo tracker de progreso
    pub fn new(total_items: usize) -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            total_items,
            current_item: 0,
            last_update: now,
        }
    }

    /// Actualiza el progreso actual
    pub fn update(&mut self, current_item: usize) {
        self.current_item = current_item;
        let now = Instant::now();
        
        // Actualizar cada 500ms para no sobrecargar la consola
        if now.duration_since(self.last_update) >= Duration::from_millis(500) {
            self.display_progress();
            self.last_update = now;
        }
    }

    /// Muestra la barra de progreso en la consola
    fn display_progress(&self) {
        if self.total_items == 0 {
            return;
        }

        let percentage = (self.current_item as f64 / self.total_items as f64 * 100.0).min(100.0);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        
        let eta = if self.current_item > 0 {
            let rate = self.current_item as f64 / elapsed;
            let remaining_items = self.total_items - self.current_item;
            remaining_items as f64 / rate
        } else {
            0.0
        };

        // Crear barra de progreso visual
        let bar_width = 40;
        let filled = ((percentage / 100.0) * bar_width as f64) as usize;
        let bar: String = "█".repeat(filled) + &"░".repeat(bar_width - filled);

        print!("\r[{}] {:.1}% | {}/{} | ETA: {:.0}s | Elapsed: {:.1}s", 
               bar, percentage, self.current_item, self.total_items, eta, elapsed);
        io::stdout().flush().unwrap();
    }

    /// Finaliza el progreso mostrando mensaje de completado
    pub fn finish(&self, message: &str) {
        println!("\r{} ✅ Completado en {:.2} segundos", message, self.start_time.elapsed().as_secs_f64());
    }

    /// Fuerza una actualización inmediata de la barra de progreso
    pub fn force_update(&mut self) {
        self.display_progress();
        self.last_update = Instant::now();
    }
}
