use std::time::Instant;
use std::io::{self, Write};

/// Tracker de progreso compatible con SiisaRestApi chunk processing
pub struct ProgressTracker {
    start_time: Instant,
    last_report_time: Instant,
    total_processed: u64,
    report_interval: u64,
}

impl ProgressTracker {
    pub fn new(report_interval: u64) -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            last_report_time: now,
            total_processed: 0,
            report_interval,
        }
    }
    
    pub fn update(&mut self, processed: u64) {
        self.total_processed = processed; // Cambio: asignar en lugar de sumar
        
        if self.total_processed % self.report_interval == 0 {
            self.report();
        }
    }
    
    fn report(&mut self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 {
            self.total_processed as f64 / elapsed
        } else {
            0.0
        };
        
        print!("\r📊 Processed: {} | Rate: {:.0} rec/s | Time: {:.1}s", 
               self.total_processed, 
               rate,
               elapsed);
        io::stdout().flush().ok();
        
        self.last_report_time = Instant::now();
    }
    
    /// Finaliza el progreso sin mensaje personalizado
    pub fn finish(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 {
            self.total_processed as f64 / elapsed
        } else {
            0.0
        };
        
        println!("\n✅ Complete: {} records in {:.1}s ({:.0} rec/s)", 
                 self.total_processed,
                 elapsed,
                 rate);
    }
    
    pub fn total(&self) -> u64 {
        self.total_processed
    }
}
