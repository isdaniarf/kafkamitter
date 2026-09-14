use std::sync::OnceLock;
use std::time::Instant;

static START: OnceLock<Instant> = OnceLock::new();
static ENABLED: OnceLock<bool> = OnceLock::new();

pub fn mark_start() {
    START.get_or_init(Instant::now);
    ENABLED.get_or_init(|| std::env::var_os("KAFKAMITTER_TRACE_STARTUP").is_some());
}

pub fn trace(label: &str) {
    if !ENABLED.get().copied().unwrap_or(false) {
        return;
    }
    if let Some(start) = START.get() {
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        eprintln!("[startup] {elapsed_ms:>8.1} ms  {label}");
    }
}
