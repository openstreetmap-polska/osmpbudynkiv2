use std::sync::atomic::{AtomicBool, Ordering};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Register a handler for SIGINT and SIGTERM.
/// First signal sets the flag; second signal force-exits.
pub fn install_handler() {
    ctrlc::set_handler(move || {
        if SHUTDOWN_REQUESTED.swap(true, Ordering::SeqCst) {
            eprintln!("\nForce shutdown");
            std::process::exit(1);
        }
        eprintln!("\nShutdown requested, finishing current operation...");
    })
    .expect("Failed to install signal handler");
}

pub fn is_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}
