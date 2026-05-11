//! Shared tracing-subscriber telemetry for all server provider backends.
//!
//! Both the mock and cuobj vtables point at these functions.  The subscriber
//! is installed once (process-global); subsequent `setup` calls from a second
//! provider are no-ops on the subscriber but still update the level handle if
//! it was stored by the first successful call.

use std::ffi::c_void;

use s3_rdma::ffi::CTelemetryConfig;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::{filter::LevelFilter, fmt, layer::SubscriberExt, reload, util::SubscriberInitExt};

type LevelReloadFn = Box<dyn Fn(LevelFilter) + Send + Sync + 'static>;
static LEVEL_RELOAD: std::sync::Mutex<Option<LevelReloadFn>> = std::sync::Mutex::new(None);

fn level_to_lf(level: u32) -> LevelFilter {
    match level {
        0 => LevelFilter::OFF,
        1 => LevelFilter::ERROR,
        2 => LevelFilter::INFO,
        _ => LevelFilter::DEBUG,
    }
}

pub(crate) unsafe extern "C" fn supports(_: *mut c_void) -> i32 { 1 }

pub(crate) unsafe extern "C" fn setup(_: *mut c_void, cfg: *const CTelemetryConfig) -> i32 {
    if cfg.is_null() { return -1; }
    let c = unsafe { &*cfg };
    let log_file = if c.log_file.is_null() {
        None
    } else {
        match unsafe { std::ffi::CStr::from_ptr(c.log_file) }.to_str() {
            Ok(s) => Some(s.to_owned()),
            Err(_) => return -1,
        }
    };

    let writer = match log_file.as_deref() {
        Some(path) => match std::fs::File::create(path) {
            Ok(f)  => BoxMakeWriter::new(std::sync::Mutex::new(f)),
            Err(e) => { tracing::error!("setup_telemetry: {path}: {e}"); return -1; }
        },
        None => BoxMakeWriter::new(|| std::io::stderr()),
    };
    let (filter, handle) = reload::Layer::new(level_to_lf(c.level));
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(writer));
    if registry.try_init().is_ok() {
        let reload_fn: LevelReloadFn = Box::new(move |lf| { let _ = handle.modify(|f| *f = lf); });
        *LEVEL_RELOAD.lock().unwrap() = Some(reload_fn);
    }
    0
}

pub(crate) unsafe extern "C" fn shutdown(_: *mut c_void) -> i32 {
    *LEVEL_RELOAD.lock().unwrap() = None;
    0
}

pub(crate) unsafe extern "C" fn set_level(_: *mut c_void, level: u32) -> i32 {
    if let Some(f) = LEVEL_RELOAD.lock().unwrap().as_ref() {
        f(level_to_lf(level));
    }
    0
}
