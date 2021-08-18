//! Timestamp utilities that run on both native and wasm targets.
use std::cmp::max;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
use std::time;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = Date)]
    fn now() -> f64;
}

/// Returns the current time in milliseconds since the Unix epoch.
#[cfg(target_arch = "wasm32")]
pub fn timestamp_now() -> u64 {
    now() as u64
}

/// Returns the current time in milliseconds since the Unix epoch.
#[cfg(not(target_arch = "wasm32"))]
pub fn timestamp_now() -> u64 {
    let now = time::SystemTime::now();
    now.duration_since(time::UNIX_EPOCH)
        .expect("could not get system time for store snapshot")
        .as_millis() as u64
}

/// Returns a monotonically increasing timestamp that is the current time (in
/// milliseconds since the Unix epoch) if current time > most recent timestamp,
/// otherwise simply the most recent timestamp. (This fn is used to ensure that
/// a clock change cannot lead to a timestamp going "back in time".)
pub fn timestamp_now_monotonic(most_recent_timestamp: u64) -> u64 {
    max(most_recent_timestamp, timestamp_now())
}
