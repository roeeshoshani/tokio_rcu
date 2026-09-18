//! this module contains test utilities that are shared between the unit tests and the integration tests.
//! it is intended for internal use only. if you are an external user of this crate, don't use this.

use std::any::Any;

/// extracts the panic message string given an error representing a panic, returned from any stdlib function which is capable of catching
/// panics (e.g. [`catch_unwind`](std::panic::catch_unwind)).
pub fn extract_string_panic_message(err: Box<dyn Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        panic!("failed to downcast panic message payload")
    }
}
