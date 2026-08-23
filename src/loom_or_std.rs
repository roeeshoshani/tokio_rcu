#[cfg(loom)]
pub use loom::*;

#[cfg(not(loom))]
pub use std::*;
