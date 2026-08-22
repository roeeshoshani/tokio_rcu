#[cfg(loom)]
mod loom {
    pub use loom::cell;
    pub use loom::sync;
}
#[cfg(loom)]
pub use loom::*;

#[cfg(not(loom))]
mod std {
    pub use std::cell;
    pub use std::sync;
}
#[cfg(not(loom))]
pub use std::*;
