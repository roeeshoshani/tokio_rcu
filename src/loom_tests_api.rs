//! in loom mode, tokio doesn't provide the necessary runtime hooks (e.g. `on_after_task_poll`), so we can't use the `enable_rcu`
//! function for testing. instead, we expose the internal hook functions so that they can be tested independently of the tokio runtime.
//! do not use this unless you know what you are doing.

pub use crate::{
    core::epoch::{EPOCH_ID_MAX, EPOCH_ID_MIN, EpochId},
    loom::std::sync::atomic,
};

pub fn on_before_task_poll() {
    crate::core::on_before_task_poll();
}
pub fn on_thread_stop() {
    crate::core::on_thread_stop()
}
pub fn on_thread_park() {
    crate::core::on_thread_park()
}
pub fn on_thread_unpark() {
    crate::core::on_thread_unpark()
}
pub fn on_after_task_poll() {
    crate::core::on_after_task_poll()
}
pub fn epoch_id_set(new_value: EpochId, ordering: atomic::Ordering) {
    crate::core::epoch::epoch_id_set(new_value, ordering);
}
pub fn epoch_id_get(ordering: atomic::Ordering) -> EpochId {
    crate::core::epoch::epoch_id_get(ordering)
}
