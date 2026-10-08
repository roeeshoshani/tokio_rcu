//! a collection of all pieces of data that are read-mostly in the quiescent state path, collected into a single cacheline aligned chunk of memory.

use crossbeam_utils::CachePadded;

use crate::{
    loom::{fn_const_if_not_loom, static_or_loom_lazy_static},
    rcu_core::{
        epoch::{EPOCH_ID_MIN, EpochId},
        notify::Notify,
    },
    utils::atomic_type::Atomic,
};

/// a collection of data that is read-mostly in the quiescent state logic.
///
/// when no rcu grace period operations are performed, which is expected to be most of the program's execution time, all data inside of this struct
/// is read-only, and never written.
///
/// we collect all such pieces of data into a struct so that we can put them all in a single cacheline aligned chunk of memory, so that in the
/// fast path where no rcu grace period is performed, the quiescent state logic is mostly read only with regard to shared data, and there is no
/// false sharing that is slowing it down.
struct QsReadMostlyData {
    /// a notification which is notified when threads update their last seen epoch id or change their status in any other meaningful
    /// way (e.g. become non-busy). used by waiters to wait for notifications in a blocking manner while waiting for threads to see
    /// their new epoch id, instead of constantly busy polling all threads.
    thread_epoch_updated_notify: Notify,

    /// the current global epoch id.
    /// its value must always be a valid epoch id value.
    ///
    /// used to synchronize threads that are waiting for a grace period with all other threads, by making all threads constantly load this
    /// value and publish their last seen epoch id.
    /// a waiter can then increment it and wait until all threads see his increment in their last seen epoch id value.
    cur_epoch_id: Atomic<EpochId>,
}
impl QsReadMostlyData {
    fn_const_if_not_loom! {
        const fn new() -> Self {
            Self {
                thread_epoch_updated_notify: Notify::new(),

                // we start with MIN+2 instead of just MIN since MIN is used as a "reset value", used during an epoch id reset operation to
                // track which threads saw the reset value. we don't want the initial value to look like a reset value, since that could cause
                // a stale epoch id value sampled at the start of the program to look like a reset value.
                cur_epoch_id: Atomic::<EpochId>::new(EPOCH_ID_MIN + 2),
            }
        }
    }
}

/// accessor to the field inside the qs read mostly data object.
#[inline(always)]
pub fn thread_epoch_updated_notify() -> &'static Notify {
    &QS_READ_MOSTLY_DATA.thread_epoch_updated_notify
}
/// accessor to the field inside the qs read mostly data object.
#[inline(always)]
pub fn cur_epoch_id() -> &'static Atomic<EpochId> {
    &QS_READ_MOSTLY_DATA.cur_epoch_id
}

static_or_loom_lazy_static! {
    /// a cacheline aligned and padded instance of all quiescent state read mostly data.
    /// this ensures that quiescent states are mostly read only with regard to shared state in the fast path where no concurrent rcu grace periods
    /// are performed, greatly improving the efficiency of the quiescent state logic.
    static QS_READ_MOSTLY_DATA: CachePadded<QsReadMostlyData> = CachePadded::new(QsReadMostlyData::new());
}
