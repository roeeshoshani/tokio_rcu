use loom_or_std::{cell::Cell, sync::atomic};

use tokio::sync::Notify;

use crate::{
    epoch::{EPOCH_ID_MIN, EpochId, epoch_id_get, epoch_id_inc, epoch_id_set},
    per_thread_storage::{
        ThreadStorageSlotId, ThreadStorageSlotValue, thread_storage_slot_alloc,
        thread_storage_slot_free, thread_storage_slot_get, thread_storage_slot_get_all,
    },
    thread_state::ThreadState,
};

mod atomic_type;
mod epoch;
mod loom_or_std;
mod membarrier;
mod per_thread_storage;
mod rcu;
mod thread_state;
mod utils;

pub use rcu::{Rcu, RcuReadGuard};

/// a notification which is notified when threads update their last seen epoch id or change their status in any other meaningful
/// way (e.g. become busy). used by waiters to wait for notifications in a blocking manner while waiting for threads to see
/// their new epoch id.
static THREAD_EPOCH_UPDATED_NOTIFY: Notify = Notify::const_new();

static EPOCH_ID_RESET_SYNC_LOCK: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

static RESET_FINISHED_NOTIFICATION: Notify = Notify::const_new();

/// wait for an RCU grace period.
async fn synchronize_rcu() {
    // perform a membarrier to make sure that all other threads see the new rcu pointer.
    membarrier::perform();

    // after the membarrier, all threads are guaranteed to have seen our new pointer.
    // we only need to wait for any potential existing users of the old pointer to finish using it.
    //
    // note that due to the membarrier, we don't need to worry about just-starting threads or just-unparking threads which
    // may access the old pointer.
    //
    // if during the check below, we see that some thread is currently parked, or we don't see the slot of some just-started
    // thread, then it means that this thread's update of its own state happens strictly after the membarrier, and the state
    // update always happens before polling the future, so there's no way for the polled future to see the old pointer.
    // the relationship is:
    // pointer swap -> membarrier -> thread's update of his own state -> thread's load of the rcu protected pointer
    // thus, all such threads are guaranteed to see the new pointer, and we can thus ignore them when waiting for all existing
    // users.

    let mut reset_sync_read_guard = EPOCH_ID_RESET_SYNC_LOCK.read().await;

    let new_epoch_id = match epoch_id_inc() {
        Ok(v) => v,
        Err(err) => {
            // epoch id overflow.

            // perform a reset of the epoch id
            if err.am_i_the_leader() {
                // re-lock the reset sync lock for writing.
                //
                // once we succeed grabbing the write lock, it is guaranteed that all current waiters have started listening to the reset
                // finished notification, and all new waiters will be blocked until we finish.
                drop(reset_sync_read_guard);
                let reset_sync_write_guard = EPOCH_ID_RESET_SYNC_LOCK.write().await;

                // reset the epoch id
                epoch_id_set(EPOCH_ID_MIN, atomic::Ordering::Relaxed);

                // make sure that all threads see the reset of the epoch id.
                membarrier::perform();

                // wait for all threads to update their last seen epoch id to the reset value.
                //
                // note that parked and not yet started threads are not relevant here, since once they wake up they
                // will see the updated reset value of the epoch id due to the membarrier.
                wait_for_running_threads_to_see_epoch_id(|last_seen_epoch_id| {
                    last_seen_epoch_id == EPOCH_ID_MIN
                })
                .await;

                // at this point, all running threads have reset their last seen epoch id, and new threads are guaranteed
                // to see at least the reset value.

                // now that we finished resetting the epoch id, we can now let new waiters in.
                drop(reset_sync_write_guard);

                // wake all non-leader waiters that are in reset mode waiting for us to finish.
                RESET_FINISHED_NOTIFICATION.notify_waiters();
            } else {
                // start listening to reset notification from the leader.
                //
                // this must be done before dropping the read lock, so that the leader doesn't start acting before we are listening
                // to notifications from him.
                let event = RESET_FINISHED_NOTIFICATION.notified();

                // let the leader start doing its thing.
                drop(reset_sync_read_guard);

                // wait for the leader to finish the reset operation and notify us.
                event.await;
            }

            // done resetting epoch id

            // re-lock the reset sync guard just in case, even though we shouldn't expect another reset any time soon.
            // note that the lock should be unlocked now since the writer unlocks it before waking us up.
            reset_sync_read_guard = EPOCH_ID_RESET_SYNC_LOCK.try_read().unwrap_or_else(|_| {
                panic!("another epoch id reset right after the previous reset")
            });

            let Ok(new_epoch_id) = epoch_id_inc() else {
                // avoid poisoning the lock
                drop(reset_sync_read_guard);

                // we should never get another overflow right after we finish resetting.
                // the epoch id should take some time to grow before it wraps around again.
                panic!("overflow when incrementing epoch id after reset")
            };

            new_epoch_id
        }
    };

    // note that parked and not-yet-started threads are irrelevant here since they are guaranteed to see the new pointer
    // due to the membarrier.
    wait_for_running_threads_to_see_epoch_id(|last_seen_epoch_id| {
        last_seen_epoch_id >= new_epoch_id
    })
    .await;
}

/// wait for all threads to see some epoch id as implemented in the given predicate which processes the last seen epoch
/// id of each thread.
///
/// this function does not take into account new threads just starting, nor new threads just existing the busy state.
async fn wait_for_running_threads_to_see_epoch_id<F: Fn(EpochId) -> bool>(
    last_seen_epoch_id_predicate: F,
) {
    loop {
        // start subscribing to the notified waiters event before checking the current state.
        //
        // if we first check the state and only then start listening, there may be a small window after we finish
        // checking the values but before we start listening where some thread updates its counter and notifies
        // all wakers, but we will miss that notification, which is problematic.
        //
        // so, we start listening before checking the values, so that even notifications that are issued while
        // or right after we finished checking are still received.
        //
        // TODO: should i implement my own optimized primitive for this instead of tokio's `Notify` for better
        // peformance?
        let notified = THREAD_EPOCH_UPDATED_NOTIFY.notified();

        // check if all threads have seen our new epoch id
        if thread_storage_slot_get_all().iter().all(|storage_slot| {
            let encoded_state = storage_slot.state.load(
                // we use acquire ordering paired with a release ordering for the store to make sure that the stores to the data
                // pointed at by the rcu protected pointer happen before we see the store to the state.
                // this is important in order to guarantee that we don't see those writes after we free the protected pointer, which will
                // lead to a UAF.
                atomic::Ordering::Acquire,
            );

            let Some(state) = ThreadState::decode(encoded_state) else {
                // if the slot is empty, ignore it
                return true;
            };

            if !state.is_busy {
                // this thread is currently not busy running any future, so it is not relevant.
                // as previously explained, even if it start running right after we check this, it is guaranteed to see
                // the new pointer and is thus not relevant to us.
                return true;
            }

            last_seen_epoch_id_predicate(state.last_seen_epoch_id)
        }) {
            // all threads saw our new epoch id, we are done waiting
            break;
        }

        // some of the threads haven't yet seen our new epoch id.
        // so, wait for them to go through a quiescent state and see our new epoch id.
        notified.await;
    }
}

thread_local! {
    static THREAD_STORAGE_SLOT: Cell<Option<ThreadStorageSlotId>> = Cell::new(None);
}

/// returns the storage slot of the current thread, assuming that a storage slot was already allocated for the current
/// thread.
fn this_thread_get_storage_slot() -> &'static ThreadStorageSlotValue {
    let storage_slot_id = THREAD_STORAGE_SLOT.get().unwrap();
    thread_storage_slot_get(storage_slot_id)
}

/// "see" a new epoch id in the current thread.
/// this fetches the current epoch id with a proper memory ordering - a release memory ordering, which provides the required
/// guaranteed, for example it guarantees that once we see an updated epoch id, we see the swap of the rcu protected pointer
/// as happened before that store to the epoch id.
fn this_thread_see_new_epoch_id() -> EpochId {
    epoch_id_get(
        // we use acquire ordering coupled with a release ordering when incrementing the epoch id to make sure that we see swap of the rcu
        // protected pointer before we see the increment of the epoch id.
        //
        // if we were to first see the increment of the epoch id, and only then see the swap of the pointer, we may publish that we have
        // seen the new epoch id, causing the waiter to free the memory, and then still use the old and now freed pointer since we haven't
        // yet seen the pointer swap.
        atomic::Ordering::Acquire,
    )
}

fn on_thread_start() {
    let epoch_id = this_thread_see_new_epoch_id();
    let storage_slot = thread_storage_slot_alloc(ThreadState {
        last_seen_epoch_id: epoch_id,
        is_busy: true,
    })
    .expect("too many concurrent threads, failed to allocate a storage slot for a new thread");

    THREAD_STORAGE_SLOT.set(Some(storage_slot));
}

fn on_thread_stop() {
    let storage_slot_id = THREAD_STORAGE_SLOT.get().unwrap();
    thread_storage_slot_free(storage_slot_id);
    THREAD_STORAGE_SLOT.set(None);
}

fn on_thread_park() {
    let storage_slot = this_thread_get_storage_slot();

    // mark this thread as non-busy.
    storage_slot.state.fetch_and(
        !1,
        // no special ordering needed here.
        // note that this relaxed store doesn't break the release-sequence of this variable (see c++ memory model for more
        // info), so it doesn't prevent the loader from synchronizing with any previous release ordered store.
        atomic::Ordering::Relaxed,
    );

    // wake all waiters since some waiters may be waiting for us to see their new epoch id, and we are instead going to sleep
    // so we will never see it.
    // wake them so that they will see that we are no longer busy and thus we are no longer using any of their rcu protected
    // pointers.
    THREAD_EPOCH_UPDATED_NOTIFY.notify_waiters();
}

fn on_thread_unpark() {
    let storage_slot = this_thread_get_storage_slot();

    // mark this thread as busy.
    storage_slot.state.fetch_or(
        1,
        // no special ordering needed here.
        // note that this relaxed store doesn't break the release-sequence of this variable (see c++ memory model for more
        // info), so it doesn't prevent the loader from synchronizing with any previous release ordered store.
        atomic::Ordering::Relaxed,
    );
}

fn on_after_task_poll() {
    let storage_slot = this_thread_get_storage_slot();
    let new_seen_epoch_id = this_thread_see_new_epoch_id();

    // at this point we want to swap the current state with the new state.
    // we could do that using the atomic `swap` operation, but we can do something more performant while still maintaining correctness.
    //
    // the slot's data is loaded from multiple threads, but it is only written to by the current thread who owns that slot.
    // we can use that fact to split the atomic `swap` operation into a `load` and then a `store`, while still being guaranteed that no
    // one will modify the value between the `load` and the `store`, since the current thread are the only one allowed to modify the
    // value.
    //
    // as for why this is more efficient, the load-then-store method requires looser memory ordering guarantees, and thus provides more
    // flexibility for optimization by the hardware's memory subsystem.
    //
    // for example, on x86, the load then store will be translated to just 2 simple `MOV` instructions, while a `swap` would have been
    // translated to a `LOCK XCHG` instruction, which requires much more effort from the hardware.
    let prev_state_encoded = storage_slot.state.load(
        // we don't need any special ordering, since this thread is the only entity which can write to this variable.
        // so, the returned value is sequentially consistent with the execution order of the code in this thread.
        //
        // also, we don't need to synchronize this load against any other shared variables, since the returned value is only used to
        // check whether it was different than the newly written value, and is thus not used in combination with any other shared state.
        atomic::Ordering::Relaxed,
    );
    storage_slot.state.store(
        ThreadState {
            last_seen_epoch_id: new_seen_epoch_id,
            is_busy: true,
        }
        .encode(),
        // we use release ordering since we want to make sure that all writes to the data pointed at by the rcu protected pointer happen
        // before this store so that no writes happen after the data is freed.
        atomic::Ordering::Release,
    );

    let prev_state = ThreadState::decode(prev_state_encoded).unwrap();

    // we are expected to be in the busy state while not parked
    debug_assert!(prev_state.is_busy);

    if prev_state.last_seen_epoch_id != new_seen_epoch_id {
        // if the last seen epoch id changed, some waiter may now be able to finish waiting. so, notify all waiters.
        THREAD_EPOCH_UPDATED_NOTIFY.notify_waiters();
    }
}

pub trait TokioRuntimeBuilderExt {
    /// enable rcu support for this tokio runtime.
    fn enable_rcu(&mut self) -> &mut Self;
}

fn membarrier_check_support_and_register() {
    assert!(membarrier::is_supported());
    membarrier::register();
}

// note that this does not work in `cfg(loom)` since tokio doesn't provide the required hooks (e.g. `on_thread_start`) when
// the loom config is enabled.
// in loom mode, for testing only, we instead expose the hook functions directly so that they can be tested independently
// of the tokio runtime.
#[cfg(not(loom))]
impl TokioRuntimeBuilderExt for tokio::runtime::Builder {
    fn enable_rcu(&mut self) -> &mut Self {
        membarrier_check_support_and_register();

        self.on_thread_start(|| {
            on_thread_start();
        })
        .on_thread_stop(|| {
            on_thread_stop();
        })
        .on_thread_park(|| {
            on_thread_park();
        })
        .on_thread_unpark(|| {
            on_thread_unpark();
        })
        .on_after_task_poll(|_| {
            on_after_task_poll();
        })
    }
}

// in loom mode, tokio doesn't provide the necessary runtime hooks (e.g. `on_thread_start`), so we can't use the `enable_rcu`
// function for testing. instead, we expose the internal hook functions so that they can be tested independently of the
// tokio runtime.
// do not use these unless you know what you are doing.
#[cfg(loom)]
pub mod loom {
    pub fn initialize() {
        crate::membarrier_check_support_and_register();
    }
    pub fn on_thread_start() {
        crate::on_thread_start()
    }
    pub fn on_thread_stop() {
        crate::on_thread_stop()
    }
    pub fn on_thread_park() {
        crate::on_thread_park()
    }
    pub fn on_thread_unpark() {
        crate::on_thread_unpark()
    }
    pub fn on_after_task_poll() {
        crate::on_after_task_poll()
    }
}
