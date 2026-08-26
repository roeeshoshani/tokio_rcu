use std::{cell::Cell, sync::atomic};

use crate::{
    epoch::{EPOCH_ID_MIN, EpochId, epoch_id_get, epoch_id_inc, epoch_id_set},
    notify::Notify,
    per_thread_storage::{
        ThreadStorageSlotId, ThreadStorageSlotValue, thread_storage_slot_alloc,
        thread_storage_slot_free, thread_storage_slot_get, thread_storage_slot_get_all,
    },
    thread_state::ThreadState,
};

mod atomic_type;
mod epoch;
mod membarrier;
mod notify;
mod per_thread_storage;
mod rcu;
mod thread_state;
mod utils;

pub use rcu::{Rcu, RcuReadGuard};

/// a notification which is notified when threads update their last seen epoch id or change their status in any other meaningful
/// way (e.g. become non-busy). used by waiters to wait for notifications in a blocking manner while waiting for threads to see
/// their new epoch id, instead of constantly busy polling all threads.
static THREAD_EPOCH_UPDATED_NOTIFY: Notify = Notify::new();

/// a lock used to synchronize the reset operation.
/// a reset operation is performed when the epoch id overflows, in order to reset the epoch id back to its minimum value.
///
/// when some thread increments the epoch id and causes it to exceed its max threshold, this thread begins a reset operation.
/// for resetting the epoch id, the thread must reset the global epoch id back to its initial value, then wait for all threads to
/// see this new state while blocking any further increments of the epoch id until all threads see the reset value.
///
/// in order to prevent the further increments of the epoch id during the reset operation, this lock is used.
/// all incrementors of the epoch id lock it for reading before incrementing, and during the reset operation, the leader of the reset (the
/// first one to increment the epoch id past its max threshold) locks this lock for writing, thus preventing any new incrementors from
/// incrementing the epoch id.
///
/// this also ensures that we don't start performing a reset operation while some incrementor thread is still waiting for threads to see
/// his incremented epoch id. if we were to start the reset while we was waiting, we would get stuck until the next overflow of the epoch
/// id.
static EPOCH_ID_RESET_SYNC_LOCK: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

/// when a thread increments the epoch id past its max threshold, this thread begins a reset operation.
/// while that thread was incrementing the epoch id, another thread may have also been incrementing the epoch id, and also saw that it
/// reached its max threshold. so, that thread also begins the reset operation.
///
/// in practice, the reset is actually only performed by a single thread - the leader, and all other threads that entered reset just wait
/// for him to finish resetting.
///
/// so, this notification used by the leader of a reset operation to notify all other threads that have also entered reset that the reset
/// operation is done.
static RESET_FINISHED_NOTIFICATION: Notify = Notify::new();

/// wait for an RCU grace period.
// TODO: make this public and describe the memory ordering guarantees in more detail.
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

    // lock the reset sync lock for reading.
    //
    // this ensures that if any reset operation is currently ongoing, we don't interrupt it by incremented the epoch id while it
    // is being reset, and we instead wait for it to finish and only then go on with our increment.
    //
    // this exclusivity is guaranteed since during reset the leader of the reset locks the reset sync lock for writing.
    //
    // this also ensures that a reset operation is not initiated while we are still waiting for threads to see our incremented epoch id,
    // since we hold this until we finish waiting.
    let mut reset_sync_read_guard = EPOCH_ID_RESET_SYNC_LOCK.read().await;

    // increment the epoch id.
    //
    // this is used as a communication primitive with the worker threads.
    // worker threads will then update their last seen epoch id by reading the global epoch id every time they pass through a quiescent
    // state.
    //
    // we can then sample their published last seen epoch id to know when they saw our increment, and once they did, we know that they
    // passed through a quiescent state.
    let new_epoch_id = match epoch_id_inc() {
        Ok(v) => v,
        Err(err) => {
            // epoch id overflow.

            // perform a reset of the epoch id
            if err.am_i_the_leader {
                // re-lock the reset sync lock for writing.
                //
                // once we succeed grabbing the write lock, it is guaranteed that:
                // - all previous waiters finished waiting for their grace period
                // - all non-leader waiters that also entered reset mode have started listening to the reset
                // - all new waiters will be blocked until we finish.
                drop(reset_sync_read_guard);
                let reset_sync_write_guard = EPOCH_ID_RESET_SYNC_LOCK.write().await;

                // reset the epoch id
                epoch_id_set(EPOCH_ID_MIN, atomic::Ordering::Relaxed);

                // make sure that all threads see the reset of the epoch id.
                membarrier::perform();

                // wait for all threads to update their last seen epoch id to the reset value.
                //
                // note that parked and not yet started threads are not relevant here, since once they wake up they will see the updated
                // reset value of the epoch id due to the membarrier, and they will fetch and publish it along with the enabling of the
                // busy flag as soon as they unpark.
                wait_for_running_threads_to_see_epoch_id(|last_seen_epoch_id| {
                    last_seen_epoch_id == EPOCH_ID_MIN
                })
                .await;

                // at this point, all running threads have reset their last seen epoch id, and new threads are guaranteed
                // to see at least the reset value.

                // now that we finished resetting the epoch id, we can now let new waiters in.
                drop(reset_sync_write_guard);

                // wake all non-leader waiters that are in reset mode waiting for us to finish.
                RESET_FINISHED_NOTIFICATION.notify();
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

    // ensure that the reset sync read guard is held up until this point.
    // this is important to make sure that a reset operation is not initiated while we are still waiting for threads to see our new
    // epoch id, otherwise we would keep waiting until the next overflow of the epoch id.
    drop(reset_sync_read_guard);
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
    /// a thread local variable which stores the slot id of the thread storage slot that was allocated for the current thread.
    ///
    /// initially, this is `None`.
    ///
    /// when a thread starts running, it allocates a storage slot for itself, and then saves the id of the allocated slot in this variable.
    /// when the thread is running, it then uses this variable for determining which slot to use for book-keeping.
    /// when the thread finishes executing, it then finally deallocates its storage slot and sets this back to `None`.
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
    THREAD_EPOCH_UPDATED_NOTIFY.notify();
}

fn on_thread_unpark() {
    let storage_slot = this_thread_get_storage_slot();

    // note that in addition to setting the is busy flag here, we also need to see a new epoch id.
    //
    // this is needed for the case where a reset operation was performed since we last went to sleep.
    // in that case, if we wake up and set the busy flag without updating the epoch id, some thread that had already incremented
    // the epoch id since the reset may think that we saw his epoch id increment since we have a stale high epoch id value, even
    // though in practice we didn't really see his epoch id increment.
    let new_seen_epoch_id = this_thread_see_new_epoch_id();
    storage_slot.state.store(
        ThreadState {
            last_seen_epoch_id: new_seen_epoch_id,
            is_busy: true,
        }
        .encode(),
        // we use release ordering to make sure that all writes to the data pointed at by the rcu protected pointer happen before this
        // store so that no writes happen after the data is freed.
        // this is needed since we actually fetch a new epoch id here, not only set the busy flag.
        atomic::Ordering::Release,
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
        // we use release ordering to make sure that all writes to the data pointed at by the rcu protected pointer happen before this
        // store so that no writes happen after the data is freed.
        atomic::Ordering::Release,
    );

    let prev_state = ThreadState::decode(prev_state_encoded).unwrap();

    // we are expected to be in the busy state while not parked
    debug_assert!(prev_state.is_busy);

    if prev_state.last_seen_epoch_id != new_seen_epoch_id {
        // if the last seen epoch id changed, some waiter may now be able to finish waiting. so, notify all waiters.
        THREAD_EPOCH_UPDATED_NOTIFY.notify();
    }
}

pub trait TokioRuntimeBuilderExt {
    /// enable rcu support for this tokio runtime.
    /// must be called when constructing the runtime in order to use any rcu related primitive inside the runtime.
    fn enable_rcu(&mut self) -> &mut Self;
}

impl TokioRuntimeBuilderExt for tokio::runtime::Builder {
    fn enable_rcu(&mut self) -> &mut Self {
        assert!(membarrier::is_supported());
        membarrier::register();

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
