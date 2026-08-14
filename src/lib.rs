use std::{
    cell::Cell,
    ops::Deref,
    sync::atomic::{self, AtomicPtr},
};

use tokio::sync::Notify;

use crate::{
    epoch::{EPOCH_ID_MIN, EpochId, epoch_id_get, epoch_id_inc, epoch_id_set},
    fair_gme_lock::FairGmeLock,
    per_thread_storage::{
        ThreadStorageSlotId, ThreadStorageSlotValue, thread_storage_slot_alloc,
        thread_storage_slot_free, thread_storage_slot_get, thread_storage_slot_get_all,
    },
    thread_state::ThreadState,
    utils::PhantomUnsendUnsync,
};

mod atomic_type;
mod epoch;
mod fair_gme_lock;
mod per_thread_storage;
mod thread_state;
mod utils;

static THREAD_EPOCH_UPDATED_NOTIFY: Notify = Notify::const_new();

/// group mutual exclusion between waiters and newly starting threads.
/// group a is waiters, group b is starting threads.
static WAITERS_VS_THREAD_START_GME: FairGmeLock = FairGmeLock::new();

static EPOCH_ID_RESET_SYNC_LOCK: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

static RESET_FINISHED_NOTIFICATION: Notify = Notify::const_new();

async fn synchronize_rcu() {
    let mut reset_sync_read_guard = EPOCH_ID_RESET_SYNC_LOCK.read().await;

    let new_epoch_id = match epoch_id_inc() {
        Ok(new_epoch_id) => {
            // no overflow
            new_epoch_id
        }
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

                // wait for all active threads to go through a quiescent state and reset their last seen epoch id.
                //
                // any new threads that will start after this finishes will also start with the reset value, at least until they see
                // our increment below.
                wait_for_all_active_threads(|last_seen_epoch_id| {
                    last_seen_epoch_id == EPOCH_ID_MIN
                })
                .await;

                // at this point, all threads have reset their last seen epoch id.

                // now that we finished resetting the epoch id, we can now let new waiters in.
                drop(reset_sync_write_guard);

                // wake all current waiters.
                RESET_FINISHED_NOTIFICATION.notify_waiters();
            } else {
                // start listening to reset notification from the leader.
                //
                // this must be done before dropping the read lock, so that the leader doesn't start acting before we are listening
                // to notifications from him.
                let event = RESET_FINISHED_NOTIFICATION.notified();

                drop(reset_sync_read_guard);

                // wait for the leader to finish waiting for all threads.
                event.await;
            }

            // done resetting epoch id

            // re-lock the reset sync guard just in case, even though we shouldn't expect another reset any time soon.
            // note that the lock should be unlocked now since the writer unlocks it before waking us up.
            reset_sync_read_guard = EPOCH_ID_RESET_SYNC_LOCK.try_read().unwrap_or_else(|_| {
                panic!("another epoch id reset right after the previous reset")
            });

            // check the result later, after we unlock the lock, to avoid posioning.
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

    wait_for_all_active_threads(|last_seen_epoch_id| last_seen_epoch_id >= new_epoch_id).await;

    // the reset sync read guard must be held until we finish our entire flow, otherwise someone may reset the epoch id while we
    // are waiting for threads to see our increment, thus causing us to wait forever.
    //
    // this line makes sure we get an error if the guard is dropped earlier.
    drop(reset_sync_read_guard);
}

/// wait until the last seen epoch id of all threads matches the given predicate.
async fn wait_for_all_active_threads<F: Fn(EpochId) -> bool>(last_seen_epoch_id_predicate: F) {
    loop {
        // block new threads from starting while we check that we are synchronized with all existing threads.
        //
        // if we find that all current threads are updated with the new epoch id, once we release the lock, any new thread which will
        // grab the lock is guaranteed to also see the new pointer, so it can't access the old one.
        //
        // if some of the threads are not yet updated, we release the lock, letting the new threads start, and in the next iteration we
        // will take those new threads into account.
        let thread_start_blocker = WAITERS_VS_THREAD_START_GME.lock_group_a();

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
        //
        // TODO: what if the epoch id overflows? we may get stuck in an infinite loop.
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
                // if this thread is not currently running any future, so it can't be using any stale rcu
                // pointer, since such pointers can't be held across await points.
                return true;
            }

            last_seen_epoch_id_predicate(state.last_seen_epoch_id)
        }) {
            // all threads saw our new epoch id, we are done waiting
            break;
        }

        // no longer need to block starting threads
        drop(thread_start_blocker);

        // some of the threads haven't yet seen our new epoch id.
        // so, wait for them to go through a quiescent state and see our new epoch id.
        notified.await;
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct RcuReadGuard<'a, T> {
    value: &'a T,
    _phantom: PhantomUnsendUnsync,
}
impl<'a, T> Deref for RcuReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

pub struct Rcu<T> {
    value_ptr: AtomicPtr<T>,
}
impl<T> Rcu<T> {
    pub fn new(value: T) -> Self {
        Self {
            value_ptr: AtomicPtr::new(Box::leak(Box::new(value))),
        }
    }

    pub fn read(&self) -> RcuReadGuard<'_, T> {
        let ptr = self.value_ptr.load(
            // we want acquire ordering to make sure that the write to the pointed-at data happens before the
            // write of the pointer itself, so that when we use the loaded pointer, we are guaranteed to get
            // a valid object.
            atomic::Ordering::Acquire,
        );

        RcuReadGuard {
            // SAFETY: pointers are always valid by the invariants of this type.
            value: unsafe { &*ptr },
            _phantom: PhantomUnsendUnsync::new(),
        }
    }

    pub async fn swap(&self, new_value: T) -> T {
        let new_value_ptr = Box::leak(Box::new(new_value));

        let old_value_ptr = self.value_ptr.swap(
            new_value_ptr,
            // for the store part, we want release ordering since we want to make sure that the write of
            // the pointed-at data to memory happen before the store of the pointer itself for everyone
            // who loads this with acquire ordering.
            //
            // for the load part, we want acquire ordering to make sure that the write to the pointed-at
            // data happens before the write of the pointer itself, so that when we use the loaded pointer,
            // we are guaranteed to get a valid object. this is important since we actually use the old
            // pointer to get back the old value.
            atomic::Ordering::AcqRel,
        );

        // SAFETY: pointers are always valid by the invariants of this type.
        let boxed_value = unsafe { Box::from_raw(old_value_ptr) };

        // wait for all previous readers to stop using the old value
        synchronize_rcu().await;

        *boxed_value
    }
}
impl<T> Drop for Rcu<T> {
    fn drop(&mut self) {
        let ptr = self.value_ptr.load(
            // we want acquire ordering to make sure that the write to the pointed-at data happens before the
            // write of the pointer itself, so that when we use the loaded pointer, we are guaranteed to get
            // a valid object.
            atomic::Ordering::Acquire,
        );

        // SAFETY: pointers are always valid by the invariants of this type.
        let _ = unsafe { Box::from_raw(ptr) };
    }
}

thread_local! {
    static THREAD_STORAGE_SLOT: Cell<Option<ThreadStorageSlotId>> = Cell::new(None);
}

pub fn on_thread_start() {
    // we want mutual exclusion with waiters while we allocate our new slot.
    // this is important for the waiter logic, not to our logic here.
    let waiters_blocker = WAITERS_VS_THREAD_START_GME.lock_group_b();

    // note that we must not unwrap here while the gme lock is locked since it does not support poisioning, and panicking while holding
    // it will make all waiters spin forever.
    let storage_slot = thread_storage_slot_alloc(ThreadState {
        // we don't need any real epoch id value here.
        // the epoch id values are only relevant when a thread is busy.
        //
        // also, when a thread starts, it may start using some rcu pointers and suddenly become relevant to
        // the rcu synchronization, but we don't need to worry about it in this callback, since the thread
        // will first call the "before poll" callback before it can use any rcu pointer, since rcu pointers can
        // only be used inside futures.
        last_seen_epoch_id: EPOCH_ID_MIN,
        is_busy: false,
    });

    // finished allocating a slot, the waiters can now see us, so we no longer need to block them.
    drop(waiters_blocker);

    let storage_slot = storage_slot
        .expect("too many concurrent threads, failed to allocate a storage slot for a new thread");

    THREAD_STORAGE_SLOT.set(Some(storage_slot));
}

pub fn on_thread_stop() {
    let storage_slot_id = THREAD_STORAGE_SLOT.get().unwrap();
    thread_storage_slot_free(storage_slot_id);
    THREAD_STORAGE_SLOT.set(None);
}

fn thread_fetch_new_epoch_id_and_update_waiters(
    storage_slot: &ThreadStorageSlotValue,
    is_busy: bool,
) {
    let new_seen_epoch_id = epoch_id_get(
        // we use acquire ordering coupled with a release ordering when incrementing the epoch id to make sure that we see swap of the rcu
        // protected pointer before we see the increment of the epoch id.
        //
        // if we were to first see the increment of the epoch id, and only then see the swap of the pointer, we may publish that we have
        // seen the new epoch id, causing the waiter to free the memory, and then still use the old and now freed pointer since we haven't
        // yet seen the pointer swap.
        //
        // furthermore, this guarantees that the store of the new thread state containing the new seen epoch id happens after the
        // increment of the epoch id in the eyes of all waiters, although this ordering is not really needed to guarantee the correctness
        // of the algorithm.
        atomic::Ordering::Acquire,
    );

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
            is_busy,
        }
        .encode(),
        // we use store ordering since we want to make sure that all writes to the data pointed at by the rcu protected pointer happen
        // before this store so that no writes happen after the data is freed.
        atomic::Ordering::Release,
    );

    // TODO: can we use `unwrap_unchecked` here for better performance? how do we mark the unsafety?
    let prev_state = ThreadState::decode(prev_state_encoded).unwrap();

    if prev_state.last_seen_epoch_id != new_seen_epoch_id {
        // if the last seen epoch id changed, some waiter may now be able to finish waiting. so, notify all waiters.
        THREAD_EPOCH_UPDATED_NOTIFY.notify_waiters();
    }
}

pub fn on_before_task_poll() {
    // TODO: can we use `unwrap_unchecked` here for better performance? how do we mark the unsafety?
    let storage_slot_id = THREAD_STORAGE_SLOT.get().unwrap();
    let storage_slot = thread_storage_slot_get(storage_slot_id);

    thread_fetch_new_epoch_id_and_update_waiters(storage_slot, true);
}

pub fn on_after_task_poll() {
    // TODO: can we use `unwrap_unchecked` here for better performance? how do we mark the unsafety?
    let storage_slot_id = THREAD_STORAGE_SLOT.get().unwrap();
    let storage_slot = thread_storage_slot_get(storage_slot_id);

    // theoretically, we could just unset the busy flag and that's it. we don't really have to fetch
    // a new epoch id here.
    //
    // the reason we do is in order to know when we need to actually notify any waiters.
    //
    // if we were to only unset the busy flag here, we would have needed to wake the waiters every single time,
    // since some waiter may be waiting for us to finish using the rcu pointer, and if we will never poll any
    // future again, we must notify that waiter here in this callback.
    //
    // in order to avoid wasteful wakeups though, we only wake the waiters up if we see a new epoch id.
    //
    // this saves redundant wakeups in the case where some waiter is waiting for all threads, but our thread
    // has already seen the waiter's new epoch id and woke the waiter up, but when the waiter woke up he saw
    // that other threads may still be using the rcu pointer.
    // in that case, waking the waiter due to updates from our thread is no longer relevant, he is only waiting
    // for other threads.
    //
    // TODO: this introduces more read-side contention on the epoch id, does it really improve performance?
    // it may slow waiters down by slowing their increment of the epoch id due to the cache-line being contended.
    thread_fetch_new_epoch_id_and_update_waiters(storage_slot, false);
}
