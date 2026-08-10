use std::{
    cell::Cell,
    ops::Deref,
    sync::atomic::{self, AtomicPtr},
};

use tokio::sync::Notify;

use crate::{
    epoch::{MIN_EPOCH_ID, epoch_id_get_cur, epoch_id_inc},
    per_thread_storage::{
        ThreadStorageSlotId, ThreadStorageSlotValue, thread_storage_slot_alloc,
        thread_storage_slot_free, thread_storage_slot_get, thread_storage_slot_get_all,
    },
    thread_state::ThreadState,
    utils::PhantomUnsendUnsync,
};

mod atomic_type;
mod epoch;
mod per_thread_storage;
mod thread_state;
mod utils;

static THREAD_EPOCH_UPDATED_NOTIFY: Notify = Notify::const_new();

async fn synchronize_rcu() {
    let new_epoch_id = epoch_id_inc();

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

            // if this thread is currently running some future, check if it has seen our epoch id.
            // if it did, it can't be using any stale rcu pointer.
            state.last_seen_epoch_id >= new_epoch_id
        }) {
            // all threads saw our new epoch id, we are done waiting
            break;
        }

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

        // wait for all previous readers to stop using the old value
        synchronize_rcu().await;

        // SAFETY: pointers are always valid by the invariants of this type.
        let boxed_value = unsafe { Box::from_raw(old_value_ptr) };

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
    let storage_slot = thread_storage_slot_alloc(ThreadState {
        // we don't need any real epoch id value here.
        // the epoch id values are only relevant when a thread is busy.
        //
        // also, when a thread starts, it may start using some rcu pointers and suddenly become relevant to
        // the rcu synchronization, but we don't need to worry about it in this callback, since the thread
        // will first call the "before poll" callback before it can use any rcu pointer, since rcu pointers can
        // only be used inside futures.
        last_seen_epoch_id: MIN_EPOCH_ID,
        is_busy: false,
    });
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
    let new_seen_epoch_id = epoch_id_get_cur(
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

    // epoch id should never go down
    // TODO: what about overflow?
    debug_assert!(new_seen_epoch_id >= prev_state.last_seen_epoch_id);

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
