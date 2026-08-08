use std::{
    cell::Cell,
    ops::Deref,
    sync::atomic::{self, AtomicPtr},
};

use tokio::sync::Notify;

use crate::{
    epoch::{epoch_id_get_cur, epoch_id_inc},
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
            let encoded_state = storage_slot.state.load(atomic::Ordering::Relaxed);
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
        // TODO: ordering
        let ptr = self.value_ptr.load(atomic::Ordering::Relaxed);

        RcuReadGuard {
            // SAFETY: TODO
            value: unsafe { &*ptr },
            _phantom: PhantomUnsendUnsync::new(),
        }
    }

    pub async fn swap(&self, new_value: T) -> T {
        let new_value_ptr = Box::leak(Box::new(new_value));

        // TODO: ordering
        let old_value_ptr = self
            .value_ptr
            .swap(new_value_ptr, atomic::Ordering::Relaxed);

        // wait for all previous readers to stop using the old value
        synchronize_rcu().await;

        // SAFETY: TODO
        let boxed_value = unsafe { Box::from_raw(old_value_ptr) };

        *boxed_value
    }
}
impl<T> Drop for Rcu<T> {
    fn drop(&mut self) {
        // TODO: ordering
        let ptr = self.value_ptr.load(atomic::Ordering::Relaxed);

        // SAFETY: TODO
        let _ = unsafe { Box::from_raw(ptr) };
    }
}

thread_local! {
    static THREAD_STORAGE_SLOT: Cell<Option<ThreadStorageSlotId>> = Cell::new(None);
}

pub fn on_thread_start() {
    let storage_slot = thread_storage_slot_alloc(ThreadState {
        // TODO: ordering
        last_seen_epoch_id: epoch_id_get_cur(atomic::Ordering::Relaxed),
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
    let new_seen_epoch_id = epoch_id_get_cur(atomic::Ordering::Relaxed);

    let prev_state_encoded = storage_slot.state.swap(
        ThreadState {
            // TODO: ordering
            last_seen_epoch_id: new_seen_epoch_id,
            is_busy,
        }
        .encode(),
        // TODO: ordering
        atomic::Ordering::Relaxed,
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

    thread_fetch_new_epoch_id_and_update_waiters(storage_slot, false);
}
