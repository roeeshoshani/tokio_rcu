use std::{num::NonZeroU16, sync::atomic};

use index_type::{IndexType, array::TypedArray};

use crate::{
    atomic_type::Atomic,
    thread_state::{EncodedThreadState, ThreadState},
};

const MAX_CONCURRENT_THREADS: usize = 4096;

#[derive(IndexType, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct ThreadStorageSlotId(
    // we use a nonzero integer here since we often store `Option<Self>` values, and using a nonzero integer
    // makes `Option<Self>` have the same size as `Self`.
    pub NonZeroU16,
);

#[derive(Debug)]
pub struct ThreadStorageSlotValue {
    pub state: Atomic<EncodedThreadState>,
}

static THREAD_STORAGE_SLOTS: TypedArray<
    ThreadStorageSlotId,
    ThreadStorageSlotValue,
    MAX_CONCURRENT_THREADS,
> = TypedArray::from_array(
    [const {
        ThreadStorageSlotValue {
            state: Atomic::<EncodedThreadState>::new(0),
        }
    }; MAX_CONCURRENT_THREADS],
);

pub fn thread_storage_slot_get_all() -> &'static [ThreadStorageSlotValue] {
    THREAD_STORAGE_SLOTS.as_slice().as_slice()
}

pub fn thread_storage_slot_get(id: ThreadStorageSlotId) -> &'static ThreadStorageSlotValue {
    &THREAD_STORAGE_SLOTS[id]
}

fn thread_storage_slot_try_alloc(
    encoded_initial_thread_state: EncodedThreadState,
) -> Option<ThreadStorageSlotId> {
    for (slot_id, slot) in THREAD_STORAGE_SLOTS.iter_enumerated() {
        // TODO: ordering
        //
        // TODO: is the initial load needed? does it improve performance over just immediately trying
        // compare exchange?
        if slot.state.load(atomic::Ordering::Relaxed) == ThreadState::NONE_ENCODED_VALUE {
            // TODO: ordering
            match slot.state.compare_exchange(
                ThreadState::NONE_ENCODED_VALUE,
                encoded_initial_thread_state,
                atomic::Ordering::Relaxed,
                atomic::Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // successfully allocated this cell
                    return Some(slot_id);
                }
                Err(_) => {
                    // failed to allocate this slot, continue trying
                }
            }
        }
    }
    None
}

pub fn thread_storage_slot_alloc(initial_thread_state: ThreadState) -> ThreadStorageSlotId {
    const MAX_ATTEMPTS: usize = 16;

    let encoded_initial_thread_state = initial_thread_state.encode();

    for _ in 0..MAX_ATTEMPTS {
        if let Some(allocated_slot_id) = thread_storage_slot_try_alloc(encoded_initial_thread_state)
        {
            return allocated_slot_id;
        }
    }

    panic!("too many concurrent threads, failed to allocate a storage slot for a new thread");
}

pub fn thread_storage_slot_free(id: ThreadStorageSlotId) {
    // TODO: ordering
    THREAD_STORAGE_SLOTS[id]
        .state
        .store(ThreadState::NONE_ENCODED_VALUE, atomic::Ordering::Relaxed);
}
