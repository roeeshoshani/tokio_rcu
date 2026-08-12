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

fn thread_storage_slot_alloc_one_try(
    encoded_initial_thread_state: EncodedThreadState,
) -> Option<ThreadStorageSlotId> {
    for (slot_id, slot) in THREAD_STORAGE_SLOTS.iter_enumerated() {
        // we first perform a load, and only if the load shows that the slot is empty, we try a compare exchange.
        // we could just perform a compare exchange directly without the extra load, but that would be slower.
        //
        // if most of the first slots are occupied, and if the slots change infrequently, which is true in our case where the slots
        // are only modified on thread creation and destruction, then performing a full compare exchange operation on each of those
        // initial occupied slots is more expansive than a load.
        //
        // the load is basically a fast path for occupied slots.
        //
        // as for the ordering of this operation, we don't really care about ordering since this is merely an optimization.
        // assuming that the slots change very infrequently, the value seen here and in the compare exchange will probably be the same,
        // regardless of ordering.
        //
        // in the uncommon case where the ordering of this and the compare exchange will be reversed, nothing bad will happen, we may
        // just confuse a used slot and think that it is free.
        if slot.state.load(atomic::Ordering::Relaxed) == ThreadState::NONE_ENCODED_VALUE {
            match slot.state.compare_exchange(
                ThreadState::NONE_ENCODED_VALUE,
                encoded_initial_thread_state,
                // in the success case, we use acquire ordering coupled with release ordering when freeing a slot to make sure that
                // all previous writes to this slot's data by its previous owner happen before the store which released the slot, so
                // that none of the previous owner's action happen while we own the slot.
                atomic::Ordering::Acquire,
                // when this operation fails, we don't care about ordering.
                // the failing load's result is not used anyway, and the fact that it failed is not used to synchronize anything.
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

pub fn thread_storage_slot_alloc(initial_thread_state: ThreadState) -> Option<ThreadStorageSlotId> {
    const MAX_ATTEMPTS: usize = 16;

    let encoded_initial_thread_state = initial_thread_state.encode();

    for _ in 0..MAX_ATTEMPTS {
        if let Some(allocated_slot_id) =
            thread_storage_slot_alloc_one_try(encoded_initial_thread_state)
        {
            return Some(allocated_slot_id);
        }
    }

    None
}

pub fn thread_storage_slot_free(id: ThreadStorageSlotId) {
    THREAD_STORAGE_SLOTS[id].state.store(
        ThreadState::NONE_ENCODED_VALUE,
        // we want to make sure that all of our writes to the slot happen before this store, otherwise it will seem as if we are using
        // this slot after it was freed.
        // coupled with an acquire load when allocating the slot.
        atomic::Ordering::Release,
    );
}
