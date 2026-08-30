//! provides a per thread storage of some thread state needed for book-keeping.
//!
//! usually, for representing thread local state, [`thread_local!`] is used.
//! but, for the thread state we need the ability to iterate over the thread local state value of all currently registered threads.
//! this is not possible with [`thread_local!`], so we manually implement that mechanism.
use std::{cell::Cell, num::NonZeroU16, sync::atomic};

use index_type::{IndexType, array::TypedArray};

use crate::{
    atomic_type::Atomic,
    thread_state::{EncodedThreadState, ThreadState},
};

/// the maximum number of concurrent tokio worker threads.
///
/// using a large value increases the memory usage.
///
/// using a small value limits the number of worker threads that can be used.
/// on machines with many cpu cores, this may actually be a problem.
///
/// we try to use a sweet spot value to allow this to run on a wide variety of machines while not wasting too much memory.
///
// TODO: make this dynamic somehow.
const MAX_CONCURRENT_THREADS: usize = 4096;

/// the id of a slot in the thread storage slots array.
#[derive(IndexType, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct ThreadStorageSlotId(
    // we use a nonzero integer here since we often store `Option<Self>` values, and using a nonzero integer
    // makes `Option<Self>` have the same size as `Self`.
    pub NonZeroU16,
);

/// the value of a single storage slot in the storage slots array.
#[derive(Debug)]
pub struct ThreadStorageSlotValue {
    /// the encoded state of the thread who owns this slot.
    pub state: Atomic<EncodedThreadState>,
}

/// the actual storage slots.
/// each thread allocates a slot by finding an empty one and acquiring it.
/// all slots are initially empty.
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

/// returns all storage slots for iterating over the state of all existing threads.
pub fn thread_storage_slot_get_all()
-> impl Iterator<Item = (ThreadStorageSlotId, &'static ThreadStorageSlotValue)> {
    THREAD_STORAGE_SLOTS.iter_enumerated()
}

/// returns the storage slot with the provided id.
pub fn thread_storage_slot_get(id: ThreadStorageSlotId) -> &'static ThreadStorageSlotValue {
    &THREAD_STORAGE_SLOTS[id]
}

/// allocates a thread storage slot, and once allocated, the slot is initialized to the provided initial state.
///
/// the selected slot's transition from being vacant to being vacant immediately sets its state to the provided state.
/// there is not "allocated but uninitialized" state. as soon as the slot is allocated, it is also initialized to the given initial state.
///
/// if the storage is full and all slots are occupied, this function returns `None`.
///
/// if a slot is allocated and `Some(_)` is returned, this function provides acquire ordering in relation to the free operation of all
/// previous users of this slot.
pub fn thread_storage_slot_alloc(initial_thread_state: ThreadState) -> Option<ThreadStorageSlotId> {
    let encoded_initial_thread_state = initial_thread_state.encode();

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

/// frees the storage slot with the given id.
/// must only be called on an id previously allocated using [`thread_storage_slot_alloc`].
/// the deallocation of the slot provides release memory ordering in relation to all future acquirers of this slot.
pub fn thread_storage_slot_free(id: ThreadStorageSlotId) {
    THREAD_STORAGE_SLOTS[id].state.store(
        ThreadState::NONE_ENCODED_VALUE,
        // we want to make sure that all of our writes to the slot happen before this store, otherwise it will seem as if we are using
        // this slot after it was freed.
        // coupled with an acquire load when allocating the slot.
        atomic::Ordering::Release,
    );
}

/// an owned thread storage slot, intended to be used as a thread local variable.
///
/// this represents an optional owned storage slot, initially it starts empty, and you can then allocate and deallocate it.
///
/// this type has a proper drop impl which frees the slot, in case the thread unexpectedly exits without manually deallocating the slot.
pub struct OwnedThreadStorageSlot {
    id: Cell<Option<ThreadStorageSlotId>>,
}
impl OwnedThreadStorageSlot {
    /// creates a new unallocated instance not associated with any actual slot.
    /// to allocate a slot, call the [`allocate`](Self::alloc) function.
    pub const fn unallocated() -> Self {
        Self {
            id: Cell::new(None),
        }
    }

    /// allocates a new slot for the current thread, if one is not already allocated.
    /// if a slot is already allocated, this function does nothing.
    pub fn alloc(&self, initial_thread_state: ThreadState) {
        if self.id.get().is_some() {
            return;
        }
        let id = thread_storage_slot_alloc(initial_thread_state).expect(
            "too many concurrent threads, failed to allocate a storage slot for a new thread",
        );
        self.id.set(Some(id));
    }

    /// deallocates the current slot, if any.
    /// if no slot is currently allocated, this function does nothing.
    pub fn dealloc(&self) {
        let Some(id) = self.id.get() else { return };
        thread_storage_slot_free(id);
        self.id.set(None);
    }

    /// returns the id of the current slot, if any.
    pub fn id(&self) -> Option<ThreadStorageSlotId> {
        self.id.get()
    }
}
impl Drop for OwnedThreadStorageSlot {
    fn drop(&mut self) {
        self.dealloc();
    }
}

thread_local! {
    /// a thread local variable which represents the storage slot currently owned by the current thread.
    static THREAD_STORAGE_SLOT: OwnedThreadStorageSlot = OwnedThreadStorageSlot::unallocated();
}

/// returns the storage slot id of the current thread, assuming that a storage slot was already allocated for the current
/// thread.
pub fn this_thread_get_storage_slot_id() -> ThreadStorageSlotId {
    THREAD_STORAGE_SLOT.with(|storage_slot| storage_slot.id().unwrap())
}

/// returns the storage slot of the current thread, assuming that a storage slot was already allocated for the current
/// thread.
pub fn this_thread_get_storage_slot() -> &'static ThreadStorageSlotValue {
    THREAD_STORAGE_SLOT.with(|storage_slot| thread_storage_slot_get(storage_slot.id().unwrap()))
}

/// allocates a storage slot for the current thread, if one is not already allocated.
pub fn this_thread_alloc_storage_slot(initial_thread_state: ThreadState) {
    THREAD_STORAGE_SLOT.with(|storage_slot| storage_slot.alloc(initial_thread_state))
}

/// deallocates the storage slot owned by the current thread, if any.
pub fn this_thread_dealloc_storage_slot() {
    THREAD_STORAGE_SLOT.with(|storage_slot| storage_slot.dealloc())
}
