//! provides a per thread storage of some thread state needed for book-keeping.
//!
//! usually, for representing thread local state, [`thread_local!`] is used.
//! but, for the thread state we need the ability to iterate over the thread local state value of all currently registered threads.
//! this is not possible with [`thread_local!`], so we manually implement that mechanism.
use std::{cell::Cell, num::NonZeroU16};

use index_type::IndexType;

use crate::{
    atomic_type::Atomic,
    thread_state::{EncodedThreadState, ThreadState},
};

mod thread_storage_slots;
pub use thread_storage_slots::{ThreadStorageSlots, ThreadStorageSlotsReadGuard};

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
static THREAD_STORAGE_SLOTS: ThreadStorageSlots = ThreadStorageSlots::new();

/// returns all storage slots for iterating over the state of all existing threads.
pub fn thread_storage_slot_get_all() -> ThreadStorageSlotsReadGuard<'static> {
    THREAD_STORAGE_SLOTS.read()
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
    ///
    /// returns the id of the new allocated slot, or the id of the existing slot if there is one.
    pub fn alloc(&self, initial_thread_state: ThreadState) -> ThreadStorageSlotId {
        if let Some(existing_id) = self.id.get() {
            return existing_id;
        }
        let id = THREAD_STORAGE_SLOTS.alloc(initial_thread_state);
        self.id.set(Some(id));
        id
    }

    /// deallocates the current slot, if any.
    /// if no slot is currently allocated, this function does nothing.
    pub fn dealloc(&self) {
        let Some(id) = self.id.get() else { return };
        // SAFETY: this slot was previously allocated from the global storage slots buffer, and was not freed yet.
        unsafe { THREAD_STORAGE_SLOTS.dealloc(id) };
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
    static THREAD_STORAGE_SLOT: OwnedThreadStorageSlot = const { OwnedThreadStorageSlot::unallocated() };
}

/// returns the storage slot id of the current thread, assuming that a storage slot was already allocated for the current
/// thread.
pub fn this_thread_get_storage_slot_id() -> ThreadStorageSlotId {
    THREAD_STORAGE_SLOT.with(|storage_slot| storage_slot.id().unwrap())
}

/// checks if the current thread currently has a storage slot allocated for it.
pub fn this_thread_does_have_allocated_storage_slot() -> bool {
    THREAD_STORAGE_SLOT.with(|storage_slot| storage_slot.id.get().is_some())
}

/// allocates a storage slot for the current thread, if one is not already allocated.
pub fn this_thread_alloc_storage_slot(initial_thread_state: ThreadState) -> ThreadStorageSlotId {
    THREAD_STORAGE_SLOT.with(|storage_slot| storage_slot.alloc(initial_thread_state))
}

/// deallocates the storage slot owned by the current thread, if any.
pub fn this_thread_dealloc_storage_slot() {
    THREAD_STORAGE_SLOT.with(|storage_slot| storage_slot.dealloc())
}
