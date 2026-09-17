use std::{
    cell::UnsafeCell,
    ops::Deref,
    ptr::NonNull,
    sync::atomic::{self, AtomicUsize},
};

use branches::likely;
use index_type::{IndexType, slice::TypedSlice, vec::TypedVec};

use crate::{
    atomic_type::Atomic,
    per_thread_storage::{ThreadStorageSlotId, ThreadStorageSlotValue},
    thread_state::{EncodedThreadState, ThreadState},
};

/// all information needed to represent the "current data" of a thread storage slots instance.
/// this is basically the raw parts of the backing vector used to allocate this data.
struct ThreadStorageSlotsCurData {
    ptr: *mut ThreadStorageSlotValue,

    /// the length of the buffer.
    ///
    /// note that this is atomic even though this whole structure typically sits inside a big `UnsafeCell` since in some specific
    /// scenarios we only want to update the len, without getting mutable access to the entire struct.
    len: AtomicUsize,

    capacity: usize,
}
impl ThreadStorageSlotsCurData {
    /// creates a new empty cur data info representing an empty buffer.
    const fn new() -> Self {
        Self {
            ptr: NonNull::dangling().as_ptr(),
            len: AtomicUsize::new(0),
            capacity: 0,
        }
    }
}

/// a dummy type used as the inner data type of the write lock. used to distinguish write lock guards from other guards.
///
/// # Safety
///
/// must only be used as the data of the write lock.
struct WriteLockMarker;

/// a dummy type used as the inner data type of the cur data lock. used to distinguish cur data lock guards from other guards.
///
/// # Safety
///
/// must only be used as the data of the cur data lock.
struct CurDataLockMarker;

/// concurrent data structure for holding the buffer containing the storage slots of the different threads that play part of the
/// rcu book-keeping.
pub struct ThreadStorageSlots {
    /// the current data buffer.
    /// protected by both the cur data lock and the write lock.
    cur_data: UnsafeCell<ThreadStorageSlotsCurData>,

    /// a lock protecting the current data.
    /// it is provided as an external lock instead of wrapping the cur data directly, since the cur data is also protected from writes
    /// by writing the write lock. doing it separately allow us to access the inner data in such scenarios without having to lock this
    /// lock when it is not needed.
    cur_data_lock: parking_lot::RwLock<CurDataLockMarker>,

    /// a lock which is used to make writers mutually exclusive, such that at any given moment, only one writer can work.
    write_lock: std::sync::Mutex<WriteLockMarker>,

    /// indices of free slots.
    /// protected by the write lock.
    free_slots: UnsafeCell<Vec<ThreadStorageSlotId>>,
}
impl ThreadStorageSlots {
    /// creates a new empty slots buffer.
    pub const fn new() -> Self {
        Self {
            cur_data: UnsafeCell::new(ThreadStorageSlotsCurData::new()),
            cur_data_lock: parking_lot::RwLock::new(CurDataLockMarker),
            write_lock: std::sync::Mutex::new(WriteLockMarker),
            free_slots: UnsafeCell::new(Vec::new()),
        }
    }

    /// returns a read guard for the current slots buffer. the returned guard dereferences to a slice of all slots.
    ///
    /// you must not block while holding the guard, and must not hold it for a "long time".
    ///
    /// this function provides acquire memory ordering in relation to writers that re-allocate the data buffer.
    pub fn read(&self) -> ThreadStorageSlotsReadGuard<'_> {
        ThreadStorageSlotsReadGuard {
            _guard: self.cur_data_lock.read(),
            origin: self,
        }
    }

    /// returns the current data as a slice.
    ///
    /// # Safety
    ///
    /// you must guarantee that during this operation, no-one will swap the current data.
    /// you must also make sure to only use the returned slice as long as it is guaranteed that no-one will swap the current data.
    unsafe fn cur_data_as_slice(&self) -> &TypedSlice<ThreadStorageSlotId, ThreadStorageSlotValue> {
        // SAFETY: caller guarantees that no-one writes to the data
        let cur_data = unsafe { &*self.cur_data.get() };

        // SAFETY: the slices stored are always valid slices.
        unsafe {
            TypedSlice::from_raw_parts(
                cur_data.ptr,
                ThreadStorageSlotId::from_raw_index(cur_data.len.load(
                    // use acquire ordering to make sure that in the case where we see len increments, we are guaranteed to first see
                    // the write to the pointed-at data.
                    atomic::Ordering::Acquire,
                )),
            )
        }
    }

    /// waits for all readers of the current data to finish using it, blocks new readers, and then lets you modify the current data.
    ///
    /// must be called while holding the write lock.
    ///
    /// provides acquire ordering in relation to any previously existing readers of the data pointer.
    /// this acquire ordering is already provided inside the callback, and intuitively, it is also maintained outside of it.
    ///
    /// furthermore, once finished, it provides release ordering in relation to future readers of the data.
    fn modify_cur_data<F, R>(
        &self,
        f: F,
        _write_guard: &std::sync::MutexGuard<'_, WriteLockMarker>,
    ) -> R
    where
        F: FnOnce(&mut ThreadStorageSlotsCurData) -> R,
    {
        // wait for all current readers to finish, and prevent new readers from entering.
        let _write_guard = self.cur_data_lock.write();

        // SAFETY: we are holding the write lock, so no one can write to this other than us.
        let cur_data = unsafe { &mut *self.cur_data.get() };

        f(cur_data)
    }

    /// allocates a new storage slot for some thread, given the thread's initial state.
    ///
    /// the selected slot's transition from being vacant to being vacant immediately sets its state to the provided state.
    /// there is not "allocated but uninitialized" state. as soon as the slot is allocated, it is also initialized to the given
    /// initial state.
    ///
    /// this function provides acquire ordering in relation to the free operation of all previous users of the returned slot.
    pub fn alloc(&self, initial_thread_state: ThreadState) -> ThreadStorageSlotId {
        let encoded_initial_thread_state = initial_thread_state.encode();

        // synchronize with other writers. at any given point, only one writer can work.
        let write_guard = self.write_lock.lock().unwrap();

        // SAFETY: we are holding the write lock.
        let free_slots = unsafe { &mut *self.free_slots.get() };

        match free_slots.pop() {
            Some(free_slot_id) => {
                // have a free slot in the existing storage, use it.
                // SAFETY: the free slot id originated from the list of free slot ids.
                unsafe {
                    self.alloc_from_free_slot(
                        encoded_initial_thread_state,
                        free_slot_id,
                        &write_guard,
                    )
                }
            }
            None => self.alloc_no_free_slots(encoded_initial_thread_state, write_guard),
        }
    }

    /// allocates a storage slot given a free slot in the existing storage buffer.
    ///
    /// # Safety
    ///
    /// the provided storage slot must have originated from the list of free slot ids.
    unsafe fn alloc_from_free_slot(
        &self,
        encoded_initial_thread_state: EncodedThreadState,
        free_slot_id: ThreadStorageSlotId,
        _write_guard: &std::sync::MutexGuard<'_, WriteLockMarker>,
    ) -> ThreadStorageSlotId {
        // SAFETY: we are holding the write lock, so no one can modify the cur data other than us.
        let cur_data = unsafe { self.cur_data_as_slice() };

        cur_data[free_slot_id].state.store(
            encoded_initial_thread_state,
            // release ordering is needed here to keep a happens-before chain between any previous users of this slot, and readers
            // of this slot.
            //
            // at this point, we ourselves are already synchronized with any previous user of this slot, since we got the slot id from
            // the list of free slot ids, which is protected by the write lock. so, any previous writes performed by previous users are
            // visible to us at this point.
            //
            // we want to make sure that any thread loading this value will also see all writes performed by previous owners of this
            // slot. so, we use release ordering here.
            atomic::Ordering::Release,
        );
        free_slot_id
    }

    /// allocates a storage slot given that the current storage buffer is empty, and a new one must be allocated.
    ///
    /// # Safety
    ///
    /// must only be called if the current data is empty (capacity == 0).
    unsafe fn alloc_no_cur_data(
        &self,
        new_slot_value: ThreadStorageSlotValue,
        write_guard: std::sync::MutexGuard<'_, WriteLockMarker>,
    ) -> ThreadStorageSlotId {
        // assuming a multi-threaded tokio runtime, which is what is expected to be used with this crate, we will have at
        // least `num_cpus` worker threads plus 1 main thread, so pre-allocate enough space for that amount.
        let mut new_data: TypedVec<ThreadStorageSlotId, ThreadStorageSlotValue> =
            TypedVec::with_capacity(num_cpus::get() + 1);
        new_data.push(new_slot_value);

        let (new_data_ptr, new_data_len, new_data_capacity) = new_data.into_raw_parts();

        self.modify_cur_data(
            |cur_data| {
                cur_data.ptr = new_data_ptr;
                cur_data.len = AtomicUsize::new(new_data_len);
                cur_data.capacity = new_data_capacity;
            },
            &write_guard,
        );

        ThreadStorageSlotId::ZERO
    }

    /// allocate a storage slot given that the current storage buffer has no empty slots.
    fn alloc_no_free_slots(
        &self,
        encoded_initial_thread_state: EncodedThreadState,
        write_guard: std::sync::MutexGuard<'_, WriteLockMarker>,
    ) -> ThreadStorageSlotId {
        // no free slots in the existing storage, allocate a bigger vector.

        let new_slot_value = ThreadStorageSlotValue {
            state: Atomic::<EncodedThreadState>::new(encoded_initial_thread_state),
        };

        // SAFETY: we are holding the write lock, so no one can write to this other than us.
        let cur_data = unsafe { &*self.cur_data.get() };

        if cur_data.capacity == 0 {
            // no storage vector currently allocated, allocate a new one.
            // SAFETY: capacity is zero so the current buffer is empty
            unsafe { self.alloc_no_cur_data(new_slot_value, write_guard) }
        } else {
            // a storage vector is currently allocated, push a new entry into it.
            // SAFETY: capacity is non-zero so the current buffer is valid
            unsafe { self.alloc_no_free_slots_grow_cur_data(new_slot_value, write_guard) }
        }
    }

    /// allocate a storage slot given that the current storage buffer has no empty slots, and given that the current storage buffer
    /// is non-empty, and we should thus grow it instead of allocating a new one.
    ///
    /// # Safety
    ///
    /// may only be called if the current buffer is non-empty (capacity != 0)
    unsafe fn alloc_no_free_slots_grow_cur_data(
        &self,
        new_slot_value: ThreadStorageSlotValue,
        write_guard: std::sync::MutexGuard<'_, WriteLockMarker>,
    ) -> ThreadStorageSlotId {
        // SAFETY: we are holding the write lock, so no one can write to this other than us.
        let cur_data = unsafe { &*self.cur_data.get() };

        let len = cur_data.len.load(
            // ordering doesn't matter, we have exclusive access to this field due to the write lock
            atomic::Ordering::Relaxed,
        );

        // SAFETY: this function is only called when we have an existing storage vector.
        let mut new_data: TypedVec<ThreadStorageSlotId, ThreadStorageSlotValue> =
            unsafe { TypedVec::from_raw_parts_unchecked(cur_data.ptr, len, cur_data.capacity) };

        if likely(len < cur_data.capacity) {
            // no-reallocation needed, we can push into the vec and it won't re-alloc.
            let new_slot_id = new_data
                .try_push(new_slot_value)
                .expect("too many concurrent threads");

            // update the len to the new incremented len
            cur_data.len.store(
                new_data.len().to_raw_index(),
                // use release ordering to make sure that the previous write to the new slot happens before the len increment.
                //
                // note that we break the release-sequence of this variable here due to using a plain store, which is not a RMW operation.
                // but, this is fine since the happens before chain is maintained through another synchronization primitive - the write
                // lock, which synchronizes us with all previous incrementers of the len.
                atomic::Ordering::Release,
            );

            // avoid dropping the data, it is still being used as the storage buffer in this case
            core::mem::forget(new_data);

            new_slot_id
        } else {
            // re-allocation needed. the re-allocation may free the current data pointer and move the allocation to a new
            // location. so, while re-allocating, we need to guarantee that no readers use the data.
            //
            // furthermore, note that the acquire and then release ordering provided by `modify_cur_data` are needed to maintain
            // a correct happens before chain for writes to the pointed-at data performed by any previous "readers", since after this
            // operation, future readers may read data from a completely different location in memory than the location to which those
            // previous writes were performed.
            //
            // the acquire then release ordering make sure that those future loads will be ordered after any previous writes performed
            // by any previous "readers".
            self.modify_cur_data(
                |cur_data| {
                    let new_slot_id = new_data.push(new_slot_value);

                    // the push changed some of the parameters, re-write them
                    let (new_data_ptr, new_data_len, new_data_capacity) = new_data.into_raw_parts();
                    cur_data.ptr = new_data_ptr;
                    cur_data.len = AtomicUsize::new(new_data_len);
                    cur_data.capacity = new_data_capacity;

                    new_slot_id
                },
                &write_guard,
            )
        }
    }

    /// de-allocates the slot with the provided id.
    ///
    /// provides release memory ordering in relation to any future readers of this slot.
    ///
    /// # Safety
    ///
    /// the provided slot id must have been allocated using this instance, and must not have been freed since it was first allocated.
    pub unsafe fn dealloc(&self, slot_id: ThreadStorageSlotId) {
        // synchronize with other writers. at any given point, only one writer can work.
        let _write_guard = self.write_lock.lock().unwrap();

        // SAFETY: we are holding the write lock, so no one can modify the cur data other than us.
        let cur_data = unsafe { self.cur_data_as_slice() };

        cur_data[slot_id].state.store(
            ThreadState::NONE_ENCODED_VALUE,
            // make sure that all of our previous writes happen before the release of the slot.
            atomic::Ordering::Release,
        );

        // SAFETY: we are holding the write lock.
        let free_slots = unsafe { &mut *self.free_slots.get() };
        free_slots.push(slot_id);
    }
}
impl Drop for ThreadStorageSlots {
    fn drop(&mut self) {
        let cur_data = self.cur_data.get_mut();
        if cur_data.capacity != 0 {
            let _ = unsafe {
                TypedVec::<ThreadStorageSlotId, ThreadStorageSlotValue>::from_raw_parts_unchecked(
                    cur_data.ptr,
                    cur_data.len.load(atomic::Ordering::Relaxed),
                    cur_data.capacity,
                )
            };
        }
    }
}

// this type can safely be shared, all accesses to shared data are properly protected by locks.
unsafe impl Sync for ThreadStorageSlots {}

/// a read guard for the thread storage slots. dereferences into a slice of all slots.
/// you must not block while holding this guard, and must not hold it for a "long time".
pub struct ThreadStorageSlotsReadGuard<'a> {
    _guard: parking_lot::RwLockReadGuard<'a, CurDataLockMarker>,
    origin: &'a ThreadStorageSlots,
}
impl<'a> Deref for ThreadStorageSlotsReadGuard<'a> {
    type Target = TypedSlice<ThreadStorageSlotId, ThreadStorageSlotValue>;

    fn deref(&self) -> &Self::Target {
        // SAFETY: we are holding a refcount to the data, so it won't be modified until we are dropped.
        unsafe { self.origin.cur_data_as_slice() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic;

    use crate::{
        epoch::{EPOCH_ID_MIN, EpochId},
        per_thread_storage::{ThreadStorageSlotId, ThreadStorageSlots},
        thread_state::ThreadState,
    };

    #[test]
    fn test_basic() {
        let slots = ThreadStorageSlots::new();
        let thread_state = ThreadState {
            last_seen_epoch_id: EPOCH_ID_MIN,
            is_busy: true,
        };
        let slot_id = slots.alloc(thread_state);
        let read_guard = slots.read();
        assert_eq!(
            read_guard[slot_id].state.load(atomic::Ordering::Relaxed),
            thread_state.encode()
        );
        unsafe { slots.dealloc(slot_id) };
    }

    #[test]
    fn test_multiple_allocs() {
        const NUM_ALLOCS: u16 = 1024;
        fn thread_state_by_alloc_index(alloc_index: u16) -> ThreadState {
            ThreadState {
                last_seen_epoch_id: ((alloc_index + 1) * 2) as EpochId,
                is_busy: true,
            }
        }
        let slots = ThreadStorageSlots::new();
        let slot_ids: Vec<ThreadStorageSlotId> = (0..NUM_ALLOCS)
            .map(|i| slots.alloc(thread_state_by_alloc_index(i)))
            .collect();

        for i in 0..NUM_ALLOCS {
            let slot_id = slot_ids[i as usize];

            assert_eq!(
                slots.read()[slot_id].state.load(atomic::Ordering::Relaxed),
                thread_state_by_alloc_index(i).encode()
            );
        }

        for slot in slot_ids {
            unsafe { slots.dealloc(slot) };
        }
    }

    #[test]
    fn test_realloc() {
        let slots = ThreadStorageSlots::new();
        let thread_state = ThreadState {
            last_seen_epoch_id: EPOCH_ID_MIN,
            is_busy: true,
        };
        let id = slots.alloc(thread_state);
        unsafe { slots.dealloc(id) };
        for _ in 0..128 {
            let new_id = slots.alloc(thread_state);
            assert_eq!(id, new_id);
            unsafe { slots.dealloc(new_id) };
        }
    }
}
