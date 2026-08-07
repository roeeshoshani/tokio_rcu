use std::{
    cell::Cell,
    marker::PhantomData,
    ops::Deref,
    sync::{
        Arc,
        atomic::{self, AtomicPtr, AtomicU32, AtomicUsize},
    },
};

use index_type::{IndexType, array::TypedArray};
use tokio::sync::Notify;

pub type EpochId = u32;
pub type EpochIdAtomic = AtomicU32;

#[derive(IndexType, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct StorageSlotId(u16);

const MAX_CONCURRENT_THREADS: usize = 4096;

static CUR_EPOCH_ID: EpochIdAtomic = EpochIdAtomic::new(1);
static THREAD_EPOCH_UPDATED_NOTIFY: Notify = Notify::const_new();

static PER_THREAD_LAST_SEEN_EPOCH_ID: TypedArray<
    StorageSlotId,
    EpochIdAtomic,
    MAX_CONCURRENT_THREADS,
> = TypedArray::from_array([const { EpochIdAtomic::new(0) }; MAX_CONCURRENT_THREADS]);

thread_local! {
    static STORAGE_SLOT_ID: Cell<StorageSlotId> = const { Cell::new(StorageSlotId(0)) };
}

/// increments the epoch id, returning the new epoch id after the increment.
fn increment_epoch_id() -> EpochId {
    // TODO: ordering
    let orig = CUR_EPOCH_ID.fetch_add(1, atomic::Ordering::Relaxed);

    if orig != EpochId::MAX {
        // SAFETY: we know that this will not overflow since the value is less than the max
        return unsafe { orig.unchecked_add(1) };
    }

    // epoch id 0 is reserved, so try incrementing again if we reach 0
    // TODO: ordering
    let orig = CUR_EPOCH_ID.fetch_add(1, atomic::Ordering::Relaxed);
    if orig == EpochId::MAX {
        // if we reached 0 again, it means that during the short time between the first increment and the second, `EpochId::MAX` increments
        // were concurrently performed. this is expected to be a very large number, so this is so unlikely that we expect it to never
        // happen.
        panic!(
            "too many concurrent incremenets to the epoch id, failed to increment it beyond the reserved value of 0"
        );
    }

    // SAFETY: we know that this will not overflow since the value is less than the max
    unsafe { orig.unchecked_add(1) }
}

async fn synchronize_rcu() {
    let new_epoch_id = increment_epoch_id();

    loop {
        // start subscribing to the notified waiters event before checking the current state.
        //
        // if we first check the state and only then start listening, there may be a small window after we finish checking the values but
        // before we start listening where some thread updates its counter and notifies all wakers, but we will miss that notification,
        // which is problematic.
        //
        // so, we start listening before checking the values, so that even notifications that are issued while or right after we finished
        // checking are still received.
        //
        // TODO: should i implement my own optimized primitive for this instead of tokio's `Notify` for better peformance?
        let notified = THREAD_EPOCH_UPDATED_NOTIFY.notified();

        // check if all threads have seen our new epoch id
        //
        // TODO: what if the epoch id overflows? we may get stuck in an infinite loop.
        if PER_THREAD_LAST_SEEN_EPOCH_ID
            .iter()
            .all(|thread_last_seen_epoch_id| {
                thread_last_seen_epoch_id.load(atomic::Ordering::Relaxed) >= new_epoch_id
            })
        {
            // all threads saw our new epoch id, we are done waiting
            break;
        }

        // some of the threads haven't yet seen our new epoch id.
        // so, wait for them to go through a quiescent state and see our new epoch id.
        notified.await;
    }
}

fn try_allocate_storage_slot(thread_epoch_id: EpochId) -> Option<StorageSlotId> {
    for (slot_id, slot) in PER_THREAD_LAST_SEEN_EPOCH_ID.iter_enumerated() {
        // TODO: ordering
        // TODO: is the initial load needed? does it improve performance over just immediately trying compare exchange?
        if slot.load(atomic::Ordering::Relaxed) == 0 {
            // TODO: ordering
            match slot.compare_exchange(
                0,
                thread_epoch_id,
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
fn allocate_storage_slot(initial_epoch_id: EpochId) -> StorageSlotId {
    const MAX_ATTEMPTS: usize = 16;

    for _ in 0..MAX_ATTEMPTS {
        if let Some(allocated_slot_id) = try_allocate_storage_slot(initial_epoch_id) {
            return allocated_slot_id;
        }
    }

    panic!("too many concurrent threads, failed to allocate a storage slot for a new thread");
}
fn free_storage_slot(slot_id: StorageSlotId) {
    // TODO: ordering
    PER_THREAD_LAST_SEEN_EPOCH_ID[slot_id].store(0, atomic::Ordering::Relaxed);
}

fn is_send<T: Send>() {}

/// a phantom type which is not `Send` and also not `Sync`.
pub struct PhantomUnsendUnsync {
    phantom: PhantomData<*const ()>,
}
impl PhantomUnsendUnsync {
    pub const fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

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

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| {
            // TODO: ordering
            let initial_epoch_id = CUR_EPOCH_ID.load(atomic::Ordering::Relaxed);
            let slot_id = allocate_storage_slot(initial_epoch_id);
            STORAGE_SLOT_ID.set(slot_id);
        })
        .on_thread_stop(|| {
            let slot_id = STORAGE_SLOT_ID.get();
            free_storage_slot(slot_id);
        })
        .on_thread_park(|| {
            let slot_id = STORAGE_SLOT_ID.get();

            // TODO: ordering
            let cur_epoch_id = CUR_EPOCH_ID.load(atomic::Ordering::Relaxed);

            // TODO: ordering
            let prev_epoch_id = PER_THREAD_LAST_SEEN_EPOCH_ID[slot_id]
                .swap(cur_epoch_id, atomic::Ordering::Relaxed);

            if prev_epoch_id != cur_epoch_id {
                // if the epoch id changed, some waiter may now be able to finish waiting. so, notify all waiters.
                THREAD_EPOCH_UPDATED_NOTIFY.notify_waiters();
            }
        })
        .build()
        .unwrap();

    rt.block_on(async {
        println!("hello");
    })
}
