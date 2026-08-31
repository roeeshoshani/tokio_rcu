use std::{
    ops::Deref,
    sync::atomic::{self, AtomicPtr},
};

use crate::{
    synchronize_rcu,
    utils::{PhantomUnsendUnsync, PtrMutSendSync},
};

/// a read guard representing the data pointed at by an rcu protected pointer. this provides a temporary view into the underlying data.
///
/// this guard must not be held across await points, and must not escape the future that acquired it in any way.
///
/// this must manually be taken care of by the programmer. incorrect use will lead to undefined behaviour.
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

/// old data of an rcu protected pointer, returned after the pointer was swapped to a new one.
///
/// **this type must not be dropped**, you must call [`wait`](RcuOldData::wait) on it.
/// that's because the data can't be freed until we know that all previous readers of this pointer finished using it.
/// dropping this type without waiting means that old readers may still be using the pointer, so the data can't be freed.
/// so, dropping this type without waiting for it leaks the pointer and panics.
pub struct RcuOldData<T> {
    /// the old data pointer, but make it `Send` and `Sync`.
    /// this is safe since this field is basically just a pointer to a heap allocation, and can thus safely be sent and shared between
    /// threads.
    old_data_ptr: PtrMutSendSync<T>,
}
impl<T> RcuOldData<T> {
    /// creates a new old data pointer guard.
    ///
    /// # Safety
    ///
    /// the provided pointer must be the old data pointer of an rcu protected pointer, and must have already been swapped by a new
    /// pointer.
    unsafe fn new(old_data_ptr: *mut T) -> Self {
        Self {
            old_data_ptr: unsafe {
                // SAFETY: the old data pointer is basically just a pointer to a heap allocation, and can thus safely be sent and shared between
                // threads.
                PtrMutSendSync::new(old_data_ptr)
            },
        }
    }
    /// wait for all potential existing users of this old pointer to finish using it, and then returned an owned version of the pointer
    /// once it is guaranteed to no longer be in use by anyone else.
    ///
    /// # cancellation safety
    ///
    /// function is not cancellation safe. if cancelled, it will leak the pointer and panic.
    pub async fn wait(self) -> Box<T> {
        // wait for all previous readers to stop using the old value
        synchronize_rcu().await;

        // SAFETY: all existing readers finished using this pointers, so it is now exclusively ours.
        // also, pointers are always valid pointers to valid data by the invariants of the `Rcu` type.
        let res = unsafe { Box::from_raw(self.old_data_ptr.ptr()) };

        // this type is not allowed to be dropped, so avoid running its panicking destructor.
        // we are finished doing all cleanup at this point anyway.
        std::mem::forget(self);

        res
    }
}
impl<T> Drop for RcuOldData<T> {
    #[track_caller]
    #[inline]
    fn drop(&mut self) {
        panic!(
            "{} can't be dropped since concurrent readers may be using it. it must first be waited for.",
            std::any::type_name::<Self>()
        );
    }
}

/// an rcu protected pointer.
pub struct Rcu<T> {
    value_ptr: AtomicPtr<T>,
}
impl<T> Rcu<T> {
    /// creates a new rcu protected pointer pointing to the given data.
    pub fn new(value: Box<T>) -> Self {
        Self {
            value_ptr: AtomicPtr::new(Box::leak(value)),
        }
    }

    /// reads the rcu protected pointer and provides access to the data it currently points to.
    ///
    /// the usage of the data is limited to the provided closure to prevent it from being used across await points, and to prevent it
    /// from escaping the calling function. this is needed to guarantee correct use of the rcu protected pointer.
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        // SAFETY: the guard only lives throughout the current function.
        // so, the future can't yield while holding it.
        // and, it can't escape since the callback function F is an HRTB, so it can't assume anything about the lifetime of the provided
        // reference.
        let guard = unsafe { self.read() };

        f(&*guard)
    }

    /// reads the rcu protected pointer, returning a read guard to the data it currently points to.
    ///
    /// # Safety
    ///
    /// this must only be called from a future running inside the tokio runtime.
    ///
    /// furthermore, this guard must not be held across await points, and must not escape.
    ///
    /// as soon as the future that acquired this read guard gets to a point where it `await`s or finished execution (basically any
    /// point which voluntarily yields the future), the guard must have already been dropped.
    pub unsafe fn read(&self) -> RcuReadGuard<'_, T> {
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

    /// swaps the current value to the new value, and returns a guard containing the old value, which can be owned after waiting
    /// for all previous users of that pointer to finish using it.
    pub fn swap_nowait(&self, new_value: Box<T>) -> RcuOldData<T> {
        let new_value_ptr = Box::leak(new_value);

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

        // SAFETY: we provide the old pointer after swapping it with a new one.
        unsafe { RcuOldData::new(old_value_ptr) }
    }

    /// swaps the current value to the new value, waits for all previous users of the old value to finish using it, and returns an
    /// owned version of the old value.
    ///
    /// # cancellation safety
    ///
    /// function is not cancellation safe. if cancelled, it will leak the old pointer and panic.
    pub async fn swap(&self, new_value: Box<T>) -> Box<T> {
        self.swap_nowait(new_value).wait().await
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
unsafe impl<T: Send> Send for Rcu<T> {}
unsafe impl<T: Sync> Sync for Rcu<T> {}
