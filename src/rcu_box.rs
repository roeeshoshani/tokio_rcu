//! an rcu primitive which provides a simple rcu-protected heap allocation of shared data (an "rcu box").
//!
//! the main type of this module is [`RcuBox`].

use std::{
    ops::Deref,
    sync::atomic::{self, AtomicPtr},
};

use crate::{
    per_thread_storage::this_thread_does_have_allocated_storage_slot,
    synchronize_rcu,
    utils::{PhantomUnsend, PtrMutSendSync},
};

/// a read guard representing the data stored in an rcu box. this provides a temporary view into the underlying data.
///
/// this guard must not be held across await points, and must not escape the future that acquired it in any way.
///
/// this must manually be taken care of by the programmer. incorrect use will lead to undefined behaviour.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RcuBoxReadGuard<'a, T> {
    value: &'a T,

    /// the guard must not be sent as it is associated with thread local state related to the rcu book-keeping, where we track which
    /// threads can use an old rcu pointer, while assuming that threads don't pass stale pointers between one another.
    _phantom: PhantomUnsend,
}
impl<'a, T> Deref for RcuBoxReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

/// old data of an rcu box, returned after the box's contents were swapped to new ones.
///
/// **this type must not be dropped**, you must call [`wait`](RcuBoxOldData::wait) on it.
/// that's because the old data can't be freed until we know that all previous readers of this box finished using it.
/// dropping this type without waiting means that old readers may still be using the old data, so it can't be freed.
/// so, dropping this type without waiting for it leaks the old data and panics.
pub struct RcuBoxOldData<T> {
    /// the old data pointer, but make it `Send` and `Sync`.
    /// this is safe since this field is basically just a pointer to a heap allocation, and can thus safely be sent and shared between
    /// threads.
    old_data_ptr: PtrMutSendSync<T>,
}
impl<T> RcuBoxOldData<T> {
    /// creates a new old data pointer guard.
    ///
    /// # Safety
    ///
    /// the provided pointer must be the old data pointer of an rcu box, and must have already been swapped with a new one.
    unsafe fn new(old_data_ptr: *mut T) -> Self {
        Self {
            old_data_ptr: unsafe {
                // SAFETY: the old data pointer is basically just a pointer to a heap allocation, and can thus safely be sent and shared between
                // threads.
                PtrMutSendSync::new(old_data_ptr)
            },
        }
    }
    /// wait for all potential existing users of this old data to finish using it, and then return an owned version of the data
    /// once it is guaranteed to no longer be in use by anyone else.
    ///
    /// # cancellation safety
    ///
    /// function is not cancellation safe. if cancelled, it will leak the old data and panic.
    pub async fn wait(self) -> Box<T> {
        // wait for all previous readers to stop using the old value
        synchronize_rcu(true).await;

        // SAFETY: all existing readers finished using this data, so it is now exclusively ours.
        // also, the data pointer is always valid and points to valid data by the invariants of the `RcuBox` type.
        let res = unsafe { Box::from_raw(self.old_data_ptr.ptr()) };

        // this type is not allowed to be dropped, so avoid running its panicking destructor.
        // we are finished doing all cleanup at this point anyway.
        std::mem::forget(self);

        res
    }
}
impl<T> Drop for RcuBoxOldData<T> {
    #[track_caller]
    #[inline]
    fn drop(&mut self) {
        if !std::thread::panicking() {
            // if we are not currently panicking, panic to let the user know that he is doing something wrong.
            panic!(
                "{} can't be dropped since concurrent readers may be using it. it must first be waited for.",
                std::any::type_name::<Self>()
            );
        } else {
            // if we are currently panic don't panic again, since this will cause the process to abort.
            // instead, just leak the value. there's nothing smart for us to do here. we can't free the memory since it may
            // still be used. we must leak it.
        }
    }
}

/// given a tuple of [`RcuBoxOldData`] instances with potentially different data types, this function waits for all of them to be reclaimed
/// at once. this is more efficient than waiting for each of them separately, as it requires only a single rcu grace period for the entire
/// batch, instead of one grace period per [`RcuBoxOldData`] instance.
///
/// the input should be a tuple of the form `(RcuBoxOldData<A>, RcuBoxOldData<B>, ...)`.
/// supports all tuples with length up to 12.
///
/// # example
///
/// ```rust
/// # use tokio_rcu::{rcu_block_on, rcu_box::{RcuBox, rcu_box_wait_multiple}};
/// # rcu_block_on(async {
/// let rcu_a = RcuBox::new(Box::new("a"));
/// let rcu_b = RcuBox::new(Box::new(vec![1, 2, 3]));
/// let rcu_c = RcuBox::new(Box::new(78));
/// let old_data_a = rcu_a.swap_nowait(Box::new("aaaa"));
/// let old_data_b = rcu_b.swap_nowait(Box::new(vec![4, 88, 12, 59, 33]));
/// let old_data_c = rcu_c.swap_nowait(Box::new(9120));
/// let (old_a, old_b, old_c) = rcu_box_wait_multiple((old_data_a, old_data_b, old_data_c)).await;
/// assert_eq!(*old_a, "a");
/// assert_eq!(*old_b, vec![1, 2, 3]);
/// assert_eq!(*old_c, 78);
/// # })
/// ```
///
/// # cancellation safety
///
/// function is not cancellation safe. if cancelled, it will leak the old data and panic.
pub async fn rcu_box_wait_multiple<T: MultipleRcuBoxOldDataInstances>(items: T) -> T::WaitResult {
    items.wait().await
}

/// a trait representing a tuple of multiple [`RcuBoxOldData`] instances, each with its own inner data type.
///
/// this is implemented for all tuples of the form `(RcuBoxOldData<A>, RcuBoxOldData<B>, ...)` with length up to 12.
///
/// used to perform aggregate operations that operate on multiple [`RcuBoxOldData`] instances at once.
pub trait MultipleRcuBoxOldDataInstances {
    /// the result of waiting for all of the rcu old data instances at once.
    /// this is a tuple of the form `(Box<A>, Box<B>, ...)`.
    type WaitResult;

    /// waits for all of the rcu old data instances at once, returning their owned data.
    fn wait(self) -> impl Future<Output = Self::WaitResult>;
}
macro_rules! impl_multiple_rcu_old_data_instances_for_tuple {
    { $(($index: tt, $t: ident)),+ } => {
        impl<$($t),+> MultipleRcuBoxOldDataInstances for ($(RcuBoxOldData<$t>),+) {
            type WaitResult = ($(Box<$t>),+);

            async fn wait(self) -> Self::WaitResult {
                // wait for all previous readers to stop using the old value
                synchronize_rcu(true).await;

                // SAFETY: all existing readers finished using this data, so it is now exclusively ours.
                // also, the data pointers are always valid and point to valid data by the invariants of the `RcuBox` type.
                let results = unsafe {
                    ($(
                        Box::from_raw(self.$index.old_data_ptr.ptr())
                    ),+)
                };

                // this type is not allowed to be dropped, so avoid running its panicking destructor.
                // we are finished doing all cleanup at this point anyway.
                std::mem::forget(self);

                results
            }
        }
    };
}
impl_multiple_rcu_old_data_instances_for_tuple! { (0, A), (1, B) }
impl_multiple_rcu_old_data_instances_for_tuple! { (0, A), (1, B), (2, C) }
impl_multiple_rcu_old_data_instances_for_tuple! { (0, A), (1, B), (2, C), (3, D) }
impl_multiple_rcu_old_data_instances_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E) }
impl_multiple_rcu_old_data_instances_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E), (5, F) }
impl_multiple_rcu_old_data_instances_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G) }
impl_multiple_rcu_old_data_instances_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H) }
impl_multiple_rcu_old_data_instances_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H), (8, I) }
impl_multiple_rcu_old_data_instances_for_tuple! {
    (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H), (8, I), (9, J)
}
impl_multiple_rcu_old_data_instances_for_tuple! {
    (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H), (8, I), (9, J), (10, K)
}
impl_multiple_rcu_old_data_instances_for_tuple! {
    (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H), (8, I), (9, J), (10, K), (11, L)
}

/// an rcu box.
///
/// this is similar to a regular [`Box`], but an rcu box's value can be swapped while readers are simultaneously reading it, without
/// requiring any locks, by relying on the rcu primitive.
pub struct RcuBox<T> {
    value_ptr: AtomicPtr<T>,
}
impl<T> RcuBox<T> {
    /// creates a new rcu box containing the given data.
    pub fn new(value: Box<T>) -> Self {
        Self {
            value_ptr: AtomicPtr::new(Box::leak(value)),
        }
    }

    /// reads the rcu box and provides access to the data it currently contains.
    ///
    /// the usage of the data is limited to the provided closure to prevent it from being used across await points, and to prevent it
    /// from escaping the calling function. this is needed to guarantee correct use of the rcu box.
    ///
    /// # Performance
    ///
    /// this function is very fast and cheap. other than calling the callback (which will probably be inlined into it), it only performs
    /// a single atomic pointer load, plus one regular load of a non-shared thread local variable.
    ///
    /// if you really care about performance, consider using [`with_unchecked`](Self::with_unchecked) or [`read`](Self::read), which are
    /// faster due to skipping some checks, at the cost of being unsafe.
    ///
    /// # Panics
    ///
    /// this function must only be called from a future running inside the tokio runtime, otherwise it will panic.
    #[inline(always)]
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        assert!(
            this_thread_does_have_allocated_storage_slot(),
            "attempted to read an rcu box outside of an rcu-enabled tokio runtime"
        );

        // SAFETY:
        // - we checked that we are inside an rcu-tracked tokio worker thread
        // - the guard only lives throughout the current function, so the future can't yield while holding it.
        // - the guard can't escape since the callback function F is an HRTB, so it can't assume anything about the lifetime of
        //   the provided reference.
        let guard = unsafe { self.read() };

        f(&*guard)
    }

    /// reads the rcu box and provides access to the data it currently contains.
    ///
    /// the usage of the data is limited to the provided closure to prevent it from being used across await points, and to prevent it
    /// from escaping the calling function. this is needed to guarantee correct use of the rcu box.
    ///
    /// # Performance
    ///
    /// this function is very fast and cheap. other than calling the callback (which will probably be inlined into it), it only performs
    /// a single atomic pointer load. that's it.
    ///
    /// # Safety
    ///
    /// this function must only be called from a future running inside the tokio runtime.
    #[inline(always)]
    pub unsafe fn with_unchecked<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        // SAFETY:
        // - caller must guarantee that we are inside an rcu-tracked tokio worker thread
        // - the guard only lives throughout the current function, so the future can't yield while holding it.
        // - the guard can't escape since the callback function F is an HRTB, so it can't assume anything about the lifetime of
        //   the provided reference.
        let guard = unsafe { self.read() };

        f(&*guard)
    }

    /// reads the rcu box, returning a read guard to the data it currently contains.
    ///
    /// # Performance
    ///
    /// this function is very fast and cheap. it only performs a single atomic pointer load. that's it.
    ///
    /// for a safe alternative with a very small amount of added overhead, see [`with`](Self::with).
    ///
    /// # Safety
    ///
    /// this must only be called from a future running inside the tokio runtime.
    ///
    /// the returned guard must not be held across await points, must not be held after the future that acquired it finishes,
    /// and must not escape that future's context (e.g. must not be saved inside a global variable and held across an await point
    /// or longer than the future's lifetime).
    ///
    /// as soon as the future that acquired this read guard gets to a point where it `await`s or finishes execution (basically any
    /// point which voluntarily yields the future), the guard must have already been dropped.
    pub unsafe fn read(&self) -> RcuBoxReadGuard<'_, T> {
        let ptr = self.value_ptr.load(
            // we want acquire ordering to make sure that the write to the pointed-at data happens before the
            // write of the pointer itself, so that when we use the loaded pointer, we are guaranteed to get
            // a valid object.
            atomic::Ordering::Acquire,
        );

        RcuBoxReadGuard {
            // SAFETY: pointers are always valid by the invariants of this type.
            value: unsafe { &*ptr },
            _phantom: PhantomUnsend::new(),
        }
    }

    /// swaps the current value with the new value, and returns a guard containing the old value, which can be owned after waiting
    /// for all previous users of that old value to finish using it.
    pub fn swap_nowait(&self, new_value: Box<T>) -> RcuBoxOldData<T> {
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
        unsafe { RcuBoxOldData::new(old_value_ptr) }
    }

    /// swaps the current value to the new value, waits for all previous users of the old value to finish using it, and returns an
    /// owned version of the old value.
    ///
    /// # cancellation safety
    ///
    /// function is not cancellation safe. if cancelled, it will leak the old value and panic.
    pub async fn swap(&self, new_value: Box<T>) -> Box<T> {
        self.swap_nowait(new_value).wait().await
    }
}
impl<T: Clone> RcuBox<T> {
    /// reads the rcu box and clones the value that it currently contains.
    pub fn read_clone(&self) -> T {
        self.with(|x| x.clone())
    }
}
impl<T> Drop for RcuBox<T> {
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

/// sending an rcu box to another thread sends the owned inner `T` data to that thread, so it requires `T: Send`.
unsafe impl<T: Send> Send for RcuBox<T> {}

/// sending a `&RcuBox<T>` to another thread allows that thread to both read the inner `T` data as a `&T`, and also to get full ownership
/// over the current `T` value by swapping it with a new value.
/// so, it requires both sharing `&T` instances with other threads, which requires `T: Sync`, and it also requires being able to send
/// owned `T` values to other threads (due to [`swap`](RcuBox::swap) related functions), which requires `T: Send`.
unsafe impl<T: Send + Sync> Sync for RcuBox<T> {}
