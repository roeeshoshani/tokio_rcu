//! an rcu primitive which provides a simple rcu-protected heap allocation of shared data (an "rcu box").
//!
//! the main type of this module is [`RcuBox`].

use std::ops::Deref;

use crate::{
    loom::std::sync::atomic::{self, AtomicPtr},
    rcu_core::RcuReadLockGuard,
    synchronize_rcu,
    utils::{PhantomUnsend, PtrMutSendSync},
};

/// a read guard representing the data stored in an rcu box. this provides a temporary view into the underlying data.
///
/// this guard is returned from [`RcuBox::read`].
///
/// the semantics of [`RcuBox::read`] enforce the correct usage of this read guard, e.g. it makes sure you can't hold it across await points.
/// it does so by binding the lifetime of this guard to the lifetime of the [`RcuReadLockGuard`] that was provided when the data was read.
///
/// this guard cannot be sent between threads (it is [`!Send`](Send)), since that would allow one to send a guard acquired on an rcu tracked
/// thread to a non rcu tracked thread, thus violating the safety contract of the read side of the rcu algorithm.
///
/// but, this guard can be shared between threads (it is [`Sync`]), since as long as the guard lives, you can freely share the contained value with
/// other threads, even non rcu tracked ones.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RcuBoxReadGuard<'a, T> {
    value: &'a T,

    /// the guard must not be sent as it is associated with thread local state related to the rcu book-keeping, where we track which
    /// threads can use an old rcu pointer, while assuming that threads don't pass stale pointers between one another.
    ///
    /// but, note that this guard can be shared between threads, so it is [`Sync`].
    /// a thread can acquire a read guard and then temporarily share the data with another thread, as long as it is still holding the read guard,
    /// which keeps the data alive.
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
        synchronize_rcu().await;

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
/// # use tokio_rcu::{rcu_block_on, primitives::rcu_box::{RcuBox, rcu_box_wait_multiple}};
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
                synchronize_rcu().await;

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
            value_ptr: AtomicPtr::new(Box::into_raw(value)),
        }
    }

    /// reads the rcu box, returning a read guard to the data it currently contains.
    ///
    /// the lifetime of the returned guard is bound to the lifetime of the provided [`RcuReadLockGuard`], which ensures correct use of the
    /// returned guard.
    ///
    /// # Performance
    ///
    /// this function is very fast and cheap. it only performs a single atomic pointer load. that's it.
    pub fn read<'a>(&'a self, guard: &'a RcuReadLockGuard) -> RcuBoxReadGuard<'a, T> {
        let ptr = self.value_ptr.load(
            // we want acquire ordering to make sure that the write to the pointed-at data happens before the
            // write of the pointer itself, so that when we use the loaded pointer, we are guaranteed to get
            // a valid object.
            atomic::Ordering::Acquire,
        );

        // we don't really use the guard, we just need it as proof that the caller is holding an rcu read lock, and to bind the lifetime of the
        // returned guard to the lifetime of the rcu read lock, to prevent it from being used outside of the critical section.
        let _ = guard;

        RcuBoxReadGuard {
            // SAFETY: pointers are always valid by the invariants of this type.
            value: unsafe { &*ptr },
            _phantom: PhantomUnsend::new(),
        }
    }

    /// swaps the current value with the new value, and returns a guard containing the old value, which can be owned after waiting
    /// for all previous users of that old value to finish using it.
    pub fn swap_nowait(&self, new_value: Box<T>) -> RcuBoxOldData<T> {
        let new_value_ptr = Box::into_raw(new_value);

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
impl<T> Drop for RcuBox<T> {
    fn drop(&mut self) {
        let ptr = self.value_ptr.load(
            // we want acquire ordering to make sure that the write to the pointed-at data happens before the
            // write of the pointer itself, so that when we use the loaded pointer, we are guaranteed to get
            // a valid object.
            atomic::Ordering::Acquire,
        );

        // SAFETY: pointers are always valid by the invariants of this type.
        // furthermore, at this point we have a mutable reference to self, so no concurrent read guards could exist, and we
        // have full ownership over the contained data, so we can safely free it without any grace period.
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
