use std::{
    ops::Deref,
    sync::atomic::{self, AtomicPtr},
};

use crate::{
    synchronize_rcu,
    utils::{PhantomUnsendUnsync, PtrMutSendSync},
};

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

    pub async fn swap(&self, new_value: T) -> T {
        let new_value_ptr = Box::leak(Box::new(new_value));

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

        // make the pointer `Send` and `Sync` if the `T` is `Send` and `Sync` so that we can hold it across await points without
        // limiting the future to being non send or non sync.
        //
        // SAFETY: the pointer is just a pointer to a heap allocated object, which we will fully own once the synchronize rcu
        // call finishes.
        let old_value_ptr = unsafe { PtrMutSendSync::new(old_value_ptr) };

        // wait for all previous readers to stop using the old value
        synchronize_rcu().await;

        // SAFETY: pointers are always valid by the invariants of this type.
        let boxed_value = unsafe { Box::from_raw(old_value_ptr.ptr()) };

        *boxed_value
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
