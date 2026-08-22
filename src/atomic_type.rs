#[cfg(loom)]
use std::sync::OnceLock;

use crate::loom_or_std::sync::atomic::{self, AtomicU8, AtomicU16, AtomicU32};

pub trait HasAtomicType {
    type AtomicType: std::fmt::Debug;
}

macro_rules! impl_has_atomic_type {
    {$int_ty: ty, $atomic_ty: ty} => {
        impl HasAtomicType for $int_ty {
            type AtomicType = $atomic_ty;
        }
    };
}

impl_has_atomic_type! {u8, AtomicU8}
impl_has_atomic_type! {u16, AtomicU16}
impl_has_atomic_type! {u32, AtomicU32}

pub type RawAtomic<T> = <T as HasAtomicType>::AtomicType;

/// a generic wrapper around atomic integer types which provides an abstraction over the std and loom interfaces, specifically
/// tailored to the use of atomics in this crate.
pub struct Atomic<T: HasAtomicType> {
    #[cfg(not(loom))]
    inner: RawAtomic<T>,

    #[cfg(loom)]
    inner: OnceLock<RawAtomic<T>>,
    #[cfg(loom)]
    initial_value: T,
}
impl<T: HasAtomicType> std::fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(not(loom))]
        {
            std::fmt::Debug::fmt(&self.inner, f)
        }
        #[cfg(loom)]
        {
            match self.inner.get() {
                Some(inner) => std::fmt::Debug::fmt(inner, f),
                None => f.write_str("<uninitialized>"),
            }
        }
    }
}

macro_rules! impl_atomic_type {
    {$int_ty: ty} => {
        impl Atomic<$int_ty> {
            pub const fn new(initial_value: $int_ty) -> Self {
                #[cfg(not(loom))]
                {
                    Self {
                        inner: RawAtomic::<$int_ty>::new(initial_value),
                    }
                }
                #[cfg(loom)]
                {
                    Self {
                        inner: OnceLock::new(),
                        initial_value,
                    }
                }
            }

            fn access(&self) -> &RawAtomic<$int_ty> {
                #[cfg(not(loom))]
                {
                    &self.inner
                }
                #[cfg(loom)]
                {
                    self.inner
                        .get_or_init(|| RawAtomic::<$int_ty>::new(self.initial_value))
                }
            }

            pub fn load(&self, ordering: atomic::Ordering) -> $int_ty {
                self.access().load(ordering)
            }

            pub fn store(&self, new_value: $int_ty, ordering: atomic::Ordering){
                self.access().store(new_value, ordering)
            }

            pub fn try_update(
                &self,
                set_order: atomic::Ordering,
                fetch_order: atomic::Ordering,
                f: impl FnMut($int_ty) -> Option<$int_ty>,
            ) -> Result<$int_ty, $int_ty> {
                #[cfg(not(loom))]
                {
                    self.access().try_update(set_order, fetch_order, f)
                }
                #[cfg(loom)]
                {
                    self.access().fetch_update(set_order, fetch_order, f)
                }
            }

            pub fn fetch_and(&self, val: $int_ty, order: atomic::Ordering) -> $int_ty {
                self.access().fetch_and(val, order)
            }

            pub fn fetch_or(&self, val: $int_ty, order: atomic::Ordering) -> $int_ty {
                self.access().fetch_or(val, order)
            }

            pub fn compare_exchange(&self,
                current: $int_ty,
                new: $int_ty,
                success: atomic::Ordering,
                failure: atomic::Ordering
            ) -> Result<$int_ty, $int_ty> {
                self.access().compare_exchange(current, new, success, failure)
            }
        }
    };
}
impl_atomic_type! {u8}
impl_atomic_type! {u16}
impl_atomic_type! {u32}
