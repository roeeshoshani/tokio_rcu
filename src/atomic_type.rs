use crate::loom::{
    fn_const_if_not_loom,
    std::sync::atomic::{self, AtomicU8, AtomicU16, AtomicU32},
};

/// represents an integer type that has a corresponding atomic type.
pub trait HasAtomicType {
    /// the atomic type of this integer type.
    /// for example, for [`u32`], this is [`AtomicU32`].
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

/// given some integer type `T`, returns the corresponding atomic type for it.
/// for example, `Atomic<u32>` is [`AtomicU32`].
/// this allows writing code in a more generic manner.
pub type RawAtomic<T> = <T as HasAtomicType>::AtomicType;

/// a generic wrapper around atomic integer types which provides an abstraction over the std and loom interfaces, specifically
/// tailored to the use of atomics in this crate.
pub struct Atomic<T: HasAtomicType> {
    inner: RawAtomic<T>,
}
impl<T: HasAtomicType> std::fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

// SAFETY: only accessed immutably after initialization, and only contains a pure atomic value.
unsafe impl<T: HasAtomicType> Sync for Atomic<T> {}

macro_rules! impl_atomic_type {
    {$int_ty: ty} => {
        #[allow(unused)]
        impl Atomic<$int_ty> {
            fn_const_if_not_loom! {
                /// creates a new atomic variable with the given initial value.
                pub const fn new(initial_value: $int_ty) -> Self {
                    Self {
                        inner: RawAtomic::<$int_ty>::new(initial_value),
                    }
                }
            }

            /// see [`AtomicUsize::load`].
            pub fn load(&self, ordering: atomic::Ordering) -> $int_ty {
                self.inner.load(ordering)
            }

            /// see [`AtomicUsize::store`].
            pub fn store(&self, new_value: $int_ty, ordering: atomic::Ordering){
                self.inner.store(new_value, ordering)
            }

            /// see [`AtomicUsize::try_update`].
            pub fn try_update(
                &self,
                set_order: atomic::Ordering,
                fetch_order: atomic::Ordering,
                f: impl FnMut($int_ty) -> Option<$int_ty>,
            ) -> Result<$int_ty, $int_ty> {
                #[cfg(not(loom))]
                {
                    self.inner.try_update(set_order, fetch_order, f)
                }
                #[cfg(loom)]
                {
                    self.inner.fetch_update(set_order, fetch_order, f)
                }
            }

            /// see [`AtomicUsize::fetch_and`].
            pub fn fetch_and(&self, val: $int_ty, order: atomic::Ordering) -> $int_ty {
                self.inner.fetch_and(val, order)
            }

            /// see [`AtomicUsize::fetch_or`].
            pub fn fetch_or(&self, val: $int_ty, order: atomic::Ordering) -> $int_ty {
                self.inner.fetch_or(val, order)
            }

            /// see [`AtomicUsize::compare_exchange`].
            pub fn compare_exchange(&self,
                current: $int_ty,
                new: $int_ty,
                success: atomic::Ordering,
                failure: atomic::Ordering
            ) -> Result<$int_ty, $int_ty> {
                self.inner.compare_exchange(current, new, success, failure)
            }
        }
    };
}
impl_atomic_type! {u8}
impl_atomic_type! {u16}
impl_atomic_type! {u32}
