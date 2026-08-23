#[cfg(loom)]
use std::mem::MaybeUninit;

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

    // loom's atomic `new` function is not const and can't be used in const contexts, but throughout this crate, we very often
    // need to use the atomic `new` function in const contexts, for example for initializing static variables of an atomic int
    // type.
    // we could use a `OnceLock`, but the oncelock will synchronize accesses to this variable due to its own internal
    // synchronization, which will prevent loom from catching any problematic accesses.
    // so, we instead rely on the user to unsafeuly guarantee initialization of this variable at the start of execution,
    // and then all accesses are not synchronized in any way.
    //
    // we specifically use std's `UnsafeCell` and not loom's since loom's `UnsafeCell` has the same problem as loom's atomic:
    // its constructor is not const. we also don't need loom's `UnsafeCell` here, since the cell is only used for the initial
    // write which initializes the variable at the start of the program. the rest of the accesses during the actual runtime
    // only use immutable references to this value, since the atomic already implements its own interior mutability.
    #[cfg(loom)]
    inner: std::cell::UnsafeCell<MaybeUninit<RawAtomic<T>>>,
    #[cfg(loom)]
    initial_value: T,
}
impl<T: HasAtomicType> Atomic<T> {
    /// provides access to the underlying atomic variable, assuming that it was already initialized.
    ///
    /// # safety
    ///
    /// this function is unsafe. it is not marked unsafe to spare the boilerplate, since it is used basically
    /// everywhere.
    ///
    /// this function must be called only after the atomic variable it initialized.
    fn access(&self) -> &RawAtomic<T> {
        #[cfg(not(loom))]
        {
            &self.inner
        }
        #[cfg(loom)]
        {
            // SAFETY: must be initialized at this point according to safety contract of this type and of this function.
            let inner = unsafe { &*self.inner.get() };
            unsafe { inner.assume_init_ref() }
        }
    }
}
impl<T: HasAtomicType> std::fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.access(), f)
    }
}

// SAFETY: only accessed immutably after initialization, and only contains a pure atomic value.
unsafe impl<T: HasAtomicType> Sync for Atomic<T> {}

macro_rules! impl_atomic_type {
    {$int_ty: ty} => {
        #[allow(unused)]
        impl Atomic<$int_ty> {
            /// creates a new atomic variable with the given initial value.
            ///
            /// # safety
            ///
            /// when `cfg(loom)` is enabled, the atomic must be initialized before any use.
            /// the unsafety is marked here, in the constructor, instead of in every single access function, to reduce
            /// boilerplate when using it.
            ///
            /// but, for preserving safety, you must first initialize this atomic variable by calling [`init`](Self::init)
            /// before performing any other access to it.
            ///
            /// when `cfg(loom)` is disabled, this function is perfectly safe, but is still makred unsafe to preserve coherence
            /// of the code using it, so that wrapping it in an unsafe block doesn't become conditional depending on cfg.
            pub const unsafe fn new(initial_value: $int_ty) -> Self {
                #[cfg(not(loom))]
                {
                    Self {
                        inner: RawAtomic::<$int_ty>::new(initial_value),
                    }
                }
                #[cfg(loom)]
                {
                    Self {
                        inner: std::cell::UnsafeCell::new(MaybeUninit::uninit()),
                        initial_value,
                    }
                }
            }

            /// initializes this atomic variable. this must be performed before performing any other access to this
            /// atomic variable.
            ///
            /// # safety
            ///
            /// - must be called before any other access to this variable.
            /// - must only be called exactly once.
            /// - must be called before spawning any threads in the program.
            #[cfg(loom)]
            pub unsafe fn init(&self) {
                let inner = unsafe { &mut *self.inner.get() };
                inner.write(RawAtomic::<$int_ty>::new(self.initial_value));
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
