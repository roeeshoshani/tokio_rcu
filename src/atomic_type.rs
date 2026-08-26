use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32};

/// given some integer type `T`, returns the corresponding atomic type for it.
/// for example, `Atomic<u32>` is [`AtomicU32`].
/// this allows writing code in a more generic manner.
pub type Atomic<T> = <T as HasAtomicType>::AtomicType;

/// represents an integer type that has a corresponding atomic type.
pub trait HasAtomicType {
    /// the atomic type of this integer type.
    /// for example, for [`u32`], this is [`AtomicU32`].
    type AtomicType;
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
