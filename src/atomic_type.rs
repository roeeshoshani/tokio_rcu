use std::sync::atomic::{AtomicU8, AtomicU32};

pub type Atomic<T> = <T as HasAtomicType>::AtomicType;

pub trait HasAtomicType {
    type AtomicType;
}

macro_rules! impl_has_atomic_type {
    {$int_ty: ty, $atomic_ty: ty} => {
        impl HasAtomicType for $int_ty {
            type AtomicType = $atomic_ty;
        }
    };
}

impl_has_atomic_type! {u32, AtomicU32}
impl_has_atomic_type! {u8, AtomicU8}
