use std::marker::PhantomData;

pub mod atomic_type;

/// a phantom type which is not `Send`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct PhantomUnsend {
    phantom: PhantomData<*const ()>,
}
impl PhantomUnsend {
    /// creates a new [`PhantomUnsend`] object.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}
unsafe impl Sync for PhantomUnsend {}

/// a phantom type which is not `Send` and not `Sync`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct PhantomUnsendUnsync {
    phantom: PhantomData<*const ()>,
}
impl PhantomUnsendUnsync {
    /// creates a new [`PhantomUnsendUnsync`] object.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

/// a wrapper around a `*mut T` which makes it `Send` and `Sync` if `T` is `Send` and `Sync`.
pub struct PtrMutSendSync<T> {
    ptr: *mut T,
}
impl<T> PtrMutSendSync<T> {
    /// creates a new wrapper around the given pointer.
    ///
    /// # Safety
    ///
    /// any `Send` or `Sync` operation performed on the wrapped pointer must be safe according to the semantics of the underlying
    /// pointer.
    #[inline(always)]
    pub unsafe fn new(ptr: *mut T) -> Self {
        Self { ptr }
    }

    /// returns the underlying pointer.
    #[inline(always)]
    pub fn ptr(&self) -> *mut T {
        self.ptr
    }
}
unsafe impl<T: Send> Send for PtrMutSendSync<T> {}
unsafe impl<T: Sync> Sync for PtrMutSendSync<T> {}

/// a cold and empty function used to mark cold paths in code.
#[cold]
#[inline(always)]
const fn cold_and_empty() {}

/// given a condition, returns that same condition, but with a hint to the compiler that the condition is most likely true.
#[inline(always)]
pub const fn likely(cond: bool) -> bool {
    if !cond {
        cold_and_empty();
    }
    cond
}

/// given a condition, returns that same condition, but with a hint to the compiler that the condition is most likely false.
#[inline(always)]
pub const fn unlikely(cond: bool) -> bool {
    if cond {
        cold_and_empty();
    }
    cond
}
