use std::marker::PhantomData;

/// a phantom type which is not `Send` and also not `Sync`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct PhantomUnsendUnsync {
    phantom: PhantomData<*const ()>,
}
impl PhantomUnsendUnsync {
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
    pub unsafe fn new(ptr: *mut T) -> Self {
        Self { ptr }
    }

    /// returns the underlying pointer.
    pub fn ptr(&self) -> *mut T {
        self.ptr
    }
}
unsafe impl<T: Send> Send for PtrMutSendSync<T> {}
unsafe impl<T: Sync> Sync for PtrMutSendSync<T> {}
