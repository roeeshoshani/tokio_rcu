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
