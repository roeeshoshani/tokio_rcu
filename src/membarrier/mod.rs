trait MemBarrierImpl {
    fn is_supported() -> bool;
    fn register();
    fn perform();
}

cfg_select! {
    target_os = "linux" => {
        mod linux;
        type MemBarrierChosenImpl = linux::MemBarrierImplLinux;
    }
    target_os = "windows" => {
        mod windows;
        type MemBarrierChosenImpl = windows::MemBarrierImplWindows;
    }
    _ => {
        compile_error!(
            "unsupported operating system: the membarrier operation is currently not supported on this operating system"
        );
    }
}

/// checks if the membarrier operation is supported on this machine.
pub fn is_supported() -> bool {
    MemBarrierChosenImpl::is_supported()
}

/// register the current process's intent to use membarriers.
/// should be called once throughout the lifetime of the process, before performing any membarrier operation.
pub fn register() {
    MemBarrierChosenImpl::register();
}

/// perform a membarrier operation.
///
/// from `man membarrier`:
/// Upon return from the system call, the calling thread has a guarantee that all its running thread siblings have passed
/// through a state where all memory accesses to user-space addresses match program order between entry to and return from the
/// system call (non-running threads are de facto in such a state).
pub fn perform() {
    MemBarrierChosenImpl::perform();
}
