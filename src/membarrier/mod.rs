/// an implementation of the membarrier primitive for some platform.
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
///
/// must be called before performing any membarrier operation.
///
/// can't be called more than once throughout the lifetime of the process.
/// calling it more than once will result in unspecified behaviour.
pub fn register() {
    MemBarrierChosenImpl::register();
}

/// perform a membarrier operation, synchronizing all threads in the current process.
///
/// from `man membarrier`:
/// Upon return from the system call, the calling thread has a guarantee that all its running thread siblings have passed
/// through a state where all memory accesses to user-space addresses match program order between entry to and return from the
/// system call (non-running threads are de facto in such a state).
pub fn perform() {
    MemBarrierChosenImpl::perform();
}
