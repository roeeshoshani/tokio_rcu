use libc::{
    ENOSYS, MEMBARRIER_CMD_PRIVATE_EXPEDITED, MEMBARRIER_CMD_QUERY,
    MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED, SYS_membarrier, c_int, syscall,
};

fn membarrier_syscall(cmd: c_int, flags: c_int, cpu_id: c_int) -> c_int {
    unsafe { syscall(SYS_membarrier, cmd, flags, cpu_id) as c_int }
}

fn membarrier_syscall_checked(cmd: c_int, flags: c_int, cpu_id: c_int) {
    assert_eq!(membarrier_syscall(cmd, flags, cpu_id), 0);
}

/// checks if the membarrier operation is supported on this machine.
pub fn is_supported() -> bool {
    let res = membarrier_syscall(MEMBARRIER_CMD_QUERY, 0, 0);
    if res == -1 {
        // the syscall does not exist on this kernel.
        let errno = unsafe { *libc::__errno_location() };
        assert_eq!(errno, ENOSYS);
        false
    } else {
        // the syscall exists on this kernel and succeeded.
        // check if all of the required commands are available.
        let required_commands =
            MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED | MEMBARRIER_CMD_PRIVATE_EXPEDITED;
        (res & required_commands) == required_commands
    }
}

/// register the current process's intent to use membarriers.
pub fn register() {
    membarrier_syscall_checked(MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED, 0, 0);
}

/// perform a memory barrier.
pub fn perform() {
    membarrier_syscall_checked(MEMBARRIER_CMD_PRIVATE_EXPEDITED, 0, 0);
}
