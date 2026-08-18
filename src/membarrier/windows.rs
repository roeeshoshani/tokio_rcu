use crate::membarrier::MemBarrierImpl;

pub struct MemBarrierImplWindows;
impl MemBarrierImpl for MemBarrierImplWindows {
    fn is_supported() -> bool {
        true
    }

    fn register() {}

    fn perform() {
        unsafe {
            windows::Win32::System::Threading::FlushProcessWriteBuffers();
        }
    }
}
