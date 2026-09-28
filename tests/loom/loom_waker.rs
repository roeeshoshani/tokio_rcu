pub struct LoomWaker(loom::sync::Notify);
impl LoomWaker {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self(loom::sync::Notify::new()))
    }
    pub fn wait(&self) {
        self.0.wait();
    }
}
impl std::task::Wake for LoomWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.notify();
    }
}
