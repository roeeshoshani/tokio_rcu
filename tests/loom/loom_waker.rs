pub struct LoomWaker(loom::sync::Notify);
impl LoomWaker {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self(loom::sync::Notify::new()))
    }
    pub fn wait_with_hooks(&self) {
        tokio_rcu::loom_tests_api::on_thread_park();
        self.0.wait();
        tokio_rcu::loom_tests_api::on_thread_unpark();
    }
}
impl std::task::Wake for LoomWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.notify();
    }
}
