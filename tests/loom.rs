#![cfg(loom)]

use std::{pin::pin, task::Poll};

use loom::sync::Arc;
use tokio_rcu::rcu_box::RcuBox;

fn busy_block_on_future<F, R>(future: F) -> R
where
    F: Future<Output = R>,
{
    let mut pinned = pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        tokio_rcu::loom_tests_api::on_before_task_poll();

        if let Poll::Ready(res) = pinned.as_mut().poll(&mut context) {
            return res;
        }

        tokio_rcu::loom_tests_api::on_after_task_poll();

        loom::thread::yield_now();
    }
}

/// spawn a loom thread with a big stack.
/// our logic uses a lot of stack space, and loom's main thread stack is very small, which leads to a stack overflow.
/// so, we run most of our heavy logic inside loom threads with big stacks.
fn loom_spawn<F, T>(f: F) -> loom::thread::JoinHandle<T>
where
    F: Send + 'static + FnOnce() -> T,
    T: Send + 'static,
{
    loom::thread::Builder::new()
        .stack_size(1024 * 1024)
        .spawn(f)
        .unwrap()
}

#[test]
fn no_uaf_basic() {
    loom::model(|| {
        let state = Arc::new(RcuBox::new(Box::new(String::from("initial"))));
        let worker1 = loom_spawn({
            let state = state.clone();
            move || {
                let prev = busy_block_on_future(state.swap(Box::new(String::from("new"))));
                assert_eq!(*prev, "initial");
                tokio_rcu::loom_tests_api::on_thread_stop();
            }
        });
        let worker2 = loom_spawn({
            let state = state.clone();
            move || {
                tokio_rcu::loom_tests_api::on_before_task_poll();
                state.with(|guard| {
                    assert!(*guard == "initial" || *guard == "new");
                });
                tokio_rcu::loom_tests_api::on_after_task_poll();
                tokio_rcu::loom_tests_api::on_thread_stop();
            }
        });

        worker1.join().unwrap();
        worker2.join().unwrap();
    })
}
