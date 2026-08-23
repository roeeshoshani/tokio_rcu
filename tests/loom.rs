#![cfg(loom)]

use std::{pin::pin, task::Poll};

use loom::sync::Arc;
use tokio_rcu::Rcu;

fn busy_block_on_future<F, R>(future: F) -> R
where
    F: Future<Output = R>,
{
    let mut pinned = pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let Poll::Ready(res) = pinned.as_mut().poll(&mut context) {
            return res;
        }
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
    tokio_rcu::loom_tests_api::initialize();

    loom::model(|| {
        let state = Arc::new(Rcu::new(String::from("initial")));
        let worker1 = loom_spawn({
            let state = state.clone();
            move || {
                tokio_rcu::loom_tests_api::on_thread_start();
                let prev = busy_block_on_future(state.swap(String::from("new")));
                assert_eq!(prev, "initial");
                tokio_rcu::loom_tests_api::on_thread_stop();
            }
        });
        let worker2 = loom_spawn({
            let state = state.clone();
            move || {
                tokio_rcu::loom_tests_api::on_thread_start();
                let guard = state.read();
                assert!(*guard == "initial" || *guard == "new");
                tokio_rcu::loom_tests_api::on_thread_stop();
            }
        });

        worker1.join().unwrap();
        worker2.join().unwrap();
    })
}
