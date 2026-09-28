#![cfg(loom)]

use std::{pin::pin, task::Poll};

use tokio_rcu::rcu_box::RcuBox;

use crate::{loom_waker::LoomWaker, uaf_detector::UafDetector};

mod loom_waker;
mod uaf_detector;

fn busy_block_on_future<F, R>(future: F) -> R
where
    F: Future<Output = R>,
{
    let mut pinned = pin!(future);
    let loom_waker = LoomWaker::new();
    let waker = loom_waker.clone().into();
    let mut context = std::task::Context::from_waker(&waker);
    loop {
        let res_opt = with_before_after_poll(|| {
            if let Poll::Ready(res) = pinned.as_mut().poll(&mut context) {
                Some(res)
            } else {
                None
            }
        });

        if let Some(res) = res_opt {
            return res;
        }

        loom_waker.wait();
    }
}

fn with_before_after_poll<R, F: FnOnce() -> R>(f: F) -> R {
    tokio_rcu::loom_tests_api::on_before_task_poll();
    let res = f();
    tokio_rcu::loom_tests_api::on_after_task_poll();
    res
}

fn with_thread_start_stop<R, F: FnOnce() -> R>(f: F) -> R {
    // note that there isn't really thread start hook in tokio rcu, so we don't need to do anything before calling the function.
    let res = f();
    tokio_rcu::loom_tests_api::on_thread_stop();
    res
}

fn thread_spawn_with_hooks<R: 'static, F: FnOnce() -> R + 'static>(
    f: F,
) -> loom::thread::JoinHandle<R> {
    loom::thread::spawn(move || with_thread_start_stop(f))
}

fn loom_model_with_hooks<F: Fn() + Send + Sync + 'static>(f: F) {
    loom::model(move || with_thread_start_stop(|| f()));
}

/// a basic test where one thread reads the value and one thread swaps it.
#[test]
fn basic_read_write() {
    #[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
    struct Results {
        saw_id0: bool,
        saw_id1: bool,
    }
    let results = std::sync::Arc::new(std::sync::Mutex::new(Results::default()));
    loom_model_with_hooks({
        let results = results.clone();
        move || {
            let uaf_detector_0 = UafDetector::new(0);
            let uaf_detector_1 = UafDetector::new(1);

            let state = loom::sync::Arc::new(RcuBox::new(uaf_detector_0));

            thread_spawn_with_hooks({
                let state = state.clone();
                move || {
                    let prev = busy_block_on_future(state.swap(uaf_detector_1));
                    assert_eq!(prev.id(), 0);
                }
            });

            // main thread logic
            {
                with_before_after_poll(|| {
                    state.with(|guard| match guard.id() {
                        0 => {
                            results.lock().unwrap().saw_id0 = true;
                        }
                        1 => {
                            results.lock().unwrap().saw_id1 = true;
                        }
                        id => panic!("unexpected guard id: {id}"),
                    });
                });
            }
        }
    });

    assert_eq!(
        *results.lock().unwrap(),
        Results {
            saw_id0: true,
            saw_id1: true,
        }
    );
}

/// a test which makes sure that using the guard returned from [`RcuBox::read`] across an await point causes UAF in a controlled and expected manner.
#[test]
fn read_and_use_after_quiescent_state_causes_uaf() {
    #[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
    struct Results {
        saw_id0: bool,
        saw_id1: bool,
        saw_uaf: bool,
    }
    let results = std::sync::Arc::new(std::sync::Mutex::new(Results::default()));
    loom_model_with_hooks({
        let results = results.clone();
        move || {
            let uaf_detector_0 = UafDetector::new(0);
            let uaf_detector_1 = UafDetector::new(1);

            let state = loom::sync::Arc::new(RcuBox::new(uaf_detector_0));

            thread_spawn_with_hooks({
                let state = state.clone();
                move || {
                    let prev = busy_block_on_future(state.swap(uaf_detector_1));
                    assert_eq!(prev.id(), 0);
                }
            });

            // main thread logic
            {
                let guard = with_before_after_poll(|| unsafe { state.read() });
                match guard.try_id() {
                    Some(id) => match id {
                        0 => {
                            results.lock().unwrap().saw_id0 = true;
                        }
                        1 => {
                            results.lock().unwrap().saw_id1 = true;
                        }
                        id => panic!("unexpected guard id: {id:?}"),
                    },
                    None => {
                        results.lock().unwrap().saw_uaf = true;
                    }
                }
            }
        }
    });

    assert_eq!(
        *results.lock().unwrap(),
        Results {
            saw_id0: true,
            saw_id1: true,
            saw_uaf: true
        }
    );
}
