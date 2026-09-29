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

        loom_waker.wait_with_hooks();
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

/// a test where one thread reads the value and one thread swaps it.
///
/// this test exercises most of the main flows of the rcu book-keeping logic.
/// it covers both the just-waking thread case, the just-starting thread case, and the already-running thread case.
#[test]
fn read_and_write() {
    #[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
    struct Results {
        saw_id0: bool,
        saw_id1: bool,
    }
    let results = std::sync::Arc::new(parking_lot::Mutex::new(Results::default()));
    loom::model({
        let results = results.clone();
        move || {
            let (uaf_detector0, uaf_detector0_key) = UafDetector::new(0);
            let uaf_detector0_ptr = Box::as_ptr(&uaf_detector0);

            let (uaf_detector1, uaf_detector1_key) = UafDetector::new(1);
            let uaf_detector1_ptr = Box::as_ptr(&uaf_detector1);

            let state = loom::sync::Arc::new(RcuBox::new(uaf_detector0));

            let writer = thread_spawn_with_hooks({
                let state = state.clone();
                move || {
                    let prev = busy_block_on_future(state.swap(uaf_detector1));
                    assert_eq!(prev.id(uaf_detector0_key), 0);
                }
            });

            let reader = thread_spawn_with_hooks({
                let state = state.clone();
                let results = results.clone();
                move || {
                    let check_guard_logic = |guard: &UafDetector| {
                        let guard_ptr = guard as *const UafDetector;
                        if guard_ptr == uaf_detector0_ptr {
                            let id = guard.id(uaf_detector0_key);
                            assert_eq!(id, 0);
                            results.lock().saw_id0 = true;
                            id
                        } else if guard_ptr == uaf_detector1_ptr {
                            let id = guard.id(uaf_detector1_key);
                            assert_eq!(id, 1);
                            results.lock().saw_id1 = true;
                            id
                        } else {
                            panic!("unexpected ptr");
                        }
                    };

                    let first_seen_id = with_before_after_poll(|| state.with(check_guard_logic));

                    // emulate this thread going to sleep and waking up from it.
                    // this is used to exercise the just-waking thread path.
                    tokio_rcu::loom_tests_api::on_thread_park();
                    tokio_rcu::loom_tests_api::on_thread_unpark();

                    // re-poll after waking from sleep.
                    let second_seen_id = with_before_after_poll(|| state.with(check_guard_logic));

                    // the id we see later must be greater than or equal the id we saw first, otherwise we see
                    // the writes happening in reverse, which should never happen.
                    assert!(second_seen_id >= first_seen_id);
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();
        }
    });

    assert_eq!(
        *results.lock(),
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
    let results = std::sync::Arc::new(parking_lot::Mutex::new(Results::default()));
    loom::model({
        let results = results.clone();
        move || {
            let (uaf_detector0, uaf_detector0_key) = UafDetector::new(0);
            let uaf_detector0_ptr = Box::as_ptr(&uaf_detector0);

            let (uaf_detector1, uaf_detector1_key) = UafDetector::new(1);
            let uaf_detector1_ptr = Box::as_ptr(&uaf_detector1);

            let state = loom::sync::Arc::new(RcuBox::new(uaf_detector0));

            let writer = thread_spawn_with_hooks({
                let state = state.clone();
                move || {
                    let prev = busy_block_on_future(state.swap(uaf_detector1));
                    assert_eq!(prev.id(uaf_detector0_key), 0);
                }
            });

            let reader = thread_spawn_with_hooks({
                let state = state.clone();
                let results = results.clone();
                move || {
                    let guard = with_before_after_poll(|| unsafe { state.read() });
                    let guard_ref: &UafDetector = &*guard;
                    let guard_ptr = guard_ref as *const UafDetector;
                    if guard_ptr == uaf_detector0_ptr {
                        // the first UAF detector may actually be in a UAF situation.
                        match guard.try_id(uaf_detector0_key) {
                            Some(id) => {
                                assert_eq!(id, 0);
                                results.lock().saw_id0 = true;
                            }
                            None => {
                                results.lock().saw_uaf = true;
                            }
                        }
                    } else if guard_ptr == uaf_detector1_ptr {
                        // the second UAF detector can't be UAF'd.
                        assert_eq!(guard.id(uaf_detector1_key), 1);
                        results.lock().saw_id1 = true;
                    } else {
                        panic!("unexpected ptr");
                    }
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();
        }
    });

    assert_eq!(
        *results.lock(),
        Results {
            saw_id0: true,
            saw_id1: true,
            saw_uaf: true
        }
    );
}
