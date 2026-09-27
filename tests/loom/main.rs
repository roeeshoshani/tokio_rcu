#![cfg(loom)]

use std::{pin::pin, task::Poll};

use tokio_rcu::rcu_box::RcuBox;

use crate::uaf_detector::UafDetector;

mod uaf_detector;

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

#[test]
fn basic_read_write() {
    #[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
    struct Results {
        saw_id0: bool,
        saw_id1: bool,
    }
    let results = std::sync::Arc::new(std::sync::Mutex::new(Results::default()));
    loom::model({
        let results = results.clone();
        move || {
            let uaf_detector_0 = Box::new(UafDetector::new(0));
            let uaf_detector_1 = Box::new(UafDetector::new(1));

            let state = loom::sync::Arc::new(RcuBox::new(uaf_detector_0));
            let worker1 = loom::thread::spawn({
                let state = state.clone();
                move || {
                    let prev = busy_block_on_future(state.swap(uaf_detector_1));
                    assert_eq!(prev.id(), 0);
                    tokio_rcu::loom_tests_api::on_thread_stop();
                }
            });

            // worker 2
            {
                tokio_rcu::loom_tests_api::on_before_task_poll();
                state.with(|guard| match guard.id() {
                    0 => {
                        results.lock().unwrap().saw_id0 = true;
                    }
                    1 => {
                        results.lock().unwrap().saw_id1 = true;
                    }
                    id => panic!("unexpected guard id: {id}"),
                });
                tokio_rcu::loom_tests_api::on_after_task_poll();
                tokio_rcu::loom_tests_api::on_thread_stop();
            }

            worker1.join().unwrap();
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

#[test]
fn read_and_use_after_quiescent_state_causes_uaf() {
    #[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
    struct Results {
        saw_id0: bool,
        saw_id1: bool,
        saw_uaf: bool,
    }
    let results = std::sync::Arc::new(std::sync::Mutex::new(Results::default()));
    loom::model({
        let results = results.clone();
        move || {
            let uaf_detector_0 = Box::new(UafDetector::new(0));
            let uaf_detector_1 = Box::new(UafDetector::new(1));

            let state = loom::sync::Arc::new(RcuBox::new(uaf_detector_0));
            let worker1 = loom::thread::spawn({
                let state = state.clone();
                move || {
                    let prev = busy_block_on_future(state.swap(uaf_detector_1));
                    assert_eq!(prev.id(), 0);
                    tokio_rcu::loom_tests_api::on_thread_stop();
                }
            });

            // worker 2
            {
                tokio_rcu::loom_tests_api::on_before_task_poll();

                let guard = unsafe { state.read() };

                tokio_rcu::loom_tests_api::on_after_task_poll();

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

                tokio_rcu::loom_tests_api::on_thread_stop();
            }

            worker1.join().unwrap();
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
