#![cfg(loom)]

use std::{pin::pin, task::Poll};

use tokio_rcu::{RcuReadLockGuard, rcu_box::RcuBox, rcu_read_lock, synchronize_rcu};

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
                            // SAFETY: guard points to a heap allocation
                            let id = unsafe { guard.id_byref(uaf_detector0_key) };
                            assert_eq!(id, 0);
                            results.lock().saw_id0 = true;
                            id
                        } else if guard_ptr == uaf_detector1_ptr {
                            // SAFETY: guard points to a heap allocation
                            let id = unsafe { guard.id_byref(uaf_detector1_key) };
                            assert_eq!(id, 1);
                            results.lock().saw_id1 = true;
                            id
                        } else {
                            panic!("unexpected ptr");
                        }
                    };

                    let first_seen_id = with_before_after_poll(|| {
                        rcu_read_lock(|rcu_read_lock_guard| {
                            check_guard_logic(&*state.read(rcu_read_lock_guard))
                        })
                    });

                    // emulate this thread going to sleep and waking up from it.
                    // this is used to exercise the just-waking thread path.
                    tokio_rcu::loom_tests_api::on_thread_park();
                    tokio_rcu::loom_tests_api::on_thread_unpark();

                    // re-poll after waking from sleep.
                    let second_seen_id = with_before_after_poll(|| {
                        rcu_read_lock(|rcu_read_lock_guard| {
                            check_guard_logic(&*state.read(rcu_read_lock_guard))
                        })
                    });

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

/// make sure that [`synchronize_rcu`] properly wakes up when the other thread goes to sleep.
#[test]
fn rcu_synchronize_wakes_up_on_thread_park() {
    loom::model(move || {
        let reader_ready_notify = std::sync::Arc::new(loom::sync::Notify::new());
        let writer_done_notify = std::sync::Arc::new(loom::sync::Notify::new());
        let writer = thread_spawn_with_hooks({
            let reader_ready_notify = reader_ready_notify.clone();
            let writer_done_notify = writer_done_notify.clone();
            move || {
                reader_ready_notify.wait();
                busy_block_on_future(synchronize_rcu());
                writer_done_notify.notify();
            }
        });

        let reader = thread_spawn_with_hooks({
            let reader_ready_notify = reader_ready_notify.clone();
            let writer_done_notify = writer_done_notify.clone();
            move || {
                // run the task polling hooks just to register ourselves as an active worker thread in the rcu book-keeping.
                with_before_after_poll(|| {});

                // tell the writer that he can start his grace period
                reader_ready_notify.notify();

                // emulate this thread going to sleep
                tokio_rcu::loom_tests_api::on_thread_park();

                // make sure that the synchronize rcu operation finishes even when we remain parked.
                // this check that the synchronize rcu operation properly detects that this thread parked and can thus be
                // ignored.
                writer_done_notify.wait();

                // don't forget to call the proper unpark hook
                tokio_rcu::loom_tests_api::on_thread_unpark();
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}

/// make sure that [`synchronize_rcu`] properly wakes up when the other thread passes through the after task poll hook.
#[test]
fn rcu_synchronize_wakes_up_on_after_poll_hook_call() {
    loom::model(move || {
        let is_writer_done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let writer = thread_spawn_with_hooks({
            let is_writer_done = is_writer_done.clone();
            move || {
                busy_block_on_future(synchronize_rcu());
                is_writer_done.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        });

        let reader = thread_spawn_with_hooks(move || {
            // register this thread
            tokio_rcu::loom_tests_api::on_before_task_poll();

            // wait for the writer to actually start its wait operation, otherwise we may just finish before he even starts,
            // in which case he will block forever.
            //
            // after this wait, the writer may or may not have already started blocking, so it makes sure that we can
            // properly wake him up in case he did.
            while tokio_rcu::loom_tests_api::epoch_id_get(std::sync::atomic::Ordering::Relaxed)
                == tokio_rcu::loom_tests_api::EPOCH_ID_MIN + 2
            {
                loom::thread::yield_now();
            }

            // make sure that the synchronize rcu operation can finish even if all we do is poll tasks and never
            // park.
            tokio_rcu::loom_tests_api::on_after_task_poll();
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}

/// make sure that [`synchronize_rcu`] properly wakes up when a thread unparks.
#[test]
fn rcu_synchronize_wakes_up_on_thread_unpark_hook_call() {
    loom::model(move || {
        let reader_ready_notify = std::sync::Arc::new(loom::sync::Notify::new());
        let writer_done_notify = std::sync::Arc::new(loom::sync::Notify::new());
        let writer = thread_spawn_with_hooks({
            let reader_ready_notify = reader_ready_notify.clone();
            let writer_done_notify = writer_done_notify.clone();
            move || {
                reader_ready_notify.wait();
                busy_block_on_future(synchronize_rcu());
                writer_done_notify.notify();
            }
        });

        let reader = thread_spawn_with_hooks({
            let reader_ready_notify = reader_ready_notify.clone();
            let writer_done_notify = writer_done_notify.clone();
            move || {
                // run the task polling hooks just to register ourselves as an active worker thread in the rcu book-keeping.
                with_before_after_poll(|| {});

                // emulate this thread going to sleep
                tokio_rcu::loom_tests_api::on_thread_park();

                // tell the writer that he can start his grace period
                reader_ready_notify.notify();

                // wait for the writer to actually start its wait operation, otherwise we may just finish before he even starts,
                // in which case he will block forever.
                //
                // after this wait, the writer may or may not have already started blocking, so it makes sure that we can
                // properly wake him up in case he did.
                while tokio_rcu::loom_tests_api::epoch_id_get(std::sync::atomic::Ordering::Relaxed)
                    == tokio_rcu::loom_tests_api::EPOCH_ID_MIN + 2
                {
                    loom::thread::yield_now();
                }

                // call unpark while the writer is waiting for us, and make sure that in all cases he still wakes up properly.
                tokio_rcu::loom_tests_api::on_thread_unpark();

                writer_done_notify.wait();
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
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
                    let rcu_read_lock_guard = unsafe { RcuReadLockGuard::new() };
                    let guard = with_before_after_poll(|| state.read(&rcu_read_lock_guard));
                    let guard_ref: &UafDetector = &*guard;
                    let guard_ptr = guard_ref as *const UafDetector;
                    if guard_ptr == uaf_detector0_ptr {
                        // the first UAF detector may actually be in a UAF situation.
                        // SAFETY: guard points to a heap allocation
                        match unsafe { guard.try_id_byref(uaf_detector0_key) } {
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
                        // SAFETY: guard points to a heap allocation
                        assert_eq!(unsafe { guard.id_byref(uaf_detector1_key) }, 1);
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

/// a test where one thread reads the value and one thread swaps it, and then the writer swaps the value, he immediately
/// reaches the reset epoch id and starts performing a reset operation.
///
/// this test covers both the just-waking thread case, the just-starting thread case, and the already-running thread case,
/// all during a reset operation.
#[test]
fn read_and_write_with_reset() {
    #[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
    struct Results {
        saw_id0: bool,
        saw_id1: bool,
    }
    let results = std::sync::Arc::new(parking_lot::Mutex::new(Results::default()));
    loom::model({
        let results = results.clone();
        move || {
            tokio_rcu::loom_tests_api::epoch_id_set(
                tokio_rcu::loom_tests_api::EPOCH_ID_MAX - 2,
                std::sync::atomic::Ordering::Relaxed,
            );

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
                            // SAFETY: guard points to a heap allocation
                            let id = unsafe { guard.id_byref(uaf_detector0_key) };
                            assert_eq!(id, 0);
                            results.lock().saw_id0 = true;
                            id
                        } else if guard_ptr == uaf_detector1_ptr {
                            // SAFETY: guard points to a heap allocation
                            let id = unsafe { guard.id_byref(uaf_detector1_key) };
                            assert_eq!(id, 1);
                            results.lock().saw_id1 = true;
                            id
                        } else {
                            panic!("unexpected ptr");
                        }
                    };

                    let first_seen_id = with_before_after_poll(|| {
                        rcu_read_lock(|rcu_read_lock_guard| {
                            check_guard_logic(&*state.read(rcu_read_lock_guard))
                        })
                    });

                    // emulate this thread going to sleep and waking up from it.
                    // this is used to exercise the just-waking thread path.
                    tokio_rcu::loom_tests_api::on_thread_park();
                    tokio_rcu::loom_tests_api::on_thread_unpark();

                    // re-poll after waking from sleep.
                    let second_seen_id = with_before_after_poll(|| {
                        rcu_read_lock(|rcu_read_lock_guard| {
                            check_guard_logic(&*state.read(rcu_read_lock_guard))
                        })
                    });

                    // the id we see later must be greater than or equal the id we saw first, otherwise we see
                    // the writes happening in reverse, which should never happen.
                    assert!(second_seen_id >= first_seen_id);
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();

            let final_epoch_id =
                tokio_rcu::loom_tests_api::epoch_id_get(std::sync::atomic::Ordering::Relaxed);
            assert_eq!(final_epoch_id, tokio_rcu::loom_tests_api::EPOCH_ID_MIN + 2)
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
