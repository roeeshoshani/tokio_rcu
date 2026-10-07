use std::{
    panic::{AssertUnwindSafe, UnwindSafe},
    pin::pin,
    task::{Poll, Waker},
    time::Duration,
};

use tokio_rcu::{
    rcu_block_on, rcu_read_lock, synchronize_rcu, test_utils::extract_string_panic_message,
};

const USE_OUTSIDE_OF_RCU_TRACKED_THREAD_ERR: &str =
    "attempted to read an rcu box in a non rcu tracked thread";
const CANT_START_RUNTIME_INSIDE_RUNTIME_ERR: &str = "Cannot start a runtime from within a runtime";

fn assert_panics_with_use_outside_of_rcu_tracked_thread_err<F: FnOnce() + UnwindSafe>(f: F) {
    let err = std::panic::catch_unwind(move || {
        f();
    })
    .unwrap_err();
    assert_eq!(
        extract_string_panic_message(err),
        USE_OUTSIDE_OF_RCU_TRACKED_THREAD_ERR
    )
}

#[test]
fn rcu_read_lock_outside_of_runtime() {
    assert_panics_with_use_outside_of_rcu_tracked_thread_err(|| rcu_read_lock(|_| {}));
}

#[tokio::test]
async fn rcu_read_lock_in_non_rcu_runtime() {
    assert_panics_with_use_outside_of_rcu_tracked_thread_err(|| rcu_read_lock(|_| {}));
}

#[test]
fn rcu_read_lock_from_non_runtime_thread_spawned_inside_runtime() {
    rcu_block_on(async {
        std::thread::spawn(move || {
            assert_panics_with_use_outside_of_rcu_tracked_thread_err(|| rcu_read_lock(|_| {}));
        })
        .join()
        .unwrap();
    })
}

#[test]
fn rcu_read_lock_from_main_thread_after_runtime_finished() {
    rcu_block_on(async move { rcu_read_lock(|_| {}) });
    assert_panics_with_use_outside_of_rcu_tracked_thread_err(|| rcu_read_lock(|_| {}));
}

#[test]
fn rcu_synchronize_inside_rcu_read_lock_by_manually_polling_never_finishes() {
    rcu_block_on(async {
        rcu_read_lock(|_guard| {
            let swap_future = synchronize_rcu();
            let mut swap_future_pin = pin!(swap_future);
            let mut cx = std::task::Context::from_waker(Waker::noop());

            for _ in 0..10_000 {
                assert_eq!(swap_future_pin.as_mut().poll(&mut cx), Poll::Pending);
            }

            // even if we wait a while and try again, this should never finish
            std::thread::sleep(Duration::from_millis(100));

            for _ in 0..10_000 {
                assert_eq!(swap_future_pin.as_mut().poll(&mut cx), Poll::Pending);
            }
        });
    });
}

#[test]
fn rcu_synchronize_inside_rcu_read_lock_using_new_current_thread_runtime() {
    rcu_block_on(async {
        rcu_read_lock(|_guard| {
            let err = std::panic::catch_unwind(AssertUnwindSafe(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(synchronize_rcu());
            }))
            .unwrap_err();

            assert!(
                extract_string_panic_message(err).contains(CANT_START_RUNTIME_INSIDE_RUNTIME_ERR)
            );
        });
    });
}

#[test]
fn rcu_read_lock_from_blocking_pool_thread() {
    rcu_block_on(async {
        tokio::task::spawn_blocking(move || {
            assert_panics_with_use_outside_of_rcu_tracked_thread_err(|| rcu_read_lock(|_| {}));
        })
        .await
        .unwrap();
    })
}
