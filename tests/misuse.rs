use std::{
    any::Any,
    hint::black_box,
    panic::{AssertUnwindSafe, UnwindSafe},
    pin::pin,
    sync::Arc,
    task::{Poll, Waker},
    time::Duration,
};

use tokio_rcu::{rcu_block_on, rcu_ptr::RcuPtr};

const USE_OUTSIDE_OF_RCU_ENABLED_RUNTIME_ERR: &str =
    "attempted to read an rcu protected pointer outside of an rcu-enabled tokio runtime";

fn extract_string_panic_message(err: Box<dyn Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        panic!("failed to downcast panic message payload")
    }
}

fn assert_panics_with_use_outside_of_rcu_enabled_runtime_err<F: FnOnce() + UnwindSafe>(f: F) {
    let err = std::panic::catch_unwind(move || {
        f();
    })
    .unwrap_err();
    assert_eq!(
        extract_string_panic_message(err),
        USE_OUTSIDE_OF_RCU_ENABLED_RUNTIME_ERR
    )
}

#[test]
fn read_outside_of_runtime() {
    let x = RcuPtr::new(Box::new(String::from("some interesting piece of text")));
    assert_panics_with_use_outside_of_rcu_enabled_runtime_err(|| x.with(|_| {}));
}

#[tokio::test]
async fn read_in_non_rcu_runtime() {
    let x = RcuPtr::new(Box::new(String::from("some interesting piece of text")));
    assert_panics_with_use_outside_of_rcu_enabled_runtime_err(|| x.with(|_| {}));
}

#[test]
fn read_from_non_runtime_thread_spawned_inside_runtime() {
    rcu_block_on(async {
        let x = RcuPtr::new(Box::new(String::from("some interesting piece of text")));
        std::thread::spawn(move || {
            assert_panics_with_use_outside_of_rcu_enabled_runtime_err(|| x.with(|_| {}));
        })
        .join()
        .unwrap();
    })
}

#[test]
fn read_from_main_thread_after_runtime_finished() {
    let x = Arc::new(RcuPtr::new(Box::new(String::from(
        "some interesting piece of text",
    ))));
    rcu_block_on({
        let x = x.clone();
        async move { x.with(|_| {}) }
    });
    assert_panics_with_use_outside_of_rcu_enabled_runtime_err(|| x.with(|_| {}));
}

#[test]
fn swap_inside_with_by_manually_polling_never_finishes() {
    rcu_block_on(async {
        let rcu_ptr = Arc::new(RcuPtr::new(Box::new(String::from(
            "some interesting piece of text",
        ))));
        rcu_ptr.with({
            let rcu_ptr = rcu_ptr.clone();
            move |value| {
                // the code below panics due to dropping the swap future before it finishes.
                let err = std::panic::catch_unwind(AssertUnwindSafe(move || {
                    let swap_future =
                        rcu_ptr.swap(Box::new(String::from("hopefully this doesnt work")));
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

                    // value must not have been dropped
                    let _: String = black_box(black_box(value).clone());
                }))
                .unwrap_err();

                assert!(extract_string_panic_message(err).contains("can't be dropped since concurrent readers may be using it. it must first be waited for."));
            }
        })
    });
}
