use std::{hint::black_box, sync::Arc, time::Duration};

use tokio_rcu::{Rcu, TokioRuntimeBuilderExt};

/// a test which makes sure that we don't cause a UAF while stress reading and writing the rcu protected pointer.
#[test]
fn no_uaf_during_stress() {
    const NUM_READER_TASKS: usize = 64;
    const NUM_WRITER_TASKS: usize = 64;
    const READER_NUM_CLONES: usize = 1000;
    const WRITER_NUM_WRITES: usize = 10_000;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_rcu()
        .build()
        .unwrap();

    rt.block_on(async {
        let initial_string = "<VALID> initial string";
        let final_string = "<VALID> final string";

        let data = Arc::new(Rcu::new(Box::new(String::from(initial_string))));
        let reader_tasks: Vec<_> = (0..NUM_READER_TASKS)
            .map(|_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        loop {
                            // extra scope to scope the rcu read guard
                            {
                                let value = data.read();

                                let orig_value = black_box(black_box(&value).clone());

                                // make sure that the string is one of the valid options.
                                // the writers overwrite the data of old strings with invalid contents, so that if we happen
                                // to see any such freed pointer, we will detect the invalid contents and fail the test.
                                assert!(orig_value.starts_with("<VALID>"));

                                // use the value for a while to try to trigger some UAFs.
                                for _ in 0..READER_NUM_CLONES {
                                    let cloned_value = black_box(black_box(&value).clone());

                                    // the same guard should always yield the same data.
                                    assert_eq!(orig_value, cloned_value);
                                }

                                if *value == final_string {
                                    break;
                                }
                            }

                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        let writer_tasks: Vec<_> = (0..NUM_WRITER_TASKS)
            .map(|writer_id| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        for i in 0..WRITER_NUM_WRITES {
                            let new_string =
                                format!("<VALID> hello from worker {} {}", writer_id, i);
                            let old_str = data.swap(Box::new(new_string)).await;

                            // overwrite the memory of the old string with some invalid data, so that if any reader happens
                            // to read it, he will detect that it is invalid and fail the test.
                            let mut old_str_bytes = old_str.into_bytes();
                            old_str_bytes.fill(b'A');

                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();

        for task in writer_tasks {
            task.await.unwrap();
        }

        let _ = data.swap(Box::new(String::from(final_string))).await;

        for task in reader_tasks {
            task.await.unwrap();
        }
    })
}

/// a test which makes sure that we don't cause a UAF while stress reading and writing the rcu protected pointer with sleeps
/// introduced in between.
/// this is specifically important for checking the race where a waiter sees some worker thread as sleeping, but the worker
/// thread wakes up immediately and starts using the pointer.
#[test]
fn no_uaf_with_sleeps() {
    const NUM_READER_TASKS: usize = 4;
    const NUM_WRITER_TASKS: usize = 4;
    const READER_NUM_CLONES: usize = 1000;
    const WRITER_NUM_WRITES: usize = 1000;
    const SHORT_SLEEP_DURATION: Duration = Duration::from_millis(10);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_rcu()
        .worker_threads(
            // make sure that we have 1 worker thread per task, so that when a task sleeps the entire thread goes to sleep
            // instead of scheduling another task.
            NUM_READER_TASKS + NUM_WRITER_TASKS,
        )
        .build()
        .unwrap();

    rt.block_on(async {
        let initial_string = "<VALID> initial string";
        let final_string = "<VALID> final string";

        let data = Arc::new(Rcu::new(Box::new(String::from(initial_string))));
        let reader_tasks: Vec<_> = (0..NUM_READER_TASKS)
            .map(|_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        loop {
                            // extra scope to scope the rcu read guard
                            {
                                let value = data.read();

                                let orig_value = black_box(black_box(&value).clone());

                                // make sure that the string is one of the valid options.
                                // the writers overwrite the data of old strings with invalid contents, so that if we happen
                                // to see any such freed pointer, we will detect the invalid contents and fail the test.
                                assert!(orig_value.starts_with("<VALID>"));

                                // use the value for a while to try to trigger some UAFs.
                                for _ in 0..READER_NUM_CLONES {
                                    let cloned_value = black_box(black_box(&value).clone());

                                    // the same guard should always yield the same data.
                                    assert_eq!(orig_value, cloned_value);
                                }

                                if *value == final_string {
                                    break;
                                }
                            }

                            tokio::time::sleep(SHORT_SLEEP_DURATION).await;
                        }
                    }
                })
            })
            .collect();
        let writer_tasks: Vec<_> = (0..NUM_WRITER_TASKS)
            .map(|writer_id| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        for i in 0..WRITER_NUM_WRITES {
                            let new_string =
                                format!("<VALID> hello from worker {} {}", writer_id, i);
                            let old_str = data.swap(Box::new(new_string)).await;

                            // overwrite the memory of the old string with some invalid data, so that if any reader happens
                            // to read it, he will detect that it is invalid and fail the test.
                            let mut old_str_bytes = old_str.into_bytes();
                            old_str_bytes.fill(b'A');

                            tokio::time::sleep(SHORT_SLEEP_DURATION).await;
                        }
                    }
                })
            })
            .collect();

        for task in writer_tasks {
            task.await.unwrap();
        }

        let _ = data.swap(Box::new(String::from(final_string))).await;

        for task in reader_tasks {
            task.await.unwrap();
        }
    })
}

// make sure that calling `enable_rcu` multiple times works fine.
// this shouldn't be done, but should behave nicely just in case.
#[test]
fn enable_rcu_multiple_calls() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // call `enable_rcu` multiple times
        .enable_rcu()
        .enable_rcu()
        .enable_rcu()
        .build()
        .unwrap();
    rt.block_on(async move {
        let state = Arc::new(Rcu::new(Box::new(String::from("some interesting string"))));
        let reader = tokio::spawn({
            let state = state.clone();
            async move {
                loop {
                    let value = state.read();
                    if *value == "done" {
                        break;
                    }
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let old_string = state.swap(Box::new(String::from("done"))).await;
        assert_eq!(*old_string, "some interesting string");
        reader.await.unwrap();
    });
}

// make sure that creating multiple runtimes which use `enable_rcu` still works fine.
// this shouldn't be done, but should behave nicely just in case.
#[test]
fn enable_rcu_multiple_runtimes() {
    let rt1 = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_rcu()
        .build()
        .unwrap();

    let rt2 = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_rcu()
        .build()
        .unwrap();

    let logic = || async move {
        let state = Arc::new(Rcu::new(Box::new(String::from("some interesting string"))));
        let reader = tokio::spawn({
            let state = state.clone();
            async move {
                loop {
                    let value = state.read();
                    if *value == "done" {
                        break;
                    }
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let old_string = state.swap(Box::new(String::from("done"))).await;
        assert_eq!(*old_string, "some interesting string");
        reader.await.unwrap();
    };

    rt1.block_on(logic());
    rt2.block_on(logic());
}

#[test]
fn double_buffering() {
    const NUM_READER_TASKS: usize = 64;
    const READER_NUM_CLONES: usize = 1000;
    const WRITER_NUM_WRITES: usize = 1000;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_rcu()
        .build()
        .unwrap();

    rt.block_on(async {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct Buffer {
            buffer_index: usize,
            bytes: Vec<u8>,
            should_readers_exit: bool,
        }

        // allocate 2 buffers to be used in our double buffering scheme
        let buf_a = Box::new(Buffer {
            buffer_index: 0,
            bytes: vec![5u8; 2048],
            should_readers_exit: false,
        });
        let buf_b = Box::new(Buffer {
            buffer_index: 1,
            bytes: vec![17u8; 4096],
            should_readers_exit: false,
        });

        let data = Arc::new(Rcu::new(buf_a));

        let reader_tasks: Vec<_> = (0..NUM_READER_TASKS)
            .map(|_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        loop {
                            // extra scope to scope the rcu read guard
                            {
                                let value = data.read();

                                let orig_value = black_box(black_box(&value).clone());

                                assert!(
                                    orig_value.buffer_index == 0 || orig_value.buffer_index == 1
                                );

                                // use the value for a while to try to trigger some UAFs.
                                for _ in 0..READER_NUM_CLONES {
                                    let cloned_value = black_box(black_box(&value).clone());

                                    // the same guard should always yield the same data.
                                    assert_eq!(orig_value, cloned_value);
                                }

                                if value.should_readers_exit {
                                    break;
                                }
                            }

                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();

        let mut cur_unused_buf = buf_b;
        for i in 0..WRITER_NUM_WRITES {
            cur_unused_buf.bytes.fill(i as u8);
            cur_unused_buf = data.swap(cur_unused_buf).await;
        }

        cur_unused_buf.should_readers_exit = true;
        data.swap(cur_unused_buf).await;

        for task in reader_tasks {
            task.await.unwrap();
        }
    })
}
