use std::sync::Arc;

use tokio_rcu::{Rcu, TokioRuntimeBuilderExt};

#[test]
fn no_uaf() {
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

        let data = Arc::new(Rcu::new(String::from(initial_string)));
        let reader_tasks: Vec<_> = (0..NUM_READER_TASKS)
            .map(|_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        loop {
                            // extra scope to scope the rcu read guard
                            {
                                let value = data.read();

                                let orig_value = value.clone();

                                // make sure that the string is one of the valid options.
                                // the writers overwrite the data of old strings with invalid contents, so that if we happen
                                // to see any such freed pointer, we will detect the invalid contents and fail the test.
                                assert!(orig_value.starts_with("<VALID>"));

                                // use the value for a while to try to trigger some UAFs.
                                for _ in 0..READER_NUM_CLONES {
                                    let cloned_value = value.clone();

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
                            let old_str = data.swap(new_string).await;

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

        let _ = data.swap(String::from(final_string)).await;

        for task in reader_tasks {
            task.await.unwrap();
        }
    })
}
