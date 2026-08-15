use std::{io::Write, sync::Arc, time::Duration};

use tokio_rcu::{Rcu, TokioRuntimeBuilderExt};

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_rcu()
        .build()
        .unwrap();

    rt.block_on(async {
        let orig_string = "Hello, world!\n";
        let final_string = "It works final!\n";
        let data = Arc::new(Rcu::new(String::from(orig_string)));
        let reader_tasks: Vec<_> = (0..200)
            .map(|_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        let mut stdout = std::io::stdout();
                        loop {
                            {
                                let value = data.read();
                                stdout.write_all(value.as_bytes()).unwrap();

                                // use the value for a while to try to trigger some UAFs.
                                for _ in 0..10_000 {
                                    let _ = value.clone();
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
        tokio::time::sleep(Duration::from_secs(1)).await;

        let writer_tasks: Vec<_> = (0..500)
            .map(|worker_id| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        for i in 0..10_000 {
                            let new_string = format!("It works {} {}!\n", worker_id, i);
                            data.swap(new_string).await;
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();

        for task in writer_tasks {
            task.await.unwrap();
        }

        let old_string = data.swap(String::from(final_string)).await;

        for task in reader_tasks {
            task.await.unwrap();
        }

        println!("old string {:?}", old_string);
    })
}
