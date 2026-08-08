use std::{io::Write, sync::Arc, time::Duration};

use tokio_rcu::Rcu;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| {
            tokio_rcu::on_thread_start();
        })
        .on_thread_stop(|| {
            tokio_rcu::on_thread_stop();
        })
        .on_before_task_poll(|_| {
            tokio_rcu::on_before_task_poll();
        })
        .on_after_task_poll(|_| {
            tokio_rcu::on_after_task_poll();
        })
        .build()
        .unwrap();

    rt.block_on(async {
        let orig_string = "Hello, world!\n";
        let final_string = "It works final!\n";
        let data = Arc::new(Rcu::new(String::from(orig_string)));
        let tasks: Vec<_> = (0..100)
            .map(|_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        let mut stdout = std::io::stdout();
                        loop {
                            {
                                let value = data.read();
                                stdout.write_all(value.as_bytes()).unwrap();

                                if *value == final_string {
                                    break;
                                }
                            }
                            tokio::time::sleep(Duration::from_secs(0)).await;
                        }
                    }
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_secs(1)).await;

        for i in 0..100_000 {
            let _ = data.swap(format!("It works {}!\n", i)).await;
        }

        let old_string = data.swap(String::from(final_string)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("old string {:?}", old_string);
        for task in tasks {
            task.await.unwrap();
        }
    })
}
