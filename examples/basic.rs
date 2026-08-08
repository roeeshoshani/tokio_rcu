use std::{sync::Arc, time::Duration};

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
        let orig_string = "Hello, world!";
        let data = Arc::new(Rcu::new(orig_string));
        let tasks: Vec<_> = (0..1)
            .map(|_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        loop {
                            {
                                let value = data.read();
                                println!("{}", *value);
                                if *value != orig_string {
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
        let old_string = data.swap("It works!").await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("old string {:?}", old_string);
        for task in tasks {
            task.await.unwrap();
        }
    })
}
