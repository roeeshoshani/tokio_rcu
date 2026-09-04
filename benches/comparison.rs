use std::{hint::black_box, sync::Arc};

use arc_swap::ArcSwap;
use tokio_rcu::{rcu_block_on, rcu_ptr::RcuPtr};

fn main() {
    divan::main();
}

const READ_ONLY_BENCH_NUM_READS_PER_ITERATION: usize = 8192;
const READ_ONLY_BENCH_NUM_READ_ITERATIONS: usize = 512;

#[divan::bench(threads = false, args = [1, 8, 16, 32, 64])]
fn bench_rcu_ptr_read_only(num_tasks: usize) {
    rcu_block_on(async move {
        let data = Arc::new(RcuPtr::new(Box::new(0)));
        let tasks: Vec<_> = (0..num_tasks)
            .map(move |_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        for _ in 0..READ_ONLY_BENCH_NUM_READ_ITERATIONS {
                            for _ in 0..READ_ONLY_BENCH_NUM_READS_PER_ITERATION {
                                black_box(data.with(|_| {}))
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
    });
}

#[divan::bench(threads = false, args = [1, 8, 16, 32, 64])]
fn bench_arc_swap_read_only(num_tasks: usize) {
    rcu_block_on(async move {
        let data = Arc::new(ArcSwap::new(Arc::new(0)));
        let tasks: Vec<_> = (0..num_tasks)
            .map(move |_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        for _ in 0..READ_ONLY_BENCH_NUM_READ_ITERATIONS {
                            for _ in 0..READ_ONLY_BENCH_NUM_READS_PER_ITERATION {
                                black_box(data.load());
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
    });
}
