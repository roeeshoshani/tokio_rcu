use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{self, AtomicBool},
    },
};

use arc_swap::ArcSwap;
use tokio_rcu::{rcu_block_on, rcu_ptr::RcuPtr};

fn main() {
    divan::main();
}

const NUM_READS_PER_ITERATION: usize = 8192;
const NUM_READ_ITERATIONS: usize = 256;

const READ_ONLY_BENCH_NUM_TASKS_ARGS: &[usize] = &[1, 8, 16, 32, 64];

#[divan::bench(threads = false, args = READ_ONLY_BENCH_NUM_TASKS_ARGS)]
fn bench_rcu_ptr_read_only(num_tasks: usize) {
    rcu_block_on(async move {
        let data = Arc::new(RcuPtr::new(Box::new(0)));
        let tasks: Vec<_> = (0..num_tasks)
            .map(move |_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        for _ in 0..NUM_READ_ITERATIONS {
                            for _ in 0..NUM_READS_PER_ITERATION {
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

#[divan::bench(threads = false, args = READ_ONLY_BENCH_NUM_TASKS_ARGS)]
fn bench_arc_swap_read_only(num_tasks: usize) {
    rcu_block_on(async move {
        let data = Arc::new(ArcSwap::new(Arc::new(0)));
        let tasks: Vec<_> = (0..num_tasks)
            .map(move |_| {
                tokio::spawn({
                    let data = data.clone();
                    async move {
                        for _ in 0..NUM_READ_ITERATIONS {
                            for _ in 0..NUM_READS_PER_ITERATION {
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct ReadWhileWritingBenchCfg {
    num_reader_tasks: usize,
    num_writer_tasks: usize,
}
const READ_WHILE_WRITING_BENCH_CFGS: &[ReadWhileWritingBenchCfg] = &[
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 1,
        num_writer_tasks: 1,
    },
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 8,
        num_writer_tasks: 1,
    },
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 8,
        num_writer_tasks: 2,
    },
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 16,
        num_writer_tasks: 1,
    },
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 16,
        num_writer_tasks: 2,
    },
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 32,
        num_writer_tasks: 1,
    },
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 32,
        num_writer_tasks: 2,
    },
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 64,
        num_writer_tasks: 1,
    },
    ReadWhileWritingBenchCfg {
        num_reader_tasks: 64,
        num_writer_tasks: 2,
    },
];

#[divan::bench(threads = false, args = READ_WHILE_WRITING_BENCH_CFGS)]
fn bench_rcu_ptr_read_while_writing(cfg: ReadWhileWritingBenchCfg) {
    rcu_block_on(async move {
        let data = Arc::new(RcuPtr::new(Box::new(0)));
        let readers: Vec<_> = (0..cfg.num_reader_tasks)
            .map({
                let data = data.clone();
                move |_| {
                    tokio::spawn({
                        let data = data.clone();
                        async move {
                            for _ in 0..NUM_READ_ITERATIONS {
                                for _ in 0..NUM_READS_PER_ITERATION {
                                    black_box(data.with(|_| {}))
                                }
                                tokio::task::yield_now().await;
                            }
                        }
                    })
                }
            })
            .collect();

        let should_writers_stop = Arc::new(AtomicBool::new(false));
        let writers: Vec<_> = (0..cfg.num_writer_tasks)
            .map({
                let data = data.clone();
                let should_writers_stop = should_writers_stop.clone();
                move |_| {
                    tokio::spawn({
                        let data = data.clone();
                        let should_writers_stop = should_writers_stop.clone();
                        async move {
                            let mut cur_owned_data = Box::new(0);
                            while !should_writers_stop.load(atomic::Ordering::Relaxed) {
                                cur_owned_data = data.swap(cur_owned_data).await;
                            }
                        }
                    })
                }
            })
            .collect();

        for task in readers {
            task.await.unwrap();
        }

        should_writers_stop.store(true, atomic::Ordering::Relaxed);

        for task in writers {
            task.await.unwrap();
        }
    });
}

#[divan::bench(threads = false, args = READ_WHILE_WRITING_BENCH_CFGS)]
fn bench_arc_swap_read_while_writing(cfg: ReadWhileWritingBenchCfg) {
    rcu_block_on(async move {
        let data = Arc::new(ArcSwap::new(Arc::new(0)));
        let readers: Vec<_> = (0..cfg.num_reader_tasks)
            .map({
                let data = data.clone();
                move |_| {
                    tokio::spawn({
                        let data = data.clone();
                        async move {
                            for _ in 0..NUM_READ_ITERATIONS {
                                for _ in 0..NUM_READS_PER_ITERATION {
                                    black_box(data.load());
                                }
                                tokio::task::yield_now().await;
                            }
                        }
                    })
                }
            })
            .collect();

        let should_writers_stop = Arc::new(AtomicBool::new(false));
        let writers: Vec<_> = (0..cfg.num_writer_tasks)
            .map({
                let data = data.clone();
                let should_writers_stop = should_writers_stop.clone();
                move |_| {
                    tokio::spawn({
                        let data = data.clone();
                        let should_writers_stop = should_writers_stop.clone();
                        async move {
                            let mut cur_owned_data = Arc::new(0);
                            while !should_writers_stop.load(atomic::Ordering::Relaxed) {
                                cur_owned_data = data.swap(cur_owned_data);
                                tokio::task::yield_now().await;
                            }
                        }
                    })
                }
            })
            .collect();

        for task in readers {
            task.await.unwrap();
        }

        should_writers_stop.store(true, atomic::Ordering::Relaxed);

        for task in writers {
            task.await.unwrap();
        }
    });
}
