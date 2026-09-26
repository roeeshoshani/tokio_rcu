# tokio_rcu

<!-- cargo-rdme start -->

a rust library providing an RCU (read-copy-update) algorithm specifically made for async rust with tokio.

this provides a lock-free and wait-free way to update a shared piece of state while it is concurrently being read and updated by
other tasks.

the core primitive provided by this crate is [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/fn.synchronize_rcu.html), which works just like the `synchronize_rcu` function in the
linux kernel - it waits for an rcu grace period, which allows writers to track when exactly they can reclaim swapped out data.

the low level [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/fn.synchronize_rcu.html) primitive can be used to build a bunch of higher level abstractions.
one very simple abstraction - a single pointer to a heap-allocated piece of shared data (an "rcu box") - is implemented in this crate
by the [`RcuBox`] type.

## performance

NOTE: this section specifically refers to [`RcuBox`], the main high level abstraction provided by this crate, but will probably also
apply to most other abstractions which can be implemented using the rcu primitive.

this crate is speicifcally useful for read-mostly data, as it makes readers extremely fast at the cost of making the writers slower.
when a reader reads the data stored in an rcu box (e.g. using [`RcuBox::read`]), the read operation is only a single load of an atomic
pointer. that's it. no branches, no book-keeping, just a single pointer load. it is basically the fastest a read can get.

during a read operation, no memory writes are performed, as opposed to spinlocks and mutexes which requires memory writes to shared
data, and sometimes even syscalls, just to access the underlying data.

specifically, for read-mostly data, the cache line containing the pointer can be shared between all readers, and the read operation
becomes just a single load from the cpu cache, which is extremely fast.
compared to spinlocks and mutexes which usually require exclusive ownership over the cacheline due to writes and other atomic
operations, this is much faster and provides much better reader latency.

also note that the time spent on a read operation is very predictable and static. other users of the data, such as concurrent readers
and even writers, do not affect the time it takes for a reader to read the data. a read is always just a single pointer load.
specifically a writer can slightly delay this load due to invalidating the cacheline containing the rcu protected pointer when
writing to it, but this is mostly negligible.
this consistency of the read operation can be very important in latency-critical applications which require a high-performance
fast path with predictable latency.

also see [benchmnarks](#benchmarks).

## quick start

```rust
use tokio_rcu::{rcu_block_on, rcu_box::RcuBox};

fn main() {
    rcu_block_on(async move {
        let numbers = RcuBox::new(Box::new(vec![1, 2, 3, 4]));

        // the rcu box's data can safely be accessed using the `with` function.
        numbers.with(|numbers| {
            assert!(numbers.contains(&3));
            assert!(!numbers.contains(&5));
        });

        // the rcu box's data can be modified while readers are using it.
        // and, the old allocation is returned.
        let new_numbers = Box::new(vec![5, 6, 7, 8]);
        let _old_numbers: Box<Vec<i32>> = numbers.swap(new_numbers).await;

        numbers.with(|numbers| {
            assert!(numbers.contains(&6));
            assert!(!numbers.contains(&10));
        });
    })
}
```

## realistic use case

for a more realistic use case, see `examples/basic.rs`.

## how does it work?

when a writer swaps out an old data pointer with a new data pointer containing updated data, he must then know when the previous
data pointer can be freed.

to free the data, the writer must first wait for all potential readers, who have already read the pointer and are now using it, to
finish using that pointer, to avoid a UAF (use-after-free) situation.

this problem can be solved in many ways, but rcu usually solves it by defining a state called a "quiescent state", such that when
a specific execution context (which can be a cpu core, or an OS thread) reaches that quiescent state, it is guaranteed to not hold
any rcu-protected pointer.

in this specific crate, the execution contexts are tokio threads, and the quiescent state was chosen to be tokio's
[`on_after_task_poll`] hook.

this works because this crate limits the usage of rcu protected pointers in a way that prevents them from being held across await
points.
so, when the runtime reaches the [`on_after_task_poll`] hook, it is guaranteed that no future is currently being executed on
the current thread, and since rcu protected pointers can't be held across await points, it is basically guaranteed that the current
thread is not holding any rcu protected pointers.

waiting an rcu grace period thus means first ensuring that all of our previous memory writes (e.g. rcu pointer swaps) are visible to
all other threads, and then just waiting for each other thread to pass at least once through a quiescent state.
after such a grace period, it is guaranteed that any swapped out pointers are no longer used by any of the threads, so their memory
can be reclaimed.

## enabling rcu support

to use the rcu primitives, you must use an rcu enabled tokio runtime.

the easiest way to do this is to use the [`rcu_block_on`](https://docs.rs/tokio_rcu/latest/tokio_rcu/fn.rcu_block_on.html) function which creates a tokio runtime with rcu support enabled, and then
runs the provided future inside that runtime using tokio's [`block_on`](https://docs.rs/tokio/latest/tokio/runtime/runtime/struct.Runtime.html#method.block_on).

if you wish to manually configure your runtime, you can use the more low-level [`enable_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/trait.TokioRuntimeBuilderExt.html#tymethod.enable_rcu) and
[`rcu_block_on`](https://docs.rs/tokio_rcu/latest/tokio_rcu/trait.TokioRuntimeExt.html#tymethod.rcu_block_on) functions.

## performance overhead

enabling rcu for a tokio runtime does introduce a little bit of overhead.

specifically, this crate uses tokio hooks (e.g. [`on_after_task_poll`]) to track quiescent states of tokio's worker threads.

but, this crate performs a lot of efforts to make this overhead as small as possible, especially in hooks like [`on_after_task_poll`]
which are called very often.

for example, the current implementation of the [`on_after_task_poll`] hook is basically just a couple of atomic loads and stores,
and is unnoticeable in terms of performance.

## other async runtimes

this crate could quite easily be ported to work with other runtimes other than tokio. i chose tokio because it is the most popular
runtime, and because it already provides hooks which allow me to track quiescent states quite easily.

## stability

this crate currently requires using the `tokio_unstable` configuration of tokio. this is required since the [`on_after_task_poll`]
hook is currently unstable, and is needed to make this crate work.

## benchmarks

to run the benchmarks, use:
```bash
cargo bench
```

the benchmarks mostly compare this crate with the `arc_swap` crate, which solves the same problem in a different way and provides
a very similar interface.

here are the results of running the benchmarks on my 20-core `12th Gen Intel(R) Core(TM) i7-12700` cpu:
```text
Timer precision: 47 ns
comparison                              fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ read_only_arc_swap                                 │               │               │               │         │
│  ├─ 1                                 22.33 ms      │ 34.62 ms      │ 22.82 ms      │ 23.87 ms      │ 100     │ 100
│  ├─ 8                                 24.86 ms      │ 46.24 ms      │ 28.97 ms      │ 29.83 ms      │ 100     │ 100
│  ├─ 16                                38.22 ms      │ 44.77 ms      │ 43.65 ms      │ 42.8 ms       │ 100     │ 100
│  ├─ 32                                65.91 ms      │ 75.48 ms      │ 68.38 ms      │ 69.26 ms      │ 100     │ 100
│  ╰─ 64                                129.6 ms      │ 137.4 ms      │ 133.1 ms      │ 132.9 ms      │ 100     │ 100
├─ read_only_rcu_box                                  │               │               │               │         │
│  ├─ 1                                 1.005 ms      │ 6.71 ms       │ 1.806 ms      │ 2.63 ms       │ 100     │ 100
│  ├─ 8                                 1.193 ms      │ 7.442 ms      │ 2.384 ms      │ 2.733 ms      │ 100     │ 100
│  ├─ 16                                1.29 ms       │ 4.381 ms      │ 2.521 ms      │ 2.626 ms      │ 100     │ 100
│  ├─ 32                                1.811 ms      │ 3.923 ms      │ 1.992 ms      │ 2.166 ms      │ 100     │ 100
│  ╰─ 64                                3.173 ms      │ 4.016 ms      │ 3.427 ms      │ 3.496 ms      │ 100     │ 100
├─ read_while_writing_arc_swap                        │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    24.81 ms      │ 29.73 ms      │ 25.46 ms      │ 26.09 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 1 writer tasks    27.52 ms      │ 46.04 ms      │ 33.75 ms      │ 34.25 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 2 writer tasks    35.69 ms      │ 58.73 ms      │ 39.27 ms      │ 40.4 ms       │ 100     │ 100
│  ├─ 16 reader tasks, 1 writer tasks   50.47 ms      │ 60.23 ms      │ 55.65 ms      │ 55.25 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 2 writer tasks   58.23 ms      │ 79.99 ms      │ 67.49 ms      │ 67.51 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 1 writer tasks   67.38 ms      │ 81.49 ms      │ 71.44 ms      │ 72.57 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 2 writer tasks   69.12 ms      │ 88.03 ms      │ 72.5 ms       │ 74.51 ms      │ 100     │ 100
│  ├─ 64 reader tasks, 1 writer tasks   130.8 ms      │ 140 ms        │ 134.5 ms      │ 134.5 ms      │ 100     │ 100
│  ╰─ 64 reader tasks, 2 writer tasks   131.8 ms      │ 143.1 ms      │ 136.8 ms      │ 136.5 ms      │ 100     │ 100
├─ read_while_writing_rcu_box                         │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    1.09 ms       │ 4.477 ms      │ 1.415 ms      │ 1.822 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 1 writer tasks    1.445 ms      │ 5.593 ms      │ 2.373 ms      │ 2.724 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 2 writer tasks    1.692 ms      │ 4.929 ms      │ 2.576 ms      │ 2.769 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 1 writer tasks   1.375 ms      │ 3.961 ms      │ 1.786 ms      │ 1.946 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 2 writer tasks   1.74 ms       │ 3.345 ms      │ 2.025 ms      │ 2.056 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 1 writer tasks   1.94 ms       │ 3.088 ms      │ 2.479 ms      │ 2.481 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 2 writer tasks   2.034 ms      │ 3.504 ms      │ 2.764 ms      │ 2.73 ms       │ 100     │ 100
│  ├─ 64 reader tasks, 1 writer tasks   3.395 ms      │ 5.704 ms      │ 4.037 ms      │ 4.074 ms      │ 100     │ 100
│  ╰─ 64 reader tasks, 2 writer tasks   3.332 ms      │ 4.919 ms      │ 4.352 ms      │ 4.263 ms      │ 100     │ 100
├─ write_while_reading_arc_swap                       │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    447 µs        │ 889.9 µs      │ 534.5 µs      │ 572.5 µs      │ 100     │ 100
│  ├─ 1 reader tasks, 8 writer tasks    622.1 µs      │ 1.843 ms      │ 870 µs        │ 959.1 µs      │ 100     │ 100
│  ├─ 1 reader tasks, 16 writer tasks   841.5 µs      │ 4.713 ms      │ 1.96 ms       │ 2.201 ms      │ 100     │ 100
│  ├─ 1 reader tasks, 32 writer tasks   1.304 ms      │ 8.912 ms      │ 2.989 ms      │ 3.186 ms      │ 100     │ 100
│  ├─ 1 reader tasks, 64 writer tasks   2.711 ms      │ 10.06 ms      │ 4.755 ms      │ 5.233 ms      │ 100     │ 100
│  ├─ 2 reader tasks, 2 writer tasks    510.9 µs      │ 921.8 µs      │ 656.5 µs      │ 667.8 µs      │ 100     │ 100
│  ├─ 4 reader tasks, 8 writer tasks    777.4 µs      │ 2.603 ms      │ 1.518 ms      │ 1.541 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 8 writer tasks    1.274 ms      │ 4.425 ms      │ 2.293 ms      │ 2.441 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 16 writer tasks  1.845 ms      │ 10.21 ms      │ 2.272 ms      │ 3.091 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 32 writer tasks  4.576 ms      │ 12.01 ms      │ 6.949 ms      │ 6.731 ms      │ 100     │ 100
│  ╰─ 64 reader tasks, 64 writer tasks  10.51 ms      │ 22.3 ms       │ 12.26 ms      │ 12.6 ms       │ 100     │ 100
╰─ write_while_reading_rcu_box                        │               │               │               │         │
   ├─ 1 reader tasks, 1 writer tasks    494.8 µs      │ 1.73 ms       │ 570.5 µs      │ 628.4 µs      │ 100     │ 100
   ├─ 1 reader tasks, 8 writer tasks    801.8 µs      │ 7.725 ms      │ 2.019 ms      │ 2.388 ms      │ 100     │ 100
   ├─ 1 reader tasks, 16 writer tasks   872.4 µs      │ 73.89 ms      │ 1.671 ms      │ 3.982 ms      │ 100     │ 100
   ├─ 1 reader tasks, 32 writer tasks   931.4 µs      │ 194 ms        │ 4.074 ms      │ 21.04 ms      │ 100     │ 100
   ├─ 1 reader tasks, 64 writer tasks   1.056 ms      │ 199 ms        │ 4.969 ms      │ 42.72 ms      │ 100     │ 100
   ├─ 2 reader tasks, 2 writer tasks    648.5 µs      │ 1.945 ms      │ 818.2 µs      │ 864.3 µs      │ 100     │ 100
   ├─ 4 reader tasks, 8 writer tasks    1.44 ms       │ 9.372 ms      │ 2.512 ms      │ 2.869 ms      │ 100     │ 100
   ├─ 8 reader tasks, 8 writer tasks    2.514 ms      │ 6.697 ms      │ 2.94 ms       │ 3.121 ms      │ 100     │ 100
   ├─ 16 reader tasks, 16 writer tasks  6.412 ms      │ 17.66 ms      │ 6.888 ms      │ 7.19 ms       │ 100     │ 100
   ├─ 32 reader tasks, 32 writer tasks  10.35 ms      │ 41.39 ms      │ 13.26 ms      │ 13.1 ms       │ 100     │ 100
   ╰─ 64 reader tasks, 64 writer tasks  15.94 ms      │ 29.01 ms      │ 19.01 ms      │ 19.01 ms      │ 100     │ 100
```

as you can see, `tokio_rcu`'s reads are faster than `arc_swap`'s reads (about 9x-40x faster on average), while `tokio_rcu`'s writes
are slower than `arc_swap`'s writes (about 2x slower on average). for a read-heavy situation, this is ideal.

furthermore, note that when using `arc_swap`, the time it takes for a single read operation seems to scale with the number of
concurrent readers (see the results of the `arc_swap_read_only` and `arc_swap_read_while_writing` benchmarks), while `tokio_rcu`'s
read operation takes roughly the same amount of time regardless of the number of concurrent readers, up until the point where there
are more readers than cpu cores (more than 20 reader tasks), at which point the readers start sharing cpu cores and competing for
their runtime, which obviously takes its toll on the performance.

also note that this constant time for the read operation holds even when writers are concurrently modifying the data - the time
spent on a single read operation remains roughly the same (see `rcu_box_read_while_writing`), unlike `arc_swap` (see
`arc_swap_read_while_writing`).

moreover, while `tokio_rcu`'s writes are slower, it is mostly because the writers are sleeping while waiting for other threads to
pass through a quiescent state, so they are NOT slower in the sense that they perform more cpu-bound work, only in the total time it
takes for a swap operation to complete after fully awaiting it. in practice the writes may actually spend less cpu time than
`arc_swap`'s write.

## testing

to run the tests, use:
```bash
cargo all-features nextest run --release
cargo all-features test --doc --release
```

(NOTE: this requires installing `cargo-all-features` and `cargo-nextest`)

`cargo-nextest` is used since it allows running each test as a separate process, which is important for testing this crate, since
this crate heavily relies on thread local variables and generally assumes that only a single tokio runtime is used per process.
furthermore, bugs in the rcu primitives can cause UAFs which may crash the process. if the process crashes when using `cargo test`,
all tests stop running and no diagnostics are reported. with `cargo-nextest`, such failures are gracefully reported as test failures.

sadly, `cargo-nextest` currently does not support running doctests, so we must run them separately. note that `cargo test --doc`
already runs each test in its own process, so luckily for us, we don't need `cargo-nextest` for process isolation in this case.

furthermore, `cargo-all-features` is used to also test the crate under the `small_epoch_id` feature flag, which is for testing mode
only, and allows testing some internal edge cases of this crate which are extremely hard to reach in the default configuration.

it is also recommended to run the tests in release mode since it increases the probability of being able to find race conditions and
other hard to catch edge cases.

## platform support

this crate is supported on every platform that is supported by tokio.

## license

This project is licensed under the MIT license.

[`on_after_task_poll`]: https://docs.rs/tokio/latest/tokio/runtime/builder/struct.Builder.html#method.on_after_task_poll
[`RcuBox`]: https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_box/struct.RcuBox.html
[`RcuBox::read`]: https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_box/struct.RcuBox.html#method.read

<!-- cargo-rdme end -->
