# tokio_rcu

<!-- cargo-rdme start -->

a rust library providing an RCU (read-copy-update) algorithm specifically made for async rust with tokio.

this provides a lock-free and wait-free way to update a shared piece of state while it is concurrently being read and updated by
other tasks.

the core primitive provided by this crate is [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/fn.synchronize_rcu.html), which works just like the `synchronize_rcu` function in the
linux kernel - it waits for an rcu grace period, which allows writers to track when exactly they can reclaim swapped out data.

the low level [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/fn.synchronize_rcu.html) primitive can be used to build a bunch of higher level abstractions.
the simplest abstraction - a single pointer to shared data - is implemented in this crate by the [`RcuPtr`] type.

## performance

NOTE: this section specifically refers to [`RcuPtr`], the main high level abstraction provided by this crate, but will probably also
apply to most other abstractions which can be implemented using the rcu primitive.

this crate is speicifcally useful for read-mostly data, as it makes readers extremely fast at the cost of making the writers slower.
when a reader reads the rcu protected data (e.g. using [`RcuPtr::read`]), the read operation is only a single load of an atomic
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
use tokio_rcu::{rcu_block_on, rcu_ptr::RcuPtr};

fn main() {
    rcu_block_on(async move {
        let numbers = RcuPtr::new(Box::new(vec![1, 2, 3, 4]));

        // rcu protected values can be accessed using the `with` function.
        numbers.with(|numbers| {
            assert!(numbers.contains(&3));
            assert!(!numbers.contains(&5));
        });

        // the rcu protected value can be modified while readers are using it.
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
any rcu protected pointer.

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

specifically, the current implementation of the [`on_after_task_poll`] hook is basically just a couple of atomic loads and stores,
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
Timer precision: 40 ns
comparison                              fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ arc_swap_read_only                                 │               │               │               │         │
│  ├─ 1                                 21.68 ms      │ 32.75 ms      │ 22.53 ms      │ 23.69 ms      │ 100     │ 100
│  ├─ 8                                 23.73 ms      │ 49.89 ms      │ 29.11 ms      │ 30.15 ms      │ 100     │ 100
│  ├─ 16                                39.49 ms      │ 44.63 ms      │ 43.61 ms      │ 42.87 ms      │ 100     │ 100
│  ├─ 32                                65.75 ms      │ 81.64 ms      │ 68.5 ms       │ 69.41 ms      │ 100     │ 100
│  ╰─ 64                                129.5 ms      │ 137.9 ms      │ 132.6 ms      │ 132.5 ms      │ 100     │ 100
├─ rcu_ptr_read_only                                  │               │               │               │         │
│  ├─ 1                                 1.042 ms      │ 7.554 ms      │ 4.998 ms      │ 4.282 ms      │ 100     │ 100
│  ├─ 8                                 1.138 ms      │ 6.34 ms       │ 2.505 ms      │ 2.8 ms        │ 100     │ 100
│  ├─ 16                                1.802 ms      │ 5.718 ms      │ 3.373 ms      │ 3.462 ms      │ 100     │ 100
│  ├─ 32                                1.837 ms      │ 5.772 ms      │ 2.103 ms      │ 2.43 ms       │ 100     │ 100
│  ╰─ 64                                3.227 ms      │ 5.033 ms      │ 3.467 ms      │ 3.511 ms      │ 100     │ 100
├─ arc_swap_read_while_writing                        │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    24.7 ms       │ 33.48 ms      │ 26.03 ms      │ 26.49 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 1 writer tasks    30.04 ms      │ 43.33 ms      │ 33.75 ms      │ 34.59 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 2 writer tasks    34.77 ms      │ 50.08 ms      │ 38.08 ms      │ 38.96 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 1 writer tasks   48.31 ms      │ 59.53 ms      │ 54.27 ms      │ 54 ms         │ 100     │ 100
│  ├─ 16 reader tasks, 2 writer tasks   54.81 ms      │ 74.05 ms      │ 65.95 ms      │ 64.88 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 1 writer tasks   67.09 ms      │ 87.9 ms       │ 69.57 ms      │ 71.65 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 2 writer tasks   68.71 ms      │ 85.48 ms      │ 73.15 ms      │ 74.21 ms      │ 100     │ 100
│  ├─ 64 reader tasks, 1 writer tasks   130.2 ms      │ 140.5 ms      │ 134.1 ms      │ 134 ms        │ 100     │ 100
│  ╰─ 64 reader tasks, 2 writer tasks   131 ms        │ 145.5 ms      │ 136 ms        │ 136 ms        │ 100     │ 100
├─ rcu_ptr_read_while_writing                         │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    1.071 ms      │ 2.557 ms      │ 1.258 ms      │ 1.37 ms       │ 100     │ 100
│  ├─ 8 reader tasks, 1 writer tasks    1.372 ms      │ 3.63 ms       │ 2.005 ms      │ 2.125 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 2 writer tasks    1.799 ms      │ 4.349 ms      │ 2.501 ms      │ 2.676 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 1 writer tasks   1.554 ms      │ 3.639 ms      │ 1.791 ms      │ 1.927 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 2 writer tasks   1.672 ms      │ 3.988 ms      │ 2.025 ms      │ 2.049 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 1 writer tasks   1.884 ms      │ 4.099 ms      │ 2.433 ms      │ 2.439 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 2 writer tasks   1.978 ms      │ 5.538 ms      │ 2.75 ms       │ 2.776 ms      │ 100     │ 100
│  ├─ 64 reader tasks, 1 writer tasks   3.316 ms      │ 7.614 ms      │ 3.976 ms      │ 3.978 ms      │ 100     │ 100
│  ╰─ 64 reader tasks, 2 writer tasks   3.38 ms       │ 5.551 ms      │ 4.476 ms      │ 4.313 ms      │ 100     │ 100
├─ arc_swap_write_while_reading                       │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    455.3 µs      │ 852.4 µs      │ 517.6 µs      │ 565.2 µs      │ 100     │ 100
│  ├─ 1 reader tasks, 8 writer tasks    558.8 µs      │ 3.202 ms      │ 814.3 µs      │ 900.5 µs      │ 100     │ 100
│  ├─ 1 reader tasks, 16 writer tasks   746.1 µs      │ 2.984 ms      │ 1.567 ms      │ 1.584 ms      │ 100     │ 100
│  ├─ 1 reader tasks, 32 writer tasks   1.304 ms      │ 6.793 ms      │ 3.76 ms       │ 3.352 ms      │ 100     │ 100
│  ├─ 1 reader tasks, 64 writer tasks   1.634 ms      │ 10.09 ms      │ 4.119 ms      │ 4.971 ms      │ 100     │ 100
│  ├─ 2 reader tasks, 2 writer tasks    468.9 µs      │ 917.3 µs      │ 617.7 µs      │ 637.6 µs      │ 100     │ 100
│  ├─ 4 reader tasks, 8 writer tasks    742.1 µs      │ 3.01 ms       │ 1.525 ms      │ 1.48 ms       │ 100     │ 100
│  ├─ 8 reader tasks, 8 writer tasks    970.5 µs      │ 3.635 ms      │ 1.915 ms      │ 1.986 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 16 writer tasks  1.903 ms      │ 5.748 ms      │ 2.21 ms       │ 2.576 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 32 writer tasks  4.504 ms      │ 10.46 ms      │ 6.464 ms      │ 6.322 ms      │ 100     │ 100
│  ╰─ 64 reader tasks, 64 writer tasks  10.1 ms       │ 18.84 ms      │ 12.01 ms      │ 12.27 ms      │ 100     │ 100
╰─ rcu_ptr_write_while_reading                        │               │               │               │         │
   ├─ 1 reader tasks, 1 writer tasks    469.3 µs      │ 1.348 ms      │ 560.7 µs      │ 613.7 µs      │ 100     │ 100
   ├─ 1 reader tasks, 8 writer tasks    793.3 µs      │ 5.803 ms      │ 1.625 ms      │ 1.762 ms      │ 100     │ 100
   ├─ 1 reader tasks, 16 writer tasks   818.5 µs      │ 64.52 ms      │ 974.4 µs      │ 2.677 ms      │ 100     │ 100
   ├─ 1 reader tasks, 32 writer tasks   902.6 µs      │ 201.1 ms      │ 4.016 ms      │ 18.98 ms      │ 100     │ 100
   ├─ 1 reader tasks, 64 writer tasks   1.127 ms      │ 199.1 ms      │ 4.997 ms      │ 42.88 ms      │ 100     │ 100
   ├─ 2 reader tasks, 2 writer tasks    688.2 µs      │ 2.18 ms       │ 830.8 µs      │ 894.1 µs      │ 100     │ 100
   ├─ 4 reader tasks, 8 writer tasks    1.319 ms      │ 11.02 ms      │ 2.552 ms      │ 3.249 ms      │ 100     │ 100
   ├─ 8 reader tasks, 8 writer tasks    2.701 ms      │ 3.951 ms      │ 2.894 ms      │ 2.976 ms      │ 100     │ 100
   ├─ 16 reader tasks, 16 writer tasks  6.441 ms      │ 20.34 ms      │ 6.93 ms       │ 7.283 ms      │ 100     │ 100
   ├─ 32 reader tasks, 32 writer tasks  10.39 ms      │ 53.25 ms      │ 12.4 ms       │ 13.93 ms      │ 100     │ 100
   ╰─ 64 reader tasks, 64 writer tasks  16.02 ms      │ 66.2 ms       │ 17.43 ms      │ 20.22 ms      │ 100     │ 100
```

as you can see, `tokio_rcu`'s reads are faster than `arc_swap`'s reads (about 6x-40x faster on average), while `tokio_rcu`'s writes
are slower than `arc_swap`'s writes (about 2x-10x slower on average). for a read-heavy situation, this is ideal.

furthermore, note that when using `arc_swap`, the time it takes for a single read operation seems to scale with the number of
concurrent readers (see the results of the `arc_swap_read_only` and `arc_swap_read_while_writing` benchmarks), while `tokio_rcu`'s
read operation takes roughly the same amount of time regardless of the number of concurrent readers, up until the point where there
are more readers than cpu cores (more than 20 reader tasks), at which point the readers start sharing cpu cores and competing for
their runtime, which obviously takes its toll on the performance.

also note that this constant time for the read operation holds even when writers are concurrently modifying the data - the time
spent on a single read operation remains roughly the same (see `rcu_ptr_read_while_writing`), unlike `arc_swap` (see
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

currently, this crate only works on linux and windows.

the limitation stems from the membarrier operation, which is currently only implemented for linux (using the membarrier syscall),
and windows (using FlushProcessWriteBuffers).

more platforms can be added in the future if needed, and given that they have a way to emulate the behaviour of membarrier.

## license

This project is licensed under the MIT license.

[`on_after_task_poll`]: https://docs.rs/tokio/latest/tokio/runtime/builder/struct.Builder.html#method.on_after_task_poll
[`RcuPtr`]: https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_ptr/struct.RcuPtr.html
[`RcuPtr::read`]: https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_ptr/struct.RcuPtr.html#method.read

<!-- cargo-rdme end -->
