# tokio_rcu

<!-- cargo-rdme start -->

a rust library providing an RCU (read-copy-update) algorithm specifically made for async rust with tokio.

this provides a lock-free and wait-free way to update a shared piece of state while it is concurrently being read and updated by
other tasks.

the core primitives provided by this crate are [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_core/fn.synchronize_rcu.html) and [`rcu_read_lock`](https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_core/rcu_read_lock/fn.rcu_read_lock.html), which work just like the `synchronize_rcu` and
`rcu_read_lock` functions in the linux kernel. [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_core/fn.synchronize_rcu.html) waits for an rcu grace period, and [`rcu_read_lock`](https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_core/rcu_read_lock/fn.rcu_read_lock.html) begins an rcu read
side critical section.

the low level [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_core/fn.synchronize_rcu.html) primitive, combined with the [`rcu_read_lock`](https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_core/rcu_read_lock/fn.rcu_read_lock.html) primitive, can be used to build a bunch of higher level
primitives.
all such primitives implemented in this crate are provided in the [`primitives`](https://docs.rs/tokio_rcu/latest/tokio_rcu/primitives/) module.
for example, one very simple higher level primitive implemented by this crate is [`RcuBox`], which is an rcu protected pointer to a heap allocated
piece of data, which can be concurrently read and swapped.

## performance

NOTE: this section specifically refers to [`RcuBox`], but will probably also apply to most other abstractions which can be implemented
using the rcu primitive.

this crate is specifically useful for read-mostly data, as it makes readers extremely fast at the cost of making the writers slower.
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

see also [benchmarks](#benchmarks).

## quick start

```rust
use tokio_rcu::{
    primitives::{rcu_box::RcuBox, rcu_waitable::RcuWaitable},
    rcu_block_on, rcu_read_lock,
};

fn main() {
    rcu_block_on(async move {
        let numbers = RcuBox::new(Box::new(vec![1, 2, 3, 4]));

        // the rcu box's contents can safely be accessed while holding an rcu read lock guard.
        rcu_read_lock(|guard| {
            let numbers = numbers.read(guard);
            assert!(numbers.contains(&3));
            assert!(!numbers.contains(&5));
        });

        // the rcu box's data can be modified while readers are using it.
        // and, the old allocation is returned.
        let new_numbers = Box::new(vec![5, 6, 7, 8]);
        let _old_numbers: Box<Vec<i32>> = numbers.swap(new_numbers).wait().await;

        rcu_read_lock(|guard| {
            let numbers = numbers.read(guard);
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

the easiest way to do this is to use the [`rcu_block_on`](https://docs.rs/tokio_rcu/latest/tokio_rcu/tokio_ext/fn.rcu_block_on.html) function which creates a tokio runtime with rcu support enabled, and then
runs the provided future inside that runtime using tokio's [`block_on`](https://docs.rs/tokio/latest/tokio/runtime/runtime/struct.Runtime.html#method.block_on).

if you wish to manually configure your runtime, you can use the more low-level [`enable_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/tokio_ext/trait.TokioRuntimeBuilderExt.html#tymethod.enable_rcu) and
[`rcu_block_on`](https://docs.rs/tokio_rcu/latest/tokio_rcu/tokio_ext/trait.TokioRuntimeExt.html#tymethod.rcu_block_on) functions.

## performance overhead

enabling rcu for a tokio runtime does introduce a little bit of overhead.

specifically, this crate uses tokio hooks (e.g. [`on_after_task_poll`]) to track quiescent states of tokio's worker threads.

but, this crate makes a significant effort to make this overhead as small as possible, especially in hooks like [`on_after_task_poll`]
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
Timer precision: 37 ns
comparison                              fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ read_only_arc_swap                                 │               │               │               │         │
│  ├─ 1                                 20.31 ms      │ 36.78 ms      │ 20.5 ms       │ 20.84 ms      │ 100     │ 100
│  ├─ 8                                 21.94 ms      │ 29.48 ms      │ 23.65 ms      │ 24.36 ms      │ 100     │ 100
│  ├─ 16                                35.45 ms      │ 44.02 ms      │ 39.69 ms      │ 39.41 ms      │ 100     │ 100
│  ├─ 32                                65.53 ms      │ 70.1 ms       │ 66.73 ms      │ 66.93 ms      │ 100     │ 100
│  ╰─ 64                                129.5 ms      │ 136.8 ms      │ 130.9 ms      │ 131.3 ms      │ 100     │ 100
├─ read_only_rcu_box                                  │               │               │               │         │
│  ├─ 1                                 630.6 µs      │ 920.6 µs      │ 655.1 µs      │ 668.9 µs      │ 100     │ 100
│  ├─ 8                                 629.9 µs      │ 1.788 ms      │ 1.137 ms      │ 1.079 ms      │ 100     │ 100
│  ├─ 16                                938.3 µs      │ 1.733 ms      │ 1.124 ms      │ 1.125 ms      │ 100     │ 100
│  ├─ 32                                1.5 ms        │ 2.566 ms      │ 1.755 ms      │ 1.744 ms      │ 100     │ 100
│  ╰─ 64                                2.908 ms      │ 3.997 ms      │ 3.189 ms      │ 3.3 ms        │ 100     │ 100
├─ read_while_writing_arc_swap                        │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    24.28 ms      │ 29.8 ms       │ 24.62 ms      │ 24.82 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 1 writer tasks    26.59 ms      │ 40.68 ms      │ 31.34 ms      │ 31.88 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 2 writer tasks    34.39 ms      │ 52.12 ms      │ 38.76 ms      │ 39.72 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 1 writer tasks   44.2 ms       │ 56.84 ms      │ 48.45 ms      │ 48.71 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 2 writer tasks   50 ms         │ 73.85 ms      │ 57.28 ms      │ 57.61 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 1 writer tasks   66.64 ms      │ 74.81 ms      │ 68.01 ms      │ 68.62 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 2 writer tasks   67.68 ms      │ 76.28 ms      │ 69.97 ms      │ 70.27 ms      │ 100     │ 100
│  ├─ 64 reader tasks, 1 writer tasks   130.3 ms      │ 137.8 ms      │ 131.9 ms      │ 132.6 ms      │ 100     │ 100
│  ╰─ 64 reader tasks, 2 writer tasks   131.1 ms      │ 142.1 ms      │ 132.9 ms      │ 133.4 ms      │ 100     │ 100
├─ read_while_writing_rcu_box                         │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    639 µs        │ 946.6 µs      │ 664.4 µs      │ 681.6 µs      │ 100     │ 100
│  ├─ 8 reader tasks, 1 writer tasks    829.4 µs      │ 1.359 ms      │ 1.06 ms       │ 1.065 ms      │ 100     │ 100
│  ├─ 8 reader tasks, 2 writer tasks    776.6 µs      │ 1.385 ms      │ 1.179 ms      │ 1.123 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 1 writer tasks   1.041 ms      │ 1.381 ms      │ 1.298 ms      │ 1.291 ms      │ 100     │ 100
│  ├─ 16 reader tasks, 2 writer tasks   1.199 ms      │ 1.595 ms      │ 1.35 ms       │ 1.35 ms       │ 100     │ 100
│  ├─ 32 reader tasks, 1 writer tasks   1.747 ms      │ 9.121 ms      │ 1.938 ms      │ 2.443 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 2 writer tasks   1.852 ms      │ 9.16 ms       │ 2.04 ms       │ 2.111 ms      │ 100     │ 100
│  ├─ 64 reader tasks, 1 writer tasks   3.179 ms      │ 4.388 ms      │ 3.531 ms      │ 3.553 ms      │ 100     │ 100
│  ╰─ 64 reader tasks, 2 writer tasks   3.367 ms      │ 5.228 ms      │ 3.644 ms      │ 3.726 ms      │ 100     │ 100
├─ write_while_reading_arc_swap                       │               │               │               │         │
│  ├─ 1 reader tasks, 1 writer tasks    111.3 µs      │ 221.1 µs      │ 116.1 µs      │ 118.6 µs      │ 100     │ 100
│  ├─ 1 reader tasks, 8 writer tasks    336 µs        │ 649.4 µs      │ 446.9 µs      │ 461.3 µs      │ 100     │ 100
│  ├─ 1 reader tasks, 16 writer tasks   700.4 µs      │ 1.081 ms      │ 879 µs        │ 887.1 µs      │ 100     │ 100
│  ├─ 1 reader tasks, 32 writer tasks   1.475 ms      │ 1.743 ms      │ 1.619 ms      │ 1.62 ms       │ 100     │ 100
│  ├─ 1 reader tasks, 64 writer tasks   2.486 ms      │ 3.307 ms      │ 3.123 ms      │ 3.094 ms      │ 100     │ 100
│  ├─ 2 reader tasks, 2 writer tasks    136.3 µs      │ 612.4 µs      │ 222.7 µs      │ 213.9 µs      │ 100     │ 100
│  ├─ 4 reader tasks, 8 writer tasks    424.6 µs      │ 840 µs        │ 604.2 µs      │ 625.8 µs      │ 100     │ 100
│  ├─ 8 reader tasks, 8 writer tasks    614.2 µs      │ 1.504 ms      │ 739.9 µs      │ 753.9 µs      │ 100     │ 100
│  ├─ 16 reader tasks, 16 writer tasks  1.526 ms      │ 4.074 ms      │ 1.794 ms      │ 1.867 ms      │ 100     │ 100
│  ├─ 32 reader tasks, 32 writer tasks  4.036 ms      │ 7.58 ms       │ 5.371 ms      │ 5.58 ms       │ 100     │ 100
│  ╰─ 64 reader tasks, 64 writer tasks  9.776 ms      │ 14.55 ms      │ 11.65 ms      │ 11.73 ms      │ 100     │ 100
╰─ write_while_reading_rcu_box                        │               │               │               │         │
   ├─ 1 reader tasks, 1 writer tasks    48.67 µs      │ 126.9 µs      │ 76.41 µs      │ 77.36 µs      │ 100     │ 100
   ├─ 1 reader tasks, 8 writer tasks    207.3 µs      │ 425.2 µs      │ 236.1 µs      │ 243.1 µs      │ 100     │ 100
   ├─ 1 reader tasks, 16 writer tasks   351.7 µs      │ 441.1 µs      │ 383.9 µs      │ 386.9 µs      │ 100     │ 100
   ├─ 1 reader tasks, 32 writer tasks   583.8 µs      │ 897.3 µs      │ 709.2 µs      │ 712.1 µs      │ 100     │ 100
   ├─ 1 reader tasks, 64 writer tasks   1.228 ms      │ 1.867 ms      │ 1.367 ms      │ 1.404 ms      │ 100     │ 100
   ├─ 2 reader tasks, 2 writer tasks    165 µs        │ 243.5 µs      │ 188.7 µs      │ 198.3 µs      │ 100     │ 100
   ├─ 4 reader tasks, 8 writer tasks    302.6 µs      │ 404 µs        │ 346.2 µs      │ 349.6 µs      │ 100     │ 100
   ├─ 8 reader tasks, 8 writer tasks    374.2 µs      │ 465.7 µs      │ 419.7 µs      │ 420.7 µs      │ 100     │ 100
   ├─ 16 reader tasks, 16 writer tasks  683.9 µs      │ 762.8 µs      │ 725.4 µs      │ 726.6 µs      │ 100     │ 100
   ├─ 32 reader tasks, 32 writer tasks  1.089 ms      │ 127.5 ms      │ 3.071 ms      │ 11.97 ms      │ 100     │ 100
   ╰─ 64 reader tasks, 64 writer tasks  2.04 ms       │ 136.3 ms      │ 3.375 ms      │ 14.71 ms      │ 100     │ 100
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

furthermore, `cargo-all-features` is used to also test the crate under the `test_mode` feature flag, which is for testing mode
only, and allows testing some internal edge cases of this crate which are extremely hard to reach in the default configuration.

it is also recommended to run the tests in release mode since it increases the probability of being able to find race conditions and
other hard to catch edge cases.

### miri

to run the miri tests, run:
```bash
cargo +nightly all-features miri nextest run
cargo +nightly all-features miri test --doc
```

for miri we run the same regular test suite, but under miri.

### loom

to run the loom tests, run:
```bash
LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg loom" cargo test --test loom --release
```

the loom test suite can be found in `tests/loom/`, and aims to make sure that this crate is actually correct in the memory
ordering sense of things, and that all the specially crafted atomic operations and fences actually provide the required
guarantees.

## platform support

this crate is supported on every platform that is supported by tokio.

## license

This project is licensed under the MIT license.

[`on_after_task_poll`]: https://docs.rs/tokio/latest/tokio/runtime/builder/struct.Builder.html#method.on_after_task_poll
[`RcuBox`]: https://docs.rs/tokio_rcu/latest/tokio_rcu/primitives/rcu_box/struct.RcuBox.html
[`RcuBox::read`]: https://docs.rs/tokio_rcu/latest/tokio_rcu/primitives/rcu_box/struct.RcuBox.html#method.read

<!-- cargo-rdme end -->
