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
when a reader reads the rcu protected data, the read operation is basically just a single load of an atomic pointer.
(plus a write to a non-shared thread local variable, which is only used to track misuse of the rcu primitive, and is negligible in
terms of performance).
no memory writes to shared data are performed, as opposed to spinlocks and mutexes which requires such memory writes, and sometimes
even syscalls, just to access the underlying data.

specifically, for read-mostly data, the cache line containing the pointer can be shared between all readers, and the read operation
basically becomes a single load from the cpu cache (plus the aforementioned negligible thread-local write).
compared to spinlocks and mutexes which usually require exclusive ownership over the cacheline due to writes and other atomic
operations, this is much faster and provides much better reader latency.

also note that the time spent on a read operation is very predictable and static. other users of the data, such as concurrent readers
and even writers, do not affect the time it takes for a reader to read the data. a read is always the same amount of operations.
specifically a writer can slightly delay this load due to invalidating the cacheline containing the rcu protected pointer when writing
to it, but this is mostly negligible.
this can be very important in latency-critical applications which require a high-performance fast path with predictable latency.

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

## testing

to run the tests, use:
```bash
cargo all-features nextest run --release
```

(NOTE: this requires installing `cargo-all-features` and `cargo-nextest`)

`cargo-nextest` is used since it allows running each test as a separate process, which is important for testing this crate, since
this crate heavily relies on thread local variables and generally assumes that only a single tokio runtime is used per process.

furthermore, `cargo-all-features` is used to also test the crate under the `small_epoch_id` feature flag, which is for testing mode
only, and allows testing some internal edge cases of this crate which are extremely hard to reach in the default configuration.

it is also recommended to run the tests in release mode since it increases the probability of being able to find race conditions and
other hard to catch edge cases.

## license

This project is licensed under the MIT license.

[`on_after_task_poll`]: https://docs.rs/tokio/latest/tokio/runtime/builder/struct.Builder.html#method.on_after_task_poll
[`RcuPtr`]: https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu_ptr/struct.RcuPtr.html

<!-- cargo-rdme end -->
