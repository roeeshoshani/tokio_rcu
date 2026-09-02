<!-- cargo-reedme: start -->

<!-- cargo-reedme: info-start

    Do not edit this region by hand
    ===============================

    This region was generated from Rust documentation comments by `cargo-reedme` using this command:

        cargo +nightly reedme

    for more info: https://github.com/nik-rev/cargo-reedme

cargo-reedme: info-end -->

a rust library providing an RCU (read-copy-update) algorithm specifically made for async rust with tokio.

this provides a lock-free and wait-free way to update a shared piece of state while it is concurrently being read and updated by
other tasks.

the core primitive provided by this crate is [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/fn.synchronize_rcu.html), which works just like the `synchronize_rcu` function in the
linux kernel - it waits for an rcu grace period, which allows writers to track when exactly they can reclaim swapped out data.

the [`synchronize_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/fn.synchronize_rcu.html) primitive can be used to build a bunch of data structures and primitives.
the simplest primitive - a single pointer to shared data - is implemented in this crate by the [`Rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu/struct.Rcu.html) type.

# performance

NOTE: this section specifically refers to [`Rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/rcu/struct.Rcu.html), the main high level primitive provided by this crate, but will probably also
apply to most other primitives which can be implemented using this crate.

this crate is speicifcally useful for read-mostly data, as it makes readers extremely fast at the cost of making the writers slower.
when a reader reads the rcu protected data, the read is a single read of an atomic pointer. no memory writes are performed, as opposed
to spinlocks and mutexes which requires memory writes and even syscalls just to access the data.

specifically, for read-mostly data, the cache line containing the pointer can be shared between all readers, and the read operation
becomes a single load from the cpu cache.
compared to spinlocks and mutexes which usually require exclusive ownership over the cacheline due to writes and other atomic ops,
this is much faster and provides much better and predictable reader latency.

also note that the time spent on a read operation is very predictable and static. other users of the data, such as concurrent readers
and even writers, do not affect the time it takes for a reader to read the data. a read is always a single atomic load.
specifically a writer can slightly delay this load due to invalidating the cacheline containing the rcu protected pointer when writing
to it, but this is mostly negligible.

# quick start

```rust
use tokio_rcu::{Rcu, rcu_block_on};

fn main() {
    rcu_block_on(async move {
        let numbers = Rcu::new(Box::new(vec![1, 2, 3, 4]));

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

# realistic use case

for a more realistic use case, see `examples/basic.rs`.

# how does it work?

when a writer swaps out an old data pointer with a new data pointer containing updated data, he must then know when the previous
data pointer can be freed.

to free the data, the writer must first wait for all potential readers, who have already read the pointer and are now using, it to
finish using that pointer, to avoid a UAF (use-after-free) situation.

this problem can be solved in many ways, but rcu usually solves is by defining a state called a “quiescent state”, such that when
a specific execution context (which can be a cpu core, or an OS thread) reaches that quiescent state, it is guaranteed to not hold
any rcu protected pointer.

in this specific crate, the execution contexts are tokio threads, and the quiescent state was chosen to be tokio’s
[`on_after_task_poll`](https://docs.rs/tokio/latest/tokio/runtime/builder/Builder/fn.on_after_task_poll.html) hook.

this works because we limit the usage of rcu protected pointers in a way that prevents them from being held across await points.
so, when the runtime reaches the `on_after_task_poll` hook, it is guaranteed that no future is currently being executed on
the current thread, and since rcu protected pointers can’t be held across await points, it is basically guaranteed that the current
thread is not holding any rcu protected pointers.

waiting an rcu grace period thus means ensuring that all of our previous writes are visible to all other threads, and then just
waiting for each thread to pass at least once through a quiescent state.
after such a grace period, it is guaranteed that any swapped out pointers are no longer used by any of the threads, so their memory
can be reclaimed.

# enabling rcu support

to use the rcu primitives, you must use an rcu enabled tokio runtime.

the easiest way to do this is to use the [`rcu_block_on`](https://docs.rs/tokio_rcu/latest/tokio_rcu/fn.rcu_block_on.html) function which creates a tokio runtime with rcu support enabled, and then
runs the provided future inside that runtime using tokio’s [`block_on`](https://docs.rs/tokio/latest/tokio/runtime/runtime/Runtime/fn.block_on.html).

if you wish to manually configure your runtime, you can use the more low-level [`enable_rcu`](https://docs.rs/tokio_rcu/latest/tokio_rcu/trait.TokioRuntimeBuilderExt.htmlfn.enable_rcu.html) and
[`rcu_block_on`](https://docs.rs/tokio_rcu/latest/tokio_rcu/trait.TokioRuntimeExt.htmlfn.rcu_block_on.html) functions.

# performance overhead

enabling rcu for a tokio runtime does introduce a little bit of overhead.

specifically, this crate uses tokio hooks (e.g. `on_after_task_poll`) to track quiescent states of tokio’s worker threads.

but, this crate performs a lot of efforts to make this overhead as small as possible, especially in hooks like `on_after_task_poll`
which are called very often.

specifically, the current implementation of the `on_after_task_poll` hook is basically just a couple of atomic reads and writes,
and is basically unnoticeable in terms of performance.

# other async runtimes

this crate could quite easily be ported to work with other runtimes other than tokio. i chose tokio because it is the most popular
runtime, and because it already provides hooks which allow me to track quiescent states quite easily.

# stability

this crate currently requires using the `tokio_unstable` configuration of tokio. this is required since the `on_after_task_poll`
hook is currently unstable, and is needed to make this crate work.

<!-- cargo-reedme: end -->
