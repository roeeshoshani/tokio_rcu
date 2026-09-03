//! a rust library providing an RCU (read-copy-update) algorithm specifically made for async rust with tokio.
//!
//! this provides a lock-free and wait-free way to update a shared piece of state while it is concurrently being read and updated by
//! other tasks.
//!
//! the core primitive provided by this crate is [`synchronize_rcu`], which works just like the `synchronize_rcu` function in the
//! linux kernel - it waits for an rcu grace period, which allows writers to track when exactly they can reclaim swapped out data.
//!
//! the low level [`synchronize_rcu`] primitive can be used to build a bunch of higher level abstractions.
//! the simplest abstraction - a single pointer to shared data - is implemented in this crate by the [`RcuPtr`] type.
//!
//! # performance
//!
//! NOTE: this section specifically refers to [`RcuPtr`], the main high level abstraction provided by this crate, but will probably also
//! apply to most other abstractions which can be implemented using the rcu primitive.
//!
//! this crate is speicifcally useful for read-mostly data, as it makes readers extremely fast at the cost of making the writers slower.
//! when a reader reads the rcu protected data, the read operation is basically just a single load of an atomic pointer.
//! (plus a write to a non-shared thread local variable, which is only used to track misuse of the rcu primitive, and is negligible in
//! terms of performance).
//! no memory writes to shared data are performed, as opposed to spinlocks and mutexes which requires such memory writes, and sometimes
//! even syscalls, just to access the underlying data.
//!
//! specifically, for read-mostly data, the cache line containing the pointer can be shared between all readers, and the read operation
//! basically becomes a single load from the cpu cache (plus the aforementioned negligible thread-local write).
//! compared to spinlocks and mutexes which usually require exclusive ownership over the cacheline due to writes and other atomic
//! operations, this is much faster and provides much better reader latency.
//!
//! also note that the time spent on a read operation is very predictable and static. other users of the data, such as concurrent readers
//! and even writers, do not affect the time it takes for a reader to read the data. a read is always the same amount of operations.
//! specifically a writer can slightly delay this load due to invalidating the cacheline containing the rcu protected pointer when writing
//! to it, but this is mostly negligible.
//! this can be very important in latency-critical applications which require a high-performance fast path with predictable latency.
//!
//! # quick start
//!
//! ```rust
//! use tokio_rcu::{rcu_block_on, rcu_ptr::RcuPtr};
//!
//! fn main() {
//!     rcu_block_on(async move {
//!         let numbers = RcuPtr::new(Box::new(vec![1, 2, 3, 4]));
//!
//!         // rcu protected values can be accessed using the `with` function.
//!         numbers.with(|numbers| {
//!             assert!(numbers.contains(&3));
//!             assert!(!numbers.contains(&5));
//!         });
//!
//!         // the rcu protected value can be modified while readers are using it.
//!         // and, the old allocation is returned.
//!         let new_numbers = Box::new(vec![5, 6, 7, 8]);
//!         let _old_numbers: Box<Vec<i32>> = numbers.swap(new_numbers).await;
//!
//!         numbers.with(|numbers| {
//!             assert!(numbers.contains(&6));
//!             assert!(!numbers.contains(&10));
//!         });
//!     })
//! }
//! ```
//!
//! # realistic use case
//!
//! for a more realistic use case, see `examples/basic.rs`.
//!
//! # how does it work?
//!
//! when a writer swaps out an old data pointer with a new data pointer containing updated data, he must then know when the previous
//! data pointer can be freed.
//!
//! to free the data, the writer must first wait for all potential readers, who have already read the pointer and are now using it, to
//! finish using that pointer, to avoid a UAF (use-after-free) situation.
//!
//! this problem can be solved in many ways, but rcu usually solves it by defining a state called a "quiescent state", such that when
//! a specific execution context (which can be a cpu core, or an OS thread) reaches that quiescent state, it is guaranteed to not hold
//! any rcu protected pointer.
//!
//! in this specific crate, the execution contexts are tokio threads, and the quiescent state was chosen to be tokio's
//! [`on_after_task_poll`] hook.
//!
//! this works because this crate limits the usage of rcu protected pointers in a way that prevents them from being held across await
//! points.
//! so, when the runtime reaches the [`on_after_task_poll`] hook, it is guaranteed that no future is currently being executed on
//! the current thread, and since rcu protected pointers can't be held across await points, it is basically guaranteed that the current
//! thread is not holding any rcu protected pointers.
//!
//! waiting an rcu grace period thus means first ensuring that all of our previous memory writes (e.g. rcu pointer swaps) are visible to
//! all other threads, and then just waiting for each other thread to pass at least once through a quiescent state.
//! after such a grace period, it is guaranteed that any swapped out pointers are no longer used by any of the threads, so their memory
//! can be reclaimed.
//!
//! # enabling rcu support
//!
//! to use the rcu primitives, you must use an rcu enabled tokio runtime.
//!
//! the easiest way to do this is to use the [`rcu_block_on`] function which creates a tokio runtime with rcu support enabled, and then
//! runs the provided future inside that runtime using tokio's [`block_on`](tokio::runtime::Runtime::block_on).
//!
//! if you wish to manually configure your runtime, you can use the more low-level [`enable_rcu`](TokioRuntimeBuilderExt::enable_rcu) and
//! [`rcu_block_on`](TokioRuntimeExt::rcu_block_on) functions.
//!
//! # performance overhead
//!
//! enabling rcu for a tokio runtime does introduce a little bit of overhead.
//!
//! specifically, this crate uses tokio hooks (e.g. [`on_after_task_poll`]) to track quiescent states of tokio's worker threads.
//!
//! but, this crate performs a lot of efforts to make this overhead as small as possible, especially in hooks like [`on_after_task_poll`]
//! which are called very often.
//!
//! specifically, the current implementation of the [`on_after_task_poll`] hook is basically just a couple of atomic loads and stores,
//! and is unnoticeable in terms of performance.
//!
//! # other async runtimes
//!
//! this crate could quite easily be ported to work with other runtimes other than tokio. i chose tokio because it is the most popular
//! runtime, and because it already provides hooks which allow me to track quiescent states quite easily.
//!
//! # stability
//!
//! this crate currently requires using the `tokio_unstable` configuration of tokio. this is required since the [`on_after_task_poll`]
//! hook is currently unstable, and is needed to make this crate work.
//!
//! [`on_after_task_poll`]: tokio::runtime::Builder::on_after_task_poll
//! [`RcuPtr`]: rcu_ptr::RcuPtr
use std::{sync::atomic, task::Poll};

use crate::{
    epoch::{EPOCH_ID_MIN, EpochId, epoch_id_get, epoch_id_inc, epoch_id_set},
    notify::Notify,
    per_thread_storage::{
        this_thread_alloc_storage_slot, this_thread_dealloc_storage_slot,
        this_thread_does_have_allocated_storage_slot, this_thread_get_storage_slot,
        this_thread_get_storage_slot_id, thread_storage_slot_get_all,
    },
    thread_state::ThreadState,
};

mod atomic_type;
mod epoch;
mod membarrier;
mod notify;
mod per_thread_storage;
pub mod rcu_ptr;
mod thread_state;
mod utils;

use tokio::runtime::RuntimeFlavor;

/// a notification which is notified when threads update their last seen epoch id or change their status in any other meaningful
/// way (e.g. become non-busy). used by waiters to wait for notifications in a blocking manner while waiting for threads to see
/// their new epoch id, instead of constantly busy polling all threads.
static THREAD_EPOCH_UPDATED_NOTIFY: Notify = Notify::new();

/// a lock used to synchronize the reset operation.
/// a reset operation is performed when the epoch id overflows, in order to reset the epoch id back to its minimum value.
///
/// when some thread increments the epoch id and causes it to exceed its max threshold, this thread begins a reset operation.
/// for resetting the epoch id, the thread must reset the global epoch id back to its initial value, then wait for all threads to
/// see this new state while blocking any further increments of the epoch id until all threads see the reset value.
///
/// in order to prevent the further increments of the epoch id during the reset operation, this lock is used.
/// all incrementors of the epoch id lock it for reading before incrementing, and during the reset operation, the leader of the reset (the
/// first one to increment the epoch id past its max threshold) locks this lock for writing, thus preventing any new incrementors from
/// incrementing the epoch id.
///
/// this also ensures that we don't start performing a reset operation while some incrementor thread is still waiting for threads to see
/// his incremented epoch id. if we were to start the reset while we was waiting, we would get stuck until the next overflow of the epoch
/// id.
static EPOCH_ID_RESET_SYNC_LOCK: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

/// when a thread increments the epoch id past its max threshold, this thread begins a reset operation.
/// while that thread was incrementing the epoch id, another thread may have also been incrementing the epoch id, and also saw that it
/// reached its max threshold. so, that thread also begins the reset operation.
///
/// in practice, the reset is actually only performed by a single thread - the leader, and all other threads that entered reset just wait
/// for him to finish resetting.
///
/// so, this notification used by the leader of a reset operation to notify all other threads that have also entered reset that the reset
/// operation is done.
static RESET_FINISHED_NOTIFICATION: Notify = Notify::new();

/// wait for an RCU grace period.
///
/// this function first performs a membarrier to synchronize all previous writes performed by the current thread with all other
/// threads in the process.
///
/// after performing the membarrier, this function waits for every thread that was active during the membarrier operation to pass
/// through a quiescent state or to became unactive.
///
/// a quiescent state of a thread is defined as a state where the thread is not executing any user-defined task, and is instead executing
/// code inside tokio's task scheduling logic.
pub async fn synchronize_rcu() {
    // perform a membarrier to make sure that all other threads see the new rcu pointer.
    membarrier::perform();

    // after the membarrier, all threads are guaranteed to have seen our new pointer.
    // we only need to wait for any potential existing users of the old pointer to finish using it.
    //
    // note that due to the membarrier, we don't need to worry about just-starting threads or just-unparking threads which
    // may access the old pointer.
    //
    // if during the check below, we see that some thread is currently parked, or we don't see the slot of some just-started
    // thread, then it means that this thread's update of its own state happens strictly after the membarrier, and the state
    // update always happens before polling the future, so there's no way for the polled future to see the old pointer.
    // the relationship is:
    // pointer swap -> membarrier -> thread's update of his own state -> thread's load of the rcu protected pointer
    // thus, all such threads are guaranteed to see the new pointer, and we can thus ignore them when waiting for all existing
    // users.

    // lock the reset sync lock for reading.
    //
    // this ensures that if any reset operation is currently ongoing, we don't interrupt it by incremented the epoch id while it
    // is being reset, and we instead wait for it to finish and only then go on with our increment.
    //
    // this exclusivity is guaranteed since during reset the leader of the reset locks the reset sync lock for writing.
    //
    // this also ensures that a reset operation is not initiated while we are still waiting for threads to see our incremented epoch id,
    // since we hold this until we finish waiting.
    let mut reset_sync_read_guard = EPOCH_ID_RESET_SYNC_LOCK.read().await;

    // increment the epoch id.
    //
    // this is used as a communication primitive with the worker threads.
    // worker threads will then update their last seen epoch id by reading the global epoch id every time they pass through a quiescent
    // state.
    //
    // we can then sample their published last seen epoch id to know when they saw our increment, and once they did, we know that they
    // passed through a quiescent state.
    let new_epoch_id = match epoch_id_inc() {
        Ok(v) => v,
        Err(err) => {
            // epoch id overflow.

            // perform a reset of the epoch id
            if err.am_i_the_leader {
                // re-lock the reset sync lock for writing.
                //
                // once we succeed grabbing the write lock, it is guaranteed that:
                // - all previous waiters finished waiting for their grace period
                // - all non-leader waiters that also entered reset mode have started listening to the reset
                // - all new waiters will be blocked until we finish.
                drop(reset_sync_read_guard);
                let reset_sync_write_guard = EPOCH_ID_RESET_SYNC_LOCK.write().await;

                // reset the epoch id
                epoch_id_set(EPOCH_ID_MIN, atomic::Ordering::Relaxed);

                // make sure that all threads see the reset of the epoch id.
                membarrier::perform();

                // wait for all threads to update their last seen epoch id to the reset value.
                //
                // note that parked and not yet started threads are not relevant here, since once they wake up they will see the updated
                // reset value of the epoch id due to the membarrier, and they will fetch and publish it along with the enabling of the
                // busy flag as soon as they unpark.
                wait_for_running_threads_to_see_epoch_id(|last_seen_epoch_id| {
                    last_seen_epoch_id == EPOCH_ID_MIN
                })
                .await;

                // at this point, all running threads have reset their last seen epoch id, and new threads are guaranteed
                // to see at least the reset value.

                // now that we finished resetting the epoch id, we can now let new waiters in.
                drop(reset_sync_write_guard);

                // wake all non-leader waiters that are in reset mode waiting for us to finish.
                RESET_FINISHED_NOTIFICATION.notify();
            } else {
                // start listening to reset notification from the leader.
                //
                // this must be done before dropping the read lock, so that the leader doesn't start acting before we are listening
                // to notifications from him.
                //
                // as for the overflow behaviour of `notified`, the time window where we hold the returned future before awaiting it
                // is very small, so we shouldn't expect overflow to occur here.
                let event = RESET_FINISHED_NOTIFICATION.notified();

                // let the leader start doing its thing.
                drop(reset_sync_read_guard);

                // wait for the leader to finish the reset operation and notify us.
                event.await;
            }

            // done resetting epoch id

            // re-lock the reset sync guard just in case, even though we shouldn't expect another reset any time soon.
            // note that the lock should be unlocked now since the writer unlocks it before waking us up.
            reset_sync_read_guard = EPOCH_ID_RESET_SYNC_LOCK.try_read().unwrap_or_else(|_| {
                panic!("another epoch id reset right after the previous reset")
            });

            let Ok(new_epoch_id) = epoch_id_inc() else {
                // avoid poisoning the lock
                drop(reset_sync_read_guard);

                // we should never get another overflow right after we finish resetting.
                // the epoch id should take some time to grow before it wraps around again.
                panic!("overflow when incrementing epoch id after reset")
            };

            new_epoch_id
        }
    };

    // note that parked and not-yet-started threads are irrelevant here since they are guaranteed to see the new pointer
    // due to the membarrier.
    wait_for_running_threads_to_see_epoch_id(|last_seen_epoch_id| {
        last_seen_epoch_id >= new_epoch_id
    })
    .await;

    // ensure that the reset sync read guard is held up until this point.
    // this is important to make sure that a reset operation is not initiated while we are still waiting for threads to see our new
    // epoch id, otherwise we would keep waiting until the next overflow of the epoch id.
    drop(reset_sync_read_guard);
}

/// wait for all other threads in the process other than the current thread to see some epoch id as implemented in the given predicate
/// which processes the last seen epoch id of each thread.
///
/// this function does not take into account new threads just starting, nor new threads just existing the busy state.
async fn wait_for_running_threads_to_see_epoch_id<F: Fn(EpochId) -> bool>(
    last_seen_epoch_id_predicate: F,
) {
    loop {
        // start subscribing to the notified waiters event before checking the current state.
        //
        // if we first check the state and only then start listening, there may be a small window after we finish
        // checking the values but before we start listening where some thread updates its counter and notifies
        // all wakers, but we will miss that notification, which is problematic.
        //
        // so, we start listening before checking the values, so that even notifications that are issued while
        // or right after we finished checking are still received.
        //
        // note that this registration operation provides acquire ordering against any previous notifiers, so we won't miss
        // any state updates.
        // to prove this, we can split our situation with the readers into 2 cases:
        // 1. a thread already notified before we registered.
        // 2. a thread hasn't already notified when we registered.
        // in case 1, we are guaranteed to see this thread's state update since the notify operation has release ordering, and paired
        // with the acquire ordering of our registration, it guarantees that we see the state update as happened before the notify
        // operation.
        // in case 2, we are guaranteed to at some point see either the state update or the notification, since the notification
        // hasn't yet been observed by us.
        //
        // as for the overflow behaviour of `notified`, the time window where we hold the returned future before awaiting it
        // is very small, so we shouldn't expect overflow to occur here.
        let notified = THREAD_EPOCH_UPDATED_NOTIFY.notified();

        // we must re-calculate this every iteration since our task may be sent between threads every time we await the notified future.
        let this_thread_storage_slot_id = this_thread_get_storage_slot_id();

        // check if all threads have seen our new epoch id
        if thread_storage_slot_get_all().all(|(storage_slot_id, storage_slot)| {
            if storage_slot_id == this_thread_storage_slot_id {
                // this slot represents the current thread. no need to wait for ourselves.
                //
                // note that if we didn't do this, then our synchronize rcu implementation would always block at least once, due
                // to having to yield at least once to let the current thread pass through a quiescent state.
                // this would be very wasteful and unnecessarily slow.
                return true;
            }
            let encoded_state = storage_slot.state.load(
                // we use acquire ordering paired with a release ordering for the store to make sure that the stores to the data
                // pointed at by the rcu protected pointer happen before we see the store to the state.
                // this is important in order to guarantee that we don't see those writes after we free the protected pointer, which will
                // lead to a UAF.
                atomic::Ordering::Acquire,
            );

            let Some(state) = ThreadState::decode(encoded_state) else {
                // if the slot is empty, ignore it.
                // it may at some point be allocated by some new thread that just started, but in this function we explicitly ignore
                // new threads.
                return true;
            };

            if !state.is_busy {
                // this thread is currently not busy running any future.
                // it may start running as soon as we finished checking it, but in this function we explicitly ignore non busy threads.
                return true;
            }

            last_seen_epoch_id_predicate(state.last_seen_epoch_id)
        }) {
            // all threads saw our new epoch id, we are done waiting
            break;
        }

        // some of the threads haven't yet seen our new epoch id.
        // so, wait for them to go through a quiescent state and see our new epoch id, or to go to sleep.
        notified.await;
    }
}

/// "see" a new epoch id in the current thread.
/// this fetches the current epoch id with a proper memory ordering - a release memory ordering, which provides the required
/// guaranteed, for example it guarantees that once we see an updated epoch id, we see the swap of the rcu protected pointer
/// as happened before that store to the epoch id.
fn this_thread_see_new_epoch_id() -> EpochId {
    epoch_id_get(
        // we use acquire ordering coupled with a release ordering when incrementing the epoch id to make sure that we see swap of the rcu
        // protected pointer before we see the increment of the epoch id.
        //
        // if we were to first see the increment of the epoch id, and only then see the swap of the pointer, we may publish that we have
        // seen the new epoch id, causing the waiter to free the memory, and then still use the old and now freed pointer since we haven't
        // yet seen the pointer swap.
        atomic::Ordering::Acquire,
    )
}

fn on_thread_start() {
    assert!(!this_thread_does_have_allocated_storage_slot());
    let epoch_id = this_thread_see_new_epoch_id();
    this_thread_alloc_storage_slot(ThreadState {
        last_seen_epoch_id: epoch_id,
        is_busy: true,
    });
}

fn on_thread_stop() {
    assert!(this_thread_does_have_allocated_storage_slot());
    this_thread_dealloc_storage_slot();
}

fn on_thread_park() {
    let storage_slot = this_thread_get_storage_slot();

    // mark this thread as non-busy.
    storage_slot.state.fetch_and(
        !1,
        // no special ordering needed here.
        // note that this relaxed store doesn't break the release-sequence of this variable (see c++ memory model for more
        // info), so it doesn't prevent the loader from synchronizing with any previous release ordered store.
        atomic::Ordering::Relaxed,
    );

    // wake all waiters since some waiters may be waiting for us to see their new epoch id, and we are instead going to sleep
    // so we will never see it.
    // wake them so that they will see that we are no longer busy and thus we are no longer using any of their rcu protected
    // pointers.
    THREAD_EPOCH_UPDATED_NOTIFY.notify();
}

fn on_thread_unpark() {
    let storage_slot = this_thread_get_storage_slot();

    // note that in addition to setting the is busy flag here, we also need to see a new epoch id.
    //
    // this is needed for the case where a reset operation was performed since we last went to sleep.
    // in that case, if we wake up and set the busy flag without updating the epoch id, some thread that had already incremented
    // the epoch id since the reset may think that we saw his epoch id increment since we have a stale high epoch id value, even
    // though in practice we didn't really see his epoch id increment.
    let new_seen_epoch_id = this_thread_see_new_epoch_id();
    storage_slot.state.store(
        ThreadState {
            last_seen_epoch_id: new_seen_epoch_id,
            is_busy: true,
        }
        .encode(),
        // we use release ordering to make sure that all writes to the data pointed at by the rcu protected pointer happen before this
        // store so that no writes happen after the data is freed.
        // this is needed since we actually fetch a new epoch id here, not only set the busy flag.
        atomic::Ordering::Release,
    );
}

fn on_after_task_poll() {
    let storage_slot = this_thread_get_storage_slot();
    let new_seen_epoch_id = this_thread_see_new_epoch_id();

    // at this point we want to swap the current state with the new state.
    // we could do that using the atomic `swap` operation, but we can do something more performant while still maintaining correctness.
    //
    // the slot's data is loaded from multiple threads, but it is only written to by the current thread who owns that slot.
    // we can use that fact to split the atomic `swap` operation into a `load` and then a `store`, while still being guaranteed that no
    // one will modify the value between the `load` and the `store`, since the current thread are the only one allowed to modify the
    // value.
    //
    // as for why this is more efficient, the load-then-store method requires looser memory ordering guarantees, and thus provides more
    // flexibility for optimization by the hardware's memory subsystem.
    //
    // for example, on x86, the load then store will be translated to just 2 simple `MOV` instructions, while a `swap` would have been
    // translated to a `LOCK XCHG` instruction, which requires much more effort from the hardware.
    let prev_state_encoded = storage_slot.state.load(
        // we don't need any special ordering, since this thread is the only entity which can write to this variable.
        // so, the returned value is sequentially consistent with the execution order of the code in this thread.
        //
        // also, we don't need to synchronize this load against any other shared variables, since the returned value is only used to
        // check whether it was different than the newly written value, and is thus not used in combination with any other shared state.
        atomic::Ordering::Relaxed,
    );
    storage_slot.state.store(
        ThreadState {
            last_seen_epoch_id: new_seen_epoch_id,
            is_busy: true,
        }
        .encode(),
        // we use release ordering to make sure that all writes to the data pointed at by the rcu protected pointer happen before this
        // store so that no writes happen after the data is freed.
        atomic::Ordering::Release,
    );

    let prev_state = ThreadState::decode(prev_state_encoded).unwrap();

    // we are expected to be in the busy state while not parked
    debug_assert!(prev_state.is_busy);

    if prev_state.last_seen_epoch_id != new_seen_epoch_id {
        // if the last seen epoch id changed, some waiter may now be able to finish waiting. so, notify all waiters.
        THREAD_EPOCH_UPDATED_NOTIFY.notify();
    }
}

/// extension methods for tokio's runtime builder.
pub trait TokioRuntimeBuilderExt {
    /// enable rcu support for this tokio runtime.
    /// must be called when constructing the runtime in order to use any rcu related primitive inside the runtime.
    ///
    /// # Safety
    ///
    /// when used, in order to use any of the rcu primitives safely, you must wrap the [`rcu_block_on`](TokioRuntimeExt::rcu_block_on)
    /// function to run the main future on the runtime. using [`block_on`](tokio::runtime::Runtime::block_on) directly is forbidden.
    ///
    /// furthermore, after calling this function, you must not register any tokio hooks of your own, since this functions registers the
    /// rcu hooks needed for book-keeping. overriding any of those hooks will lead to undefined behaviour.
    unsafe fn enable_rcu(&mut self) -> &mut Self;
}

impl TokioRuntimeBuilderExt for tokio::runtime::Builder {
    unsafe fn enable_rcu(&mut self) -> &mut Self {
        assert!(membarrier::is_supported());
        membarrier::register();

        self.on_thread_start(|| {
            on_thread_start();
        })
        .on_thread_stop(|| {
            on_thread_stop();
        })
        .on_thread_park(|| {
            on_thread_park();
        })
        .on_thread_unpark(|| {
            on_thread_unpark();
        })
        .on_after_task_poll(|_| {
            on_after_task_poll();
        })
    }
}

/// extension methods for tokio's runtime.
pub trait TokioRuntimeExt {
    /// runs a future to completion on the tokio runtime, with RCU support.
    ///
    /// this can only be used with multi-threaded runtimes.
    ///
    /// # Safety
    ///
    /// to use this, you must first call [`enable_rcu`](TokioRuntimeBuilderExt::enable_rcu) when building the runtime.
    unsafe fn rcu_block_on<F: Future>(&self, future: F) -> F::Output;
}
impl TokioRuntimeExt for tokio::runtime::Runtime {
    unsafe fn rcu_block_on<F: Future>(&self, future: F) -> F::Output {
        // rcu is only supported for multithreaded runtimes
        assert_eq!(self.handle().runtime_flavor(), RuntimeFlavor::MultiThread);

        self.block_on(unsafe {
            // SAFETY: we pass the wrapped future directly to `block_on`
            RcuRootFuture::new(future)
        })
    }
}

/// runs the provided future inside a new multi-threaded tokio runtime with all features enabled and with rcu support.
pub fn rcu_block_on<F: Future>(future: F) -> F::Output {
    unsafe {
        // SAFETY: we use `rcu_block_on`
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .enable_rcu()
            .build()
            .unwrap();

        // SAFETY: we called `enable_rcu`
        rt.rcu_block_on(future)
    }
}

/// a wrapper around the root future of a tokio `block_on` call.
///
/// this is required since tokio's hooks only apply to tokio's worker threads, but not to the main thread which initially calls `block_on`.
///
/// but, we need the main thread to also perform the book-keeping needed by the rcu primitive, in order for it to be able use the rcu
/// primitives and to interact with the other threads using the rcu primitives.
///
/// so, we wrap the main future passed to `block_on` in a custom wrapper which emulates the call to the different worker hooks.
/// this lets the main thread participate in the book-keeping like any other worker thread.
#[derive(Debug, Clone, Copy)]
struct RcuRootFuture<F> {
    inner_future: F,
    has_already_been_polled: bool,
}
impl<F> RcuRootFuture<F> {
    /// wraps the provided future with the rcu root future logic.
    ///
    /// # Safety
    ///
    /// may only be used to wrap the future provided to tokio's `block_on` function on a multithreaded runtime.
    /// using this incorrectly will lead to undefined behaviour.
    unsafe fn new(inner_future: F) -> Self {
        Self {
            inner_future,
            has_already_been_polled: false,
        }
    }
}
impl<F: Future> Future for RcuRootFuture<F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        if !self.has_already_been_polled {
            // first time being polled on the main thread.
            // mark the thread's start.
            on_thread_start();

            // SAFETY: we don't move out of anything
            unsafe { self.as_mut().get_unchecked_mut().has_already_been_polled = true }
        } else {
            // we have already been polled in the previous iteration.
            //
            // we have returned `Poll::Pending` in the previous iteration, so the main thread parked itself and went to sleep,
            // and now we are being polled again.
            //
            // this is basically an unpark.
            on_thread_unpark();
        }

        // SAFETY: we do not move out of anything, we just project a field, which is safe
        let inner_future = unsafe { self.map_unchecked_mut(|x| &mut x.inner_future) };

        let res = inner_future.poll(cx);

        // just finished polling the task.
        on_after_task_poll();

        match res {
            Poll::Ready(_) => {
                // in this case, the main future is done, so the main thread is also done.
                on_thread_stop();
            }
            Poll::Pending => {
                // if we return `Poll::Pending`, the main thread will park itself until an IO event occurs and wakes it up.
                //
                // so this is basically a thread park.
                on_thread_park();
            }
        }

        res
    }
}
