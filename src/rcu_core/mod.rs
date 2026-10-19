//! the core rcu algorithm, including the logic of the different async runtime hooks, and the logic of waiting a grace period.

pub(crate) mod epoch;
mod epoch_id_update_notifications;
mod notify;
mod per_thread_storage;
mod qs_read_mostly_data;
mod rcu_read_lock;
mod thread_state;

pub use rcu_read_lock::{RcuReadLockGuard, rcu_read_lock, rcu_read_lock_unchecked};

#[cfg(not(loom))]
use crate::rcu_core::notify::Notify;
use crate::{
    loom::{
        static_or_loom_lazy_static,
        std::sync::atomic::{self, AtomicUsize},
    },
    rcu_core::{
        epoch::{EPOCH_ID_MAX, EPOCH_ID_MIN, EpochId, epoch_id_get, epoch_id_set},
        epoch_id_update_notifications::{
            subscribe_to_epoch_id_notifications, thread_epoch_updated_notify,
            thread_epoch_updated_notify_except_cur_thread,
        },
        per_thread_storage::{
            this_thread_alloc_storage_slot, this_thread_dealloc_storage_slot,
            this_thread_does_have_allocated_storage_slot, this_thread_get_storage_slot_id,
            thread_storage_slot_get_all,
        },
        thread_state::ThreadState,
    },
    utils::{likely, unlikely},
};

/// a dummy type used as the inner data type of [`GRACE_PERIODS_SERIALIZATION_MUTEX`]. used to distinguish mutex guards of that mutex from
/// other guards.
///
/// # Safety
///
/// must only be used as the data of the [`GRACE_PERIODS_SERIALIZATION_MUTEX`] mutex.
struct GPSerializationMutexMarker;

static_or_loom_lazy_static! {
    /// a mutex which serializes grace period operations.
    ///
    /// there can only be 1 task which is actively working on performing a grace period at any given moment, but that task may perform the grace period
    /// on behalf of multiple other waiters as well as itself.
    static GP_SERIALIZATION_MUTEX: tokio::sync::Mutex<GPSerializationMutexMarker> =
        tokio::sync::Mutex::const_new(GPSerializationMutexMarker);

    /// a lock used to synchronize the reset operation of the gp counters.
    /// a reset operation is performed when the gp counters overflow, in order to reset the gp counters back to zero.
    ///
    /// when some thread increments the gp started counter and causes it to reach its max value, that thread begins a reset operation.
    /// for resetting the gp counters, the thread must reset the global reset counters back to zero while blocking any further increments
    /// of the counters.
    ///
    /// in order to prevent the further increments of the counters during the reset operation, this lock is used.
    /// all incrementors of the counters lock it for reading before incrementing, and during the reset operation, the leader of the reset (the
    /// first one to increment the counter to its max value) locks this lock for writing, thus preventing any new incrementors from
    /// incrementing the counters.
    ///
    /// this also ensures that we don't start performing a reset operation while some incrementor thread is still in the middle of performing
    /// a grace period using his pre-reset gp counter value.
    static GP_COUNTERS_RESET_SYNC_LOCK: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

    /// when a thread increments the gp started counter to its max value, that thread begins a reset operation.
    /// while that thread was incrementing the counter, another thread may have also been incrementing the counter, and also saw that it
    /// reached its max value. so, that thread also begins the reset operation.
    ///
    /// in practice, the reset is actually only performed by a single thread - the leader, and all other threads that entered reset just wait
    /// for him to finish resetting.
    ///
    /// so, this notification is used by the leader of a reset operation to notify all other threads that have also entered reset that the reset
    /// operation is done.
    static GP_COUNTERS_RESET_DONE: Notify = Notify::new();

    /// a counter incremented every time a grace period is started, used to allocate a serial "grace period id" to each waiter, such that future
    /// waiters that start after that waiter have greater gp id values.
    ///
    /// these allocated gp id values are then published to [`GP_FINISHED_COUNTER`] when a waiter finishes performing a grace period. that way the
    /// waiter can report which gp ids have been waiter for - all those lower than or equal to the published value.
    static GP_STARTED_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// a counter published whenever a full grace period operation is performed. the value published is a gp id value sampled from
    /// [`GP_STARTED_COUNTER`] before the gp operation started, and the published value reports which gp ids have been waiter for - all those
    /// lower than or equal to the published value.
    static GP_FINISHED_COUNTER: AtomicUsize = AtomicUsize::new(0);
}

/// this function just performs an SC fence, but it provides special guarantees when performed directly after modifying the global
/// epoch id.
///
/// performing an SC fence after a global epoch id modification, combined with the SC fences in the thread-start and thread-wake
/// paths (for example, see the SC fence in [`on_thread_unpark`]), provides the guarantee that for every thread other than the calling thread,
/// if we check that thread's state after this fence, either we see that other thread as busy, or that thread sees our epoch id modification and
/// any operation performed before it once his fence is over.
///
/// this then allows us to only consider busy threads when later waiting for threads to see our modified epoch id.
///
/// note that this function is currently unused since in practice we usually achieve the post epoch id modification sc fence
/// by combining it with the SC fence performed by the [`wait_for_running_threads_to_see_epoch_id`] function.
/// but, we still keep this function, mainly for documentation, and as a way to refer to the sc fence that is needed after an
/// epoch id modification.
#[inline(always)]
#[allow(unused)]
fn post_epoch_id_modification_sc_fence() {
    atomic::fence(atomic::Ordering::SeqCst);
}

/// wait for an RCU grace period.
///
/// once this function returns, it is guaranteed that any rcu-protected piece of data that was made unreachable (e.g. by swapping it with
/// another piece of data) before calling this function is now no longer used by any thread in the process, including the current thread.
pub async fn synchronize_rcu() {
    // before doing anything, lock the gp counters reset sync lock for reading, to make sure that we don't start until any currently ongoing
    // reset operations is finished, and to make sure that during our operation, no reset operation will suddenly begin.
    let mut reset_sync_read_guard = GP_COUNTERS_RESET_SYNC_LOCK.read().await;

    // increment the gp counter, while properly handling the overflow case.
    let my_gp_counter = 'inc_gp_loop: loop {
        match GP_STARTED_COUNTER.try_update(
            // for the set part, use release ordering to make sure that any gp performer that loads this value actually also performs the gp for us.
            // this makes sure that the aforementioned gp performer will see all writes performed by us before he starts performing his grace period,
            // which is exactly what we need to ensure that his gp also applies to us.
            atomic::Ordering::Release,
            // no ordering needed for the fetch - we are not ordering anything against older waiters here.
            atomic::Ordering::Relaxed,
            |v| v.checked_add(1),
        ) {
            Ok(prev_value) => {
                // calculate our post increment gp counter value
                let my_gp_counter = prev_value + 1;

                if unlikely(my_gp_counter == usize::MAX) {
                    // our increment caused the counter to reach its max, begin a reset operation.
                    // note that since we are the one who actually performed the exact increment from MAX-1 to MAX, we are the "leader" of the
                    // reset operation, and we do the work for everyone else, and then notify them once we're done.

                    // re-lock the reset sync lock for writing.
                    //
                    // this ensures that any existing gp waiters finish their gp, and that new waiters block until we finish, so we have absolute
                    // exclusive access over the global state during the reset operation.
                    //
                    // once this the reset sync lock is write-locked, every other waiter is in one of two possible states:
                    // - just started the gp operation and is blocked waiting for the initial read lock operation of the reset sync lock.
                    // - entered "slave" reset mode, and is blocked waiting for a reset done notification from us.
                    drop(reset_sync_read_guard);
                    let reset_sync_write_guard = GP_COUNTERS_RESET_SYNC_LOCK.write().await;

                    // reset both counters back to 0. no ordering is needed, the reset sync lock provides the synchronization for us here, it basically
                    // ensures exclusivity.
                    GP_STARTED_COUNTER.store(0, atomic::Ordering::Relaxed);
                    GP_FINISHED_COUNTER.store(0, atomic::Ordering::Relaxed);

                    // release the guard, letting new waiters enter and start incrementing.
                    drop(reset_sync_write_guard);

                    // tell all other waiters who entered reset mode with us that the reset operation is done.
                    GP_COUNTERS_RESET_DONE.notify();

                    // re-grab the reset sync lock for reading, so we can re-try the increment
                    reset_sync_read_guard = GP_COUNTERS_RESET_SYNC_LOCK.read().await;
                } else {
                    // hurray! we got a valid, non overflow, gp counter value, to identify our grace period operation.
                    break my_gp_counter;
                }
            }
            Err(_) => {
                // the gp started counter is already at max. in this case, there must be an ongoing reset operation happening.
                // so, we need to wait for it to finish, then retry.

                // dropping the reset sync read guard will allow the reset leader to enter his critical section. before doing that, we must first
                // start listening to notifications from him, so that we don't miss his reset done notification.
                let reset_done = GP_COUNTERS_RESET_DONE.notified();

                // drop the guard, letting the leader begin his operation.
                drop(reset_sync_read_guard);

                // wait for the leader to finish and to notify us
                reset_done.await;

                // re-grab the reset sync lock for reading and re-try the increment
                reset_sync_read_guard = GP_COUNTERS_RESET_SYNC_LOCK.read().await;
                continue 'inc_gp_loop;
            }
        };
    };

    // enter the critical section which is serialized between all tasks waiting for grace periods.
    let gp_serialization_guard = GP_SERIALIZATION_MUTEX.lock().await;

    // check if someone already did the work for us
    let gp_finished_counter = GP_FINISHED_COUNTER.load(
        // no ordering needed here, instead we use a fence only when the ordering is really needed.
        atomic::Ordering::Relaxed,
    );
    if gp_finished_counter >= my_gp_counter {
        // we need acquire ordering to ensure that we actually see all gp related operations performed by whoever performed the gp for us, so that
        // the gp is actually performed for us.
        atomic::fence(atomic::Ordering::Acquire);

        // since we are not the ones who did the work, it is expected that the finished counter is actually greater than our counter in this case,
        // and never equal.
        debug_assert_ne!(gp_finished_counter, my_gp_counter);

        // someone did the work for us, we are done.
        return;
    }

    // re-load the gp started counter.
    // this ensures that when we perform the actual grace period work below, we do the work for all current waiters in addition to doing it for
    // ourselves.
    // note that we may have slept for a while since the last time we read this counter, which was when we incrmeneted it. since then, other waiters
    // may have also incremented it.
    // if we see their increments, we also see all writes they performed before it, so when we perform our grace period, we basically perform it for
    // them as well, and give them the memory ordering guarantees that they need in regards to writes performed by them before they started the grace
    // period.
    let gp_started_counter = GP_STARTED_COUNTER.load(
        // we need acquire ordering to make sure that wee see all writes performed by other gp waiters before they started their gp, so that we
        // can perform the gp for them as well.
        atomic::Ordering::Acquire,
    );

    synchronize_rcu_serialized(&gp_serialization_guard).await;

    // publish the fact that we finished the gp for all the waiters included in our gp started counter read.
    GP_FINISHED_COUNTER.store(
        gp_started_counter,
        // use release ordering, paired with acquire ordering in loaders who check if we performed the gp for them.
        // this makes sure that if another waiter sees we performed the gp for them, he actually sees all writes performed by us during the gp
        // operation, which ensures he actually sees the gp operation.
        atomic::Ordering::Release,
    );

    // this makes sure that the guard is help up until this point and is not accidentally dropped earlier than needed.
    drop(gp_serialization_guard);

    // the reset sync guard must be held all the way up to this point, as it is assumed to be protecting the entire grace period operation,
    // to prevent any reset operation from starting while we are using pre-reset gp counter values.
    drop(reset_sync_read_guard);
}

/// actually performs the core logic of the grace period operation, which must be serialized using the [`GRACE_PERIODS_SERIALIZATION_MUTEX`] mutex
/// such that only one task is working performing a grace period at any given moment.
async fn synchronize_rcu_serialized(
    gp_serialization_guard: &tokio::sync::MutexGuard<'_, GPSerializationMutexMarker>,
) {
    // before incrementing the epoch id, check for overflow.
    let orig_epoch_id = epoch_id_get(
        // no ordering is needed, we have exclusive access to the epoch id thanks to the gp serialization mutex
        atomic::Ordering::Relaxed,
    );

    if unlikely(orig_epoch_id == EPOCH_ID_MAX) {
        synchronize_rcu_epoch_overflow(gp_serialization_guard).await
    } else {
        synchronize_rcu_no_epoch_overflow(orig_epoch_id, gp_serialization_guard).await
    }
}

/// the synchronize rcu logic in the case where the epoch id is not overflowed.
async fn synchronize_rcu_no_epoch_overflow(
    orig_epoch_id: EpochId,
    gp_serialization_guard: &tokio::sync::MutexGuard<'_, GPSerializationMutexMarker>,
) {
    // increment the epoch id.
    //
    // this is used as a communication primitive with the worker threads.
    // worker threads will then update their last seen epoch id by reading the global epoch id every time they pass through a quiescent
    // state.
    //
    // we can then sample their published last seen epoch id to know when they saw our increment, and once they did, we know that they
    // passed through a quiescent state.
    let new_epoch_id = orig_epoch_id + 2;
    epoch_id_set(
        new_epoch_id,
        // use release ordering to make sure that when a thread loads our epoch id value, he sees all writes performed by us before that.
        // this ensures that when we see that thread publish that epoch id back to us, we are guaranteed that he saw all of our previous writes,
        // for example any pointer swap we have previously performed, so he can't use that old pointer in the future any longer.
        atomic::Ordering::Release,
    );

    // in theory a `post_epoch_id_modification_sc_fence` would be needed here, but the wait operation below already
    // performs that SC fence for us.

    // wait for all threads to see the new epoch id.
    // we only need to consider busy threads thanks to the SC fence.
    //
    // note that we also wait for the current thread here.
    // this is needed as a workaround to remove overhead from the fast-path of the rcu to the slow path.
    // specifically, this helps preventing a specific category of misuse where a user tries to swap an rcu pointer while simultaneously
    // holding a read guard to it on the same thread, for example by manually polling the swap future.
    // making this also wait for the calling thread prevents this misuse from causing a UAF, instead converting it to a deadlock - the
    // wait operation will never finish unless the calling thread actually passes through a quiescent state, at which point he can no longer
    // be holding any read guards.
    // a deadlock is not ideal, but this should never happen during proper use of this library anyway, and it prevents the UAF without
    // adding overhead of checks in the fast path, which is a big win.
    // also note that setting this flag means that the wait operation will always yield at least once, to let the calling thread pass
    // through a quiescent state, even if all threads immediately pass through a quiescent state and see the epoch id increment.
    wait_for_running_threads_to_see_epoch_id(
        |last_seen_epoch_id| last_seen_epoch_id >= new_epoch_id,
        true,
        gp_serialization_guard,
    )
    .await;
}

/// the synchronize rcu logic in the case where the epoch id is overflowed.
async fn synchronize_rcu_epoch_overflow(
    gp_serialization_guard: &tokio::sync::MutexGuard<'_, GPSerializationMutexMarker>,
) {
    // reset the epoch id
    epoch_id_set(
        EPOCH_ID_MIN,
        // we don't need any ordering here. this is not yet part of the actual grace period, this is only meant
        // to reset the epoch id back to its minimum value.
        // the actually release store that is part of the grace period will be performed later, when incrementing
        // the post-reset epoch id.
        //
        // furthermore, we are more than fine with breaking the release sequence of the epoch id, since we are
        // currently the only one using it for any operation, thanks to the gp serialization mutex.
        atomic::Ordering::Relaxed,
    );

    // in theory a `post_epoch_id_modification_sc_fence` would be needed here, but the wait operation below already
    // performs that SC fence for us.

    // wait for all threads to update their last seen epoch id to the reset value.
    // we only need to consider busy threads thanks to the SC fence.
    //
    // as for the busy threads, you may think that us seeing that their last seen epoch id is MIN is not enough, since that MIN may be
    // some stale value they have from a previous reset operation. but, this actually can't happen due to the increment that we perform
    // right after this wait, where we increment to MIN+2 and once again wait for everyone to update to MIN+2.
    //
    // this guarantees that after that increment, every thread will either see MIN+2 or be sleeping but guaranteed to see MIN+2 when he
    // wakes up.
    wait_for_running_threads_to_see_epoch_id(
        |last_seen_epoch_id| last_seen_epoch_id == EPOCH_ID_MIN,
        false,
        gp_serialization_guard,
    )
    .await;

    // increment the epoch id once, to move it away from the reset value.
    // this prevents threads from holding a stale reset value in their state, which may then confuse future reset operations by making
    // them think that a thread saw their reset even though he has the reset value from a previous reset operation.
    epoch_id_set(
        EPOCH_ID_MIN + 2,
        // we want release ordering since this basically represent the epoch id increment, but simpler, since we know what the current
        // value of the epoch id is. see comment in `epoch_id_inc` explaining why release is needed for this operation.
        atomic::Ordering::Release,
    );

    // in theory a `post_epoch_id_modification_sc_fence` would be needed here, but the wait operation below already
    // performs that SC fence for us.

    // wait for all threads to see the epoch id increment, and to move away from the reset value.
    // we only need to consider busy threads thanks to the SC fence.
    //
    // note that here, like in the non-reset increment path, we also need to wait for the calling thread. see the non-reset
    // wait for more info on why this is needed.
    wait_for_running_threads_to_see_epoch_id(
        |last_seen_epoch_id| last_seen_epoch_id == EPOCH_ID_MIN + 2,
        true,
        gp_serialization_guard,
    )
    .await;

    // note that at this point, we have basically waited (at least) a grace period, since we incremented the epoch id and waited
    // for everyone to see it.
}

/// wait for all threads in the process other than the current thread to see some epoch id as implemented in the given predicate
/// which processes the last seen epoch id of each thread.
///
/// this function does not take into account new threads just starting, nor new threads just exiting the busy state.
///
/// if `include_calling_thread` is set, this function also waits for the calling thread itself to see the updated epoch id as implemented
/// in the given predicate. this is usually not needed and should be set to `false`. see [`synchronize_rcu`] for more info.
///
/// this function also performs an SC fence before reading any of the thread states.
async fn wait_for_running_threads_to_see_epoch_id<F: Fn(EpochId) -> bool>(
    last_seen_epoch_id_predicate: F,
    include_calling_thread: bool,
    gp_serialization_guard: &tokio::sync::MutexGuard<'_, GPSerializationMutexMarker>,
) {
    loop {
        // subscribe to the epoch id notifications before checking the current state.
        //
        // if we first check the state and only then start listening, there may be a small window after we finish
        // checking the values but before we start listening where some thread updates its state and notifies
        // us, but we will miss that notification, which is problematic.
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
        // with the acquire ordering of our subscribe operation, it guarantees that we see the state update as happened before the subscribe
        // operation.
        // in case 2, we are guaranteed to at some point see the notification, since the notification hasn't yet been observed by us.
        //
        // as for the overflow behaviour of the subscribe operation, the time window where we hold the returned future before awaiting it
        // is very small, so we shouldn't expect overflow to occur here.
        let epoch_notifications_subscription =
            subscribe_to_epoch_id_notifications(gp_serialization_guard);

        // we need an SC fence here, paired with an SC fence in all notifiers, to prevent the following deadlock scenario:
        // we start listening by calling `notified`, then read each thread's state. the quiescent states on their side first
        // update the thread's state, and then call `notify`.
        // but, there might be a scenario where the quiescent states miss our `notified` registration so their `notify` call
        // does not wake us, but we miss their state update, so we go to sleep, causing a deadlock.
        // this fence prevents that scenario from ever occurring, by making sure that either we see the state update, or they
        // see our `notified` registration. the case where we both miss each other is no longer possible.
        atomic::fence(atomic::Ordering::SeqCst);

        // we must re-calculate this every iteration since our task may be sent between threads every time we await the notified future.
        let this_thread_storage_slot_id = this_thread_get_storage_slot_id();

        // check if all threads have seen our new epoch id
        if thread_storage_slot_get_all()
            .iter_enumerated()
            .all(|(storage_slot_id, storage_slot)| {
                if !include_calling_thread
                    && unlikely(storage_slot_id == this_thread_storage_slot_id)
                {
                    // this slot represents the current thread.
                    // we may or may not need to wait for ourselves, depending on the caller's choice.
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
            })
        {
            // all threads saw our new epoch id, we are done waiting
            break;
        }

        // some of the threads haven't yet seen our new epoch id.
        // so, wait for them to go through a quiescent state and see our new epoch id, or to go to sleep.
        epoch_notifications_subscription.await;

        // in loom mode, add a hint to the loom model that this is a wait loop that depends on other threads to make progress
        // for this to properly continue, otherwise we get stuck in an infinite loop of not seeing the progress made by any
        // of the threads.
        #[cfg(loom)]
        {
            ::loom::thread::yield_now();
        }
    }
}

/// "see" a new epoch id in the current thread.
/// this fetches the current epoch id with a proper memory ordering - an acquire memory ordering, which provides the required
/// guarantees. for example it guarantees that once we see an updated epoch id, we see the swap of the rcu protected pointer
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

pub fn on_thread_stop() {
    // note that at this point, this thread may or may not have a slot allocated to it.
    // this hook is called by both tokio worker threads, and tokio blocking threads.
    // blocking threads will not have a slot at all, since we only allocate a slot in the `on_before_task_poll` hook.
    // tokio worker threads may or may not have a slot, depending on whether they have polled any task throughout their
    // lifetime.
    if this_thread_dealloc_storage_slot() {
        // if we actually had a slot, wake all waiters since some waiters may be waiting for us to see their new epoch id, and we are instead
        // going to stop running so we will never see it.
        // wake them so that they will see that we are no longer busy and thus we are no longer using any of their rcu protected pointers.
        //
        // before performing the notify, we must issue an SC fence, paired with an SC fence in the synchronize rcu logic, to prevent
        // deadlocks in the waiters. see the SC fence in synchronize rcu after calling `thread_epoch_updated_notify().notified()`.
        atomic::fence(atomic::Ordering::SeqCst);
        thread_epoch_updated_notify();
    }
}

pub fn on_thread_park() {
    // the `on_thread_park` hook may be called before a slot is allocated, since a slot is only allocated in `on_before_task_poll`,
    // but a worker thread may decide to park even before polling its first future, for example if there are no tasks to be executed by it.
    if unlikely(!this_thread_does_have_allocated_storage_slot()) {
        return;
    }

    {
        let storage_slot = &thread_storage_slot_get_all()[this_thread_get_storage_slot_id()];

        // mark this thread as non-busy.
        storage_slot.state.fetch_and(
            !1,
            // no special ordering needed here.
            // note that this relaxed store doesn't break the release-sequence of this variable (see c++ memory model for more
            // info), so it doesn't prevent the loader from synchronizing with any previous release ordered store.
            //
            // you may think that we need release, to make sure that when waiters see that we are non-busy, they also see all our previous writes
            // to rcu-protected pointers as happens before that, but this is already guaranteed by the `on_after_task_poll` hook which writes with
            // release ordering, and we are keeping its release-sequence going.
            atomic::Ordering::Relaxed,
        );
    }

    // wake all waiters since some waiters may be waiting for us to see their new epoch id, and we are instead going to sleep
    // so we will never see it.
    // wake them so that they will see that we are no longer busy and thus we are no longer using any of their rcu protected
    // pointers.
    //
    // note that we specifically use `notify_except_current_thread` to avoid having a thread wake itself up immediately as it tries to park.
    // consider the scenario where a thread starts waiting for a notification on the `thread_epoch_updated_notify` object. the thread starts
    // waiting and parks itself. when it parks, this park hook is called, and immediately calls notify on the same `Notify` object that this
    // thread itself just started waiting for. this causes the thread itself to wake itself up as soon as it tried to park, preventing the thread
    // from properly parking and waiting for a notification.
    //
    // so, we use a version of the notify which doesn't wake waiter futures that were last polled on the current thread. this clearly prevents
    // the previously mentioned problem.
    //
    // furthermore, skipping the current thread doesn't create any new problems. if there are other tasks that were last polled on the current
    // thread and are waiting for this notify, they are clearly not waiting for this thread to park. they may actually be waiting for this thread
    // to see their new epoch id, but that is handled through the after poll hook, not the park hook.
    // basically, there's no reason for a task to wait for a notification telling it that the thread that last polled it parks. it will not advance
    // their grace period in any meaningful way.
    // the only scenario where this may be relevant is where a task was last polled on thread A, and the last thread it still needs to wait for is
    // thread A, and then that task gets migrated to thread B. in that case, the fact that A parked will make that task finish its wait.
    // but, this case is already handled by the regular quiescent state hooks. in the aforementioned scenario, once the task is finished being polled
    // on thread A, the `on_after_task_poll` hook on thread A will see a new epoch id, and will thus perform the notify operation.
    //
    // so, having the thread wake itself up when it parks is unnecessary in every possible scenario.
    //
    // before performing the notify, we must issue an SC fence, paired with an SC fence in the synchronize rcu logic, to prevent
    // deadlocks in the waiters. see the SC fence in synchronize rcu after calling `thread_epoch_updated_notify().notified()`.
    atomic::fence(atomic::Ordering::SeqCst);
    thread_epoch_updated_notify_except_cur_thread();
}

pub fn on_thread_unpark() {
    // the `on_thread_unpark` hook may be called before a slot is allocated, since a slot is only allocated in `on_before_task_poll`,
    // but a worker thread may decide to park (and then unpark) even before polling its first future, for example if there are no tasks to
    // be executed by it.
    if unlikely(!this_thread_does_have_allocated_storage_slot()) {
        return;
    }

    let storage_slot = &thread_storage_slot_get_all()[this_thread_get_storage_slot_id()];

    // tell the waiters that we are now back to being busy, and that we are in the process of fetching a new seen epoch id.
    //
    // you may think that we could instead first read the epoch id and then store the busy bit and the new epoch id together in a single
    // write, but this write is specifically used in combination with the SC fence below to guarantee proper happens-before relationships
    // in some scenarios. see docs on the fence below for more info.
    storage_slot.state.store(
        ThreadState {
            last_seen_epoch_id: 0,
            is_busy: true,
        }
        .encode(),
        // no ordering is needed here, this is only used to mark to the waiters that we are busy and that they should wait for us to
        // fetch a new epoch id. the real synchronization with the waiters is when we finally publish the new seen epoch id.
        //
        // but, we still use release ordering just to keep the release-sequence going. previous writes to the state performed by us used release
        // ordering to synchronize with readers, and we don't want to ruin their synchronization.
        atomic::Ordering::Release,
    );

    // this fence, combined with the state store above, is used to protect from the following scenario:
    // a writer swaps a pointer X to Y, increments epoch id from E to E+1. checks all threads, sees some reader thread as sleeping.
    // the reader thread then wakes up, sees the old epoch id E, sees the old pointer X, and uses the old pointer after it was freed.
    //
    // more generically speaking, this fence, combined with the state store above, is used to provide the following guarantee to anyone who
    // modifies the global epoch id and then also performs an SC fence (see `post_epoch_id_modification_sc_fence`): either he sees this thread
    // as busy, or this thread is guaranteed to see his epoch id modification and all writes previously performed by him.
    //
    // this is used to solve the previously mentioned problem by guaranteeing that it will never happen, and it is also used in the reset path
    // by the leader, to guarantee that we see his reset epoch id.
    atomic::fence(atomic::Ordering::SeqCst);

    // fetch a new epoch id and publish it
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

    // we need to notify any potential waiters that saw us as busy with last seen epoch id of 0, and are blocking due to
    // waiting for us to see their new epoch id.
    //
    // before performing the notify, we must issue an SC fence, paired with an SC fence in the synchronize rcu logic, to prevent
    // deadlocks in the waiters. see the SC fence in synchronize rcu after calling `thread_epoch_updated_notify().notified()`.
    atomic::fence(atomic::Ordering::SeqCst);
    thread_epoch_updated_notify();
}

pub fn on_before_task_poll() {
    // note that we only allocate in the `on_before_task_poll` hook, instead of the more reasonable `on_thread_start` hook,
    // since the `on_thread_start` hook is also called by blocking threads, but we only want to account for tokio worker
    // threads in our rcu book-keeping.
    // and, the `on_before_task_poll` hook is obviously only called for tokio worker threads, so it is the ideal place to
    // perform the slot allocation.
    if likely(this_thread_does_have_allocated_storage_slot()) {
        return;
    }

    // the sequence of operations here is exactly the same as `on_thread_unpark`, except that we allocate a slot instead of re-using an
    // existing one.
    //
    // for more info on why this specific sequence of operations is used, see `on_thread_unpark`.
    let slot_id = this_thread_alloc_storage_slot(ThreadState {
        last_seen_epoch_id: 0,
        is_busy: true,
    });

    // see `on_thread_unpark` for more info
    atomic::fence(atomic::Ordering::SeqCst);

    // see `on_thread_unpark` for more info
    let epoch_id = this_thread_see_new_epoch_id();
    thread_storage_slot_get_all()[slot_id].state.store(
        ThreadState {
            last_seen_epoch_id: epoch_id,
            is_busy: true,
        }
        .encode(),
        // no ordering is needed here, but we use release to keep the release-sequence going. previous users of this slot performed release
        // writes to synchronize with readers, and we don't want to ruin their synchronization.
        atomic::Ordering::Release,
    );

    // see `on_thread_unpark` for more info
    atomic::fence(atomic::Ordering::SeqCst);
    thread_epoch_updated_notify();
}

pub fn on_after_task_poll() {
    let new_seen_epoch_id = this_thread_see_new_epoch_id();

    // extra scope to scope the lifetime of the read guard of the storage slots buffer
    let prev_state_encoded = {
        let storage_slot = &thread_storage_slot_get_all()[this_thread_get_storage_slot_id()];
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

        prev_state_encoded
    };

    let prev_state = ThreadState::decode(prev_state_encoded).unwrap();

    // we are expected to be in the busy state while not parked
    debug_assert!(prev_state.is_busy);

    if unlikely(prev_state.last_seen_epoch_id != new_seen_epoch_id) {
        // if the last seen epoch id changed, some waiter may now be able to finish waiting. so, notify all waiters.
        //
        // before performing the notify, we must issue an SC fence, paired with an SC fence in the synchronize rcu logic, to prevent
        // deadlocks in the waiters. see the SC fence in synchronize rcu after calling `thread_epoch_updated_notify().notified()`.
        atomic::fence(atomic::Ordering::SeqCst);
        thread_epoch_updated_notify();
    }
}

/// returns whether the calling thread is an rcu tracked thread. all threads of an rcu enabled runtime are rcu tracked threads.
///
/// rcu protected data may only be accessed on rcu tracked threads, since only tracked threads are waited for when waiting a grace period.
pub fn is_rcu_tracked_thread() -> bool {
    this_thread_does_have_allocated_storage_slot()
}
