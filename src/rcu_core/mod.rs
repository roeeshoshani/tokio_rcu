//! the core rcu algorithm, including the logic of the different async runtime hooks, and the logic of waiting a grace period.

pub(crate) mod epoch;
mod notify;
mod per_thread_storage;
mod thread_state;

use crate::{
    loom::{static_or_loom_lazy_static, std::sync::atomic},
    rcu_core::{
        epoch::{EPOCH_ID_MIN, EpochId, epoch_id_get, epoch_id_inc, epoch_id_set},
        notify::Notify,
        per_thread_storage::{
            this_thread_alloc_storage_slot, this_thread_dealloc_storage_slot,
            this_thread_does_have_allocated_storage_slot, this_thread_get_storage_slot_id,
            thread_storage_slot_get_all,
        },
        thread_state::ThreadState,
    },
    utils::{likely, unlikely},
};

static_or_loom_lazy_static! {
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
    /// his incremented epoch id. if we were to start the reset while he was waiting, he would get stuck until the next overflow of the epoch
    /// id.
    static EPOCH_ID_RESET_SYNC_LOCK: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

    /// when a thread increments the epoch id past its max threshold, this thread begins a reset operation.
    /// while that thread was incrementing the epoch id, another thread may have also been incrementing the epoch id, and also saw that it
    /// reached its max threshold. so, that thread also begins the reset operation.
    ///
    /// in practice, the reset is actually only performed by a single thread - the leader, and all other threads that entered reset just wait
    /// for him to finish resetting.
    ///
    /// so, this notification is used by the leader of a reset operation to notify all other threads that have also entered reset that the reset
    /// operation is done.
    static RESET_FINISHED_NOTIFICATION: Notify = Notify::new();
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
    // lock the reset sync lock for reading.
    //
    // this ensures that if any reset operation is currently ongoing, we don't interrupt it by incrementing the epoch id while it
    // is being reset, and we instead wait for it to finish and only then go on with our increment.
    //
    // this exclusivity is guaranteed since during reset the leader of the reset locks the reset sync lock for writing.
    //
    // this also ensures that a reset operation is not initiated while we are still waiting for threads to see our incremented epoch id,
    // since we hold this until we finish waiting.
    let reset_sync_read_guard = EPOCH_ID_RESET_SYNC_LOCK.read().await;

    // increment the epoch id.
    //
    // this is used as a communication primitive with the worker threads.
    // worker threads will then update their last seen epoch id by reading the global epoch id every time they pass through a quiescent
    // state.
    //
    // we can then sample their published last seen epoch id to know when they saw our increment, and once they did, we know that they
    // passed through a quiescent state.
    match epoch_id_inc() {
        Ok(new_epoch_id) => {
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
            )
            .await;

            // ensure that the reset sync read guard is held up until this point.
            // this is important to make sure that a reset operation is not initiated while we are still waiting for threads to see our new
            // epoch id, otherwise we would keep waiting until the next overflow of the epoch id.
            drop(reset_sync_read_guard);
        }
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
                epoch_id_set(
                    EPOCH_ID_MIN,
                    // we don't need any ordering here. this is not yet part of the actual grace period, this is only meant
                    // to reset the epoch id back to its minimum value.
                    // the actually release store that is part of the grace period will be performed later, when incrementing
                    // the post-reset epoch id.
                    //
                    // furthermore, we are more than fine with breaking the release sequence of the epoch id, since we are
                    // currently the only one using it for any operation, due to the write lock of the reset sync lock.
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
                )
                .await;

                // note that at this point, we have basically waited (at least) a grace period, since we incremented the epoch id and waited
                // for everyone to see it.
                //
                // furthermore, note that the grace period also applies to all non-leader waiters that are in reset mode with us, since when
                // we locked the reset lock for writing, we were guaranteed that we see all of their previous memory writes, and we performed
                // the grace period after locking that lock.
                // so, we can just notify them that the grace period is over, and they don't to do any more work.

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

                // the waiter performed the grace period for us, so we are done.
            }
        }
    };
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
        notified.await;

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
        // deadlocks in the waiters. see the SC fence in synchronize rcu after calling `THREAD_EPOCH_UPDATED_NOTIFY.notified()`.
        atomic::fence(atomic::Ordering::SeqCst);
        THREAD_EPOCH_UPDATED_NOTIFY.notify();
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
    // consider the scenario where a thread starts waiting for a notification on the `THREAD_EPOCH_UPDATED_NOTIFY` object. the thread starts
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
    // deadlocks in the waiters. see the SC fence in synchronize rcu after calling `THREAD_EPOCH_UPDATED_NOTIFY.notified()`.
    atomic::fence(atomic::Ordering::SeqCst);
    THREAD_EPOCH_UPDATED_NOTIFY.notify_except_current_thread();
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
    // deadlocks in the waiters. see the SC fence in synchronize rcu after calling `THREAD_EPOCH_UPDATED_NOTIFY.notified()`.
    atomic::fence(atomic::Ordering::SeqCst);
    THREAD_EPOCH_UPDATED_NOTIFY.notify();
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
    THREAD_EPOCH_UPDATED_NOTIFY.notify();
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
        // deadlocks in the waiters. see the SC fence in synchronize rcu after calling `THREAD_EPOCH_UPDATED_NOTIFY.notified()`.
        atomic::fence(atomic::Ordering::SeqCst);
        THREAD_EPOCH_UPDATED_NOTIFY.notify();
    }
}

/// returns whether the calling thread is an rcu tracked thread.
///
/// rcu protected data may only be accessed on rcu tracked threads, since only tracked threads are waited for when waiting a grace period.
pub fn is_rcu_tracked_thread() -> bool {
    this_thread_does_have_allocated_storage_slot()
}
