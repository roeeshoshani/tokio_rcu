// TODO: add module level docs

use std::task::{Poll, Waker};

use crate::{
    loom::{
        static_or_loom_lazy_static,
        std::{
            pin::Pin,
            sync::atomic::{self, AtomicBool, AtomicUsize},
            thread::{self, ThreadId},
        },
    },
    rcu_core::GPSerializationMutexMarker,
};

static_or_loom_lazy_static! {
    /// the total number of notify operations ever performed.
    /// incremented by 1 each time someone sends a notification.
    /// this value safely wraps around on overflow, while still maintaining the correctness of the algorithm, except for
    /// very extreme cases, which must be dealt with by users of this primitive, and are not our concern here.
    static NUM_NOTIFICATIONS: AtomicUsize = AtomicUsize::new(0);

    /// a hint on whether there currently is any subscriber.
    ///
    /// if this reads as false, notifiers can assume that there is no currently active subscriber and can just do nothing.
    /// if this reads as true, there may or may not actually be an active subscriber, but notifiers must manually check [`SUBSCRIBER_INFO`] to see.
    ///
    /// this is used as an optimization, so that in the case where there is no active subscriber, the notify operations is read only, and does not
    /// perform any writes to shared memory, which keeps it extremely fast, especially when multiple cpu cores simultanously perform many notify
    /// operations.
    // TODO: move to qs read mostly data
    static HAS_SUBSCRIBER: AtomicBool = AtomicBool::new(false);

    /// full information about the currently active subscriber.
    static SUBSCRIBER_INFO: crate::loom::parking_lot::RwLock<Option<SubscriberInfo>> = crate::loom::parking_lot::RwLock::new(None);
}

/// full information about the currently active subscriber.
struct SubscriberInfo {
    /// the subscriber's waker.
    waker: Waker,

    /// the subscriber's thread id, which is the thread id of the thread on which the subscriber was last polled.
    thread_id: ThreadId,
}

/// subscribe to epoch id update notifications, returning a future which when awaited will wait for a notification.
///
/// when this function returns, the returned future has already properly registered itself and is listening for notifications.
/// any notification received after this function returns, even if it wasn't `poll`ed or `await`ed yet, will be received by the
/// returned future, and once `poll`ed it will complete immediately.
///
/// when you are finished awaiting the returned future, it provides acquire memory ordering against the notifier who notified you.
///
/// # overflow
///
/// note that if after the registration and before the first poll of the returned future, `usize::MAX + 1` calls to `notify` are
/// performed, all of those wakeups would be missed, and awaiting the returned future will block, even though the `notify` calls
/// should have woke the returned future up, since it was already registered when those calls were made.
///
/// this is a known limitation of the current implementation, and when using this primitive, you should be aware of it, and make sure
/// that your code works properly even in such extreme edge cases.
pub fn subscribe_to_epoch_id_notifications<'a>(
    gp_serialization_guard: &'a tokio::sync::MutexGuard<'a, GPSerializationMutexMarker>,
) -> EpochIdNotificationsSubscriber<'a> {
    EpochIdNotificationsSubscriber::new(gp_serialization_guard)
}

/// the result of a subscriber registering his subscriber info.
enum RegisterSubscriberInfoResult {
    /// the subscriber info was successfully registered, and is waiting for notifications.
    Ok,

    /// a previous regitration of the subscriber's info has been consumed by a notifier, so the subscriber should wake up, and is finished waiting.
    WakeUp,
}

/// a subscriber, waiting for notifications for epoch id updates.
pub struct EpochIdNotificationsSubscriber<'a> {
    num_notifications_snapshot: usize,

    had_already_registered: bool,
    did_someone_consume_my_subscriber_info: bool,

    // not really used, just used to enforce that only one task is using this at any given moment.
    // this algorithm is explicitly designed to support only the single waiter case, since our gp waiters are serialized.
    _gp_serialization_guard: &'a tokio::sync::MutexGuard<'a, GPSerializationMutexMarker>,
}
impl<'a> EpochIdNotificationsSubscriber<'a> {
    fn new(
        gp_serialization_guard: &'a tokio::sync::MutexGuard<'a, GPSerializationMutexMarker>,
    ) -> Self {
        HAS_SUBSCRIBER.store(
            true,
            // no ordering is needed here. see the load of `HAS_SUBSCRIBER` for more info.
            atomic::Ordering::Relaxed,
        );

        Self {
            _gp_serialization_guard: gp_serialization_guard,
            num_notifications_snapshot: NUM_NOTIFICATIONS.load(
                // no special ordering is needed here, since this is only synchronizing with old notifiers, and we don't care about them, only
                // new notifiers which actually wake us up are relevant.
                // TODO: is it ok that we don't synchronize with old notifiers? think this through
                atomic::Ordering::Relaxed,
            ),
            had_already_registered: false,
            did_someone_consume_my_subscriber_info: false,
        }
    }

    #[inline]
    fn did_num_notifications_increment_since_snapshot(&self) -> bool {
        let new_num_notifications = NUM_NOTIFICATIONS.load(
            // no ordering here, we instead use a fence only when an ordering is really needed
            atomic::Ordering::Relaxed,
        );
        if new_num_notifications != self.num_notifications_snapshot {
            // got some notifications since we started listening

            // need acquire ordering to synchronize with the notifier who woke us up.
            // not relevant to this primitive itself, but relevant to users of this primitive.
            atomic::fence(atomic::Ordering::Acquire);

            return true;
        }

        false
    }

    #[inline]
    fn register_subscriber_info(
        self: Pin<&mut Self>,
        cx: &std::task::Context,
    ) -> RegisterSubscriberInfoResult {
        if self.had_already_registered {
            // we had already registered ourselves at some point, check if the existing info is already correct
            let subscriber_info_opt = SUBSCRIBER_INFO.read();
            match subscriber_info_opt.as_ref() {
                Some(subscriber_info) => {
                    let cur_tid = thread::current().id();
                    let cur_waker = cx.waker();

                    // check if each part of the existing subscriber info is good and can be re-used
                    let is_tid_good = subscriber_info.thread_id == cur_tid;
                    let is_waker_good = subscriber_info.waker.will_wake(cur_waker);

                    if is_tid_good && is_waker_good {
                        // fast path: we can re-use the existing waker and thread id, and we don't need to lock the subscriber info for writing.
                    } else {
                        // slow path: some of the info needs updating, we need to lock the subscriber info for writing.

                        // first drop the original read lock
                        let _ = subscriber_info;

                        // now lock for writing
                        let mut subscriber_info_opt = SUBSCRIBER_INFO.write();
                        // SAFETY: we already know that it is not `None`, and no-one could have written to it since we dropped the read lock, since
                        // the gp serialization guard ensures we have exclusive access.
                        let subscriber_info =
                            unsafe { subscriber_info_opt.as_mut().unwrap_unchecked() };
                        if !is_tid_good {
                            subscriber_info.thread_id = cur_tid;
                        }
                        if !is_waker_good {
                            subscriber_info.waker = cur_waker.clone();
                        }
                    }

                    RegisterSubscriberInfoResult::Ok
                }
                None => {
                    // we registered ourselves, and someone consumed that info. this means someone notified us, so we are done waiting.

                    // SAFETY: we don't move out of self
                    unsafe {
                        self.get_unchecked_mut()
                            .did_someone_consume_my_subscriber_info = true
                    };

                    RegisterSubscriberInfoResult::WakeUp
                }
            }
        } else {
            // first time being polled, need to register ourselves
            let mut subscriber_info_opt = SUBSCRIBER_INFO.write();
            *subscriber_info_opt = Some(SubscriberInfo {
                waker: cx.waker().clone(),
                thread_id: thread::current().id(),
            });

            // mark that we have registered outselves.
            // SAFETY: we don't move out of self
            unsafe { self.get_unchecked_mut().had_already_registered = true };

            RegisterSubscriberInfoResult::Ok
        }
    }
}

impl<'a> Future for EpochIdNotificationsSubscriber<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.did_num_notifications_increment_since_snapshot() {
            return Poll::Ready(());
        }

        match self.as_mut().register_subscriber_info(cx) {
            RegisterSubscriberInfoResult::Ok => {}
            RegisterSubscriberInfoResult::WakeUp => return Poll::Ready(()),
        }

        // before actually going to sleep, check the num notifications again.
        //
        // this is needed because since we last checked, during the time we registered our waker, someone may had woke us up.
        //
        // if we don't check this, we may miss a notifier who woke us up before we registered our waker, but after we initially checked
        // the number of notifications. missing this would cause us to incorrectly yield, even though we should wake up.
        if self.did_num_notifications_increment_since_snapshot() {
            return Poll::Ready(());
        }

        Poll::Pending
    }
}

impl<'a> Drop for EpochIdNotificationsSubscriber<'a> {
    fn drop(&mut self) {
        // reset the `HAS_SUBSCRIBER` flag back to false.
        //
        // as an optimization, we can skip this write to shared memory if someone had consumed our waker, since in that case, that someone had
        // already reset the flag to false for us.
        if !self.did_someone_consume_my_subscriber_info {
            HAS_SUBSCRIBER.store(
                false,
                // no ordering is needed here. see the load of `HAS_SUBSCRIBER` for more info.
                atomic::Ordering::Relaxed,
            );
        }

        // if we registered ourselves, remove that stale data, if any
        if self.had_already_registered {
            *SUBSCRIBER_INFO.write() = None;
        }
    }
}

pub fn thread_epoch_updated_notify() {
    thread_epoch_updated_notify_inner(false);
}

pub fn thread_epoch_updated_notify_except_cur_thread() {
    thread_epoch_updated_notify_inner(true);
}

fn thread_epoch_updated_notify_inner(skip_waiter_if_current_thread: bool) {
    if !HAS_SUBSCRIBER.load(
        // we don't need any ordering here, since we don't need any synchronization with writers of this variable.
        //
        // there are 2 types of writers to this variable: the waiter that registers and unregisters himself, and other
        // notifiers, that write to it when notifying the waiter, to unregister him.
        //
        // for the waiter part, we don't need acquire ordering here, since waiters dont do any special preparation before
        // writing to this field.
        //
        // for the notifiers part, we also don't need acquire, since notifiers only set this to false, and when this is false,
        // we just do nothing, so it doesn't require any further synchronization.
        atomic::Ordering::Relaxed,
    ) {
        // fast path: no active waiter, no need to do anything.
        // note that this fast path is especially fast since it is read only - no write operations are performed.
        // so, in the fast path, all notifier threads don't fight over exclusivity over cache-lines, they can all
        // share the cache line.
        return;
    }

    NUM_NOTIFICATIONS.fetch_add(
        1,
        // use release ordering to synchronize with the waiter, such that if the waiter sees our increment, it guarantees a happens before
        // relationship in relation to all of our previous memory writes.
        //
        // not relevant to this primitive itself, but relevant to users of this primitive. atomic::Ordering::Release,
        atomic::Ordering::Release,
    );

    // start with a read lock so that if we end up not needing to do anything, we don't slow other threads down.
    // only if we know for sure that we need to write, we will lock for writing.
    let subscriber_info_opt = SUBSCRIBER_INFO.read();
    let Some(subscriber_info) = subscriber_info_opt.as_ref() else {
        // no actual subscriber at this point in time, so we're done
        return;
    };

    let cur_tid = thread::current().id();
    if skip_waiter_if_current_thread && subscriber_info.thread_id == cur_tid {
        // the waiter is the current thread, skip it.
        return;
    }

    // at this point, we know we want to wake the current subscriber, so we now need to lock the subscriber info for writing.
    let _ = subscriber_info;
    let mut subscriber_info_opt = SUBSCRIBER_INFO.write();

    // we need to re-check the subscriber info, since between the time we dropped the read lock and the time we acquired the write lock, it may
    // have been modified.
    let Some(subscriber_info) = subscriber_info_opt.as_ref() else {
        // no actual subscriber at this point in time, so we're done
        return;
    };

    let cur_tid = thread::current().id();
    if skip_waiter_if_current_thread && subscriber_info.thread_id == cur_tid {
        // the waiter is the current thread, skip it.
        return;
    }

    // consume the current subscriber info value
    // SAFETY: we just checked that this is not `None`, and we're holding the write lock, so it couldn't have changed.
    let subscriber_info_owned = unsafe { subscriber_info_opt.take().unwrap_unchecked() };
    subscriber_info_owned.waker.wake();

    // the single subscriber has been woken up by us, so there is no longer any subscriber.
    HAS_SUBSCRIBER.store(
        false,
        // no ordering is needed here. see the load of `HAS_SUBSCRIBER` for more info.
        atomic::Ordering::Relaxed,
    );
}
