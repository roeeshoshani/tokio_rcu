use std::{
    marker::PhantomPinned,
    panic::UnwindSafe,
    pin::Pin,
    ptr::NonNull,
    task::{Poll, Waker},
};

use crate::loom::{
    CellDataNonNullPtr, UnsafeCell, fn_const_if_not_loom,
    std::{
        sync::atomic::{self, AtomicUsize},
        thread::{self, ThreadId},
    },
};

/// a synchronization data structure used to pass notifications between different tasks.
/// similar in functionality to [`tokio::sync::Notify`], but a simplified version of it more tailored to the specific use in this crate.
pub struct Notify {
    /// the total number of wakeups ever performed.
    /// incremented by 1 each time someone notifies this notify object.
    /// this value safely wraps around on overflow, while still maintaining the correctness of the algorithm, except for
    /// very extreme cases.
    num_wakeups: AtomicUsize,

    /// a lock protecting the waiters list and all slots within it, including the data of all those slots.
    ///
    /// if a slot may or may not be in the waiters list, you must lock this lock to access it, and only if you know for sure
    /// that it is not in the waiters list, you can safely access it without this lock.
    lock: crate::loom::parking_lot::Mutex<()>,

    /// the head of the list of all current waiters that registered for wake up once the notify object is notified.
    /// the list, including all data contained in all slots contained in it, is protected by the lock.
    waiters_list_head: UnsafeCell<Next>,

    /// the number of currently active waiters.
    /// note that this is greater than or equal to the number of waiters in the list, since some waiters may not yet be
    /// in the list since haven't been polled yet.
    num_active_waiters: AtomicUsize,
}
impl Notify {
    fn_const_if_not_loom! {
        /// creates a new notify object.
        pub const fn new() -> Self {
            Self {
                num_wakeups: AtomicUsize::new(0),
                lock: crate::loom::parking_lot::Mutex::new(()),
                waiters_list_head: UnsafeCell::new(None),
                num_active_waiters: AtomicUsize::new(0),
            }
        }
    }

    /// returns a future which when awaited will wait for a notification.
    ///
    /// when this function returns, the returned future has already properly registered itself and is listening to notifications.
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
    /// this is a known limitation of the current implementation, and when using this type, you should be aware of it, and make sure
    /// that your code works properly even in such extreme edge cases.
    pub fn notified(&self) -> Notified<'_> {
        Notified::new(self)
    }

    /// notifies all currently registered waiters.
    ///
    /// provides release memory ordering when a waiter finishes awaiting and was woken up by you or any notifier after you.
    pub fn notify(&self) {
        self.notify_impl(false);
    }

    /// notifies all currently registered waiters, other than the waiters which were registered by the current thread.
    ///
    /// note that this may still cause a notifier on the current thread to wake up, but it will not explicitly wake its task's
    /// waker. if that waiter will later be polled for some other reason, it will finish its wait due to this notify calls, but
    /// this notify call will not explicitly cause it to wake up if it is currently blocking.
    ///
    /// this is used when the state change being notified about can't possibly be of any interest to the current thread itself.
    /// see [`on_thread_park`](crate::on_thread_park) for the specific case where this is needed, and `Slot::thread_id` for more info.
    ///
    /// provides release memory ordering when a waiter finishes awaiting and was woken up by you or any notifier after you.
    pub fn notify_except_current_thread(&self) {
        self.notify_impl(true);
    }

    /// increments the number of active waiters using the given ordering.
    fn inc_num_active_waiters(&self, ordering: atomic::Ordering) {
        let prev_value = self.num_active_waiters.fetch_add(1, ordering);
        debug_assert_ne!(prev_value, usize::MAX);
    }

    /// decrements the number of active waiters using the given ordering.
    fn dec_num_active_waiters(&self, ordering: atomic::Ordering) {
        let prev_value = self.num_active_waiters.fetch_sub(1, ordering);
        debug_assert_ne!(prev_value, 0);
    }

    fn notify_impl(&self, skip_waiters_of_current_thread: bool) {
        if self.num_active_waiters.load(
            // we don't need any ordering here, since we don't need any synchronization with writers of this variable.
            //
            // there are 2 types of writers to this variable: waiters that register and unregister themselves, and other
            // notifiers, that write to it when notifying waiters to unregister them.
            //
            // for the waiters part, we don't need acquire ordering here, since waiters dont do any special preparation before
            // writing to this field.
            //
            // for the notifiers part, we also don't need acquire, since notifiers only subtract, and the only interesting case
            // is when this reaches 0, in which case we do nothing.
            atomic::Ordering::Relaxed,
        ) == 0
        {
            // fast path: no active waiters, no need to do anything.
            // note that this fast path is especially fast since it is read only - no write operations are performed.
            // so, in the fast path, all notifier threads don't fight over exclusivity over cache-lines, they can all
            // share the cache line.
            return;
        }

        self.num_wakeups.fetch_add(
            1,
            // need release ordering for the memory ordering guarantees chosen for this data structure.
            // note that due to this operation being a RMW operation, it also preserves the existing release-sequence, without having to
            // use an acquire ordering here (for more info on release-sequences, see c++ memory model).
            atomic::Ordering::Release,
        );

        // only fetch the current thread id if we actually need it, since this is not free under loom.
        let cur_thread_id = skip_waiters_of_current_thread.then(|| thread::current().id());

        let _guard = self.lock.lock();

        // SAFETY: in the following code, we assume exclusivity over all data in the list due to the lock.
        //
        // furthermore, note that we are creating referencing to `Slot`s inside the list which may alias concurrently existing `&mut Slot`
        // references that exist for those slots due to them being contained in `Notified`, and when `Notified` is polled, a `&mut Notified`
        // is created.
        //
        // this may seem like a violation of rust's aliasing rules, but since `Slot` is `!Unpin`, we are allowed to create aliasing references
        // to it in this manner.
        unsafe {
            // the link (the `next` pointer of a slot, or the head pointer of the list) which currently points at the slot we are looking at.
            // for the first slot in the list this is the head pointer of the list, and for any other slot this is the `next` pointer of the slot
            // preceding it.
            let mut link_to_cur_slot = self.waiters_list_head.get_mut_ptr();

            while let Some(cur_slot_ptr) = link_to_cur_slot.read() {
                let cur_slot = cur_slot_ptr.as_ref();

                // the link of the slot we are looking at, and the slot it currently points at (if any).
                let cur_link = cur_slot.next.get_mut_ptr();
                let next_slot_ptr = cur_link.read();

                // avoid having the current thread wake itself up.
                // this protection helps deal with a quirk in the rcu implementation, where we notify some notification object whenever a thread
                // parks itself, but without this protection, as soon as a thread would start waiting for a notification and park itself, the park
                // operation would wake himself up due to logic in our on park hook, making the thread unable to actually wait for a notification,
                // instead being stuck in a constant loop of trying to park and then immediately waking up.
                let skip =
                    cur_thread_id.is_some_and(|id| cur_slot.thread_id.get_const_ptr().read() == id);
                if !skip {
                    // in this case, we want to remove this slot and wake its waker.

                    // first remove the current slot from the list, so that the link which used to point at it now points at the next slot.
                    // we do this before waking it up so that if its wake callback panics, we leave the list in a reasonable state.

                    link_to_cur_slot.write(next_slot_ptr);
                    if let Some(next_slot_ptr) = next_slot_ptr {
                        let next_slot = next_slot_ptr.as_ref();

                        // the next slot takes the place of the slot we just removed, so it must also take its pprev.
                        // this is either `None` if the removed slot was the head of the list (in which case the next slot is now the head), or a
                        // pointer to the `next` pointer of the slot preceding the removed slot (which may be a slot we decided not to remove).
                        next_slot
                            .pprev
                            .get_mut_ptr()
                            .write(cur_slot.pprev.get_mut_ptr().replace(None));
                    }

                    // tell the node that he is no longer in the list.
                    // this is important for when the future containing the slot is dropped, so that it knows whether to try to remove
                    // itself from the list or not.
                    cur_slot.is_in_list.get_mut_ptr().write(false);

                    // we removed this waiter from the list, so it is no longer an active waiter. subtract it from the num
                    // active waiters counter.
                    self.dec_num_active_waiters(
                        // no special ordering is needed here. see the load of num active waiters for more info.
                        atomic::Ordering::Relaxed,
                    );

                    let waker_opt = cur_slot.waker.get_mut_ptr().replace(None);
                    if let Some(waker) = waker_opt {
                        // if this panics, nothing bad happens.
                        // the list is currently in a valid state, and this node is no longer part of it.
                        waker.wake();
                    }
                } else {
                    // the slot belongs to the current thread, so we leave it in the list and don't wake it up.
                    // this means the slot after it is now pointed at by the current slot's link, so we continue from there.
                    link_to_cur_slot = cur_link;
                }
            }
        }
    }
}
unsafe impl Send for Notify {}
unsafe impl Sync for Notify {}

/// notify maintains a valid state even if panics occur while using it.
impl UnwindSafe for Notify {}

type Next = Option<NonNull<Slot>>;

struct Slot {
    /// a pointer to the "next" field of the previous slot, or `None` if this slot is the head of the list.
    pprev: UnsafeCell<Option<CellDataNonNullPtr<Next>>>,

    /// a pointer to the next slot, or `None` if this is the last slot in the list.
    next: UnsafeCell<Next>,

    waker: UnsafeCell<Option<Waker>>,

    is_in_list: UnsafeCell<bool>,

    /// the thread id of this waiter. updated whenever the [`Notified`] instance is polled.
    /// this is used to prevent a thread from waking himself up.
    ///
    /// this is an implementation quirk, but it is quite important. the rcu book-keeping logic notifies the global notification object whenever
    /// a thread parks. but, when a thread waits for a notification, he immediately parks, which then immediately notifies the exact notification
    /// that he just started waiting on. to prevent this from immediately waking that thread up, we make sure that a thread can't wake itself up.
    thread_id: UnsafeCell<ThreadId>,

    // this makes sure that the compiler doesn't emit the llvm `noalias` attribute for `&mut Self` values.
    // without this, putting the future into the intrusive linked list is inherently UB, since calling poll on `Notified` requires
    // constructing a `&mut Notified`, and while that `&mut Notified` exists, someone may be iterating over the list and modifying
    // some fields. furthermore, since `Slot` is a field inside `Notified`, the `&mut Notified` basically implies `&mut Slot`.
    // so, in that case, we are reading/writing a pointer which points to data which is currently used as part of a mutable reference.
    // this is normally UB, but `PhantomPinned` currently provides an escape hatch.
    _phantom: PhantomPinned,
}
impl Slot {
    fn new() -> Self {
        Self {
            pprev: UnsafeCell::new(None),
            waker: UnsafeCell::new(None),
            next: UnsafeCell::new(None),
            is_in_list: UnsafeCell::new(false),
            thread_id: UnsafeCell::new(thread::current().id()),
            _phantom: PhantomPinned,
        }
    }
}

/// a future which will complete once a notification is received.
/// the future is registered as soon as it is created, and while registered it is listening to any received notifications.
pub struct Notified<'a> {
    /// the notify object from which this notified object was created.
    notify: &'a Notify,

    /// an embedded intrusive linked list slot. used to register ourselves into the waiters list.
    slot: Slot,

    /// a snapshot of the notify object's num wakeups counter, taken at the point in time when this notified object was created.
    num_wakeups_snapshot: usize,

    /// have we ever registered ourslves into the waiters list throughout the lifetime of this notified object?
    was_registered_into_list: bool,
}
impl<'a> Notified<'a> {
    fn new(notify: &'a Notify) -> Self {
        notify.inc_num_active_waiters(
            // no special ordering is needed here. see the load of num active waiters for more info.
            atomic::Ordering::Relaxed,
        );

        Self {
            slot: Slot::new(),
            num_wakeups_snapshot: notify.num_wakeups.load(
                // the value loaded here does not need to be synchronized with, so we don't need any ordering in that sense, but we need
                // acquire ordering so that the notified registration operation has acquire semantics, which is relevant for the
                // users of this primitive.
                atomic::Ordering::Acquire,
            ),
            notify,
            was_registered_into_list: false,
        }
    }
}
impl<'a> Future for Notified<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let new_num_wakeups = self.notify.num_wakeups.load(
            // no ordering here, we instead use a fence only when an ordering is really needed
            atomic::Ordering::Relaxed,
        );
        if new_num_wakeups != self.num_wakeups_snapshot {
            // wake up was called since we started listening

            // need acquire ordering for the memory ordering guarantees chosen for this data structure.
            atomic::fence(atomic::Ordering::Acquire);

            return Poll::Ready(());
        }

        // extra scope for scoping the lock guard
        {
            let _guard = self.notify.lock.lock();

            // SAFETY: all unsafe actions below assume exclusive access due to holding the lock.
            unsafe {
                // update the current thread id of this waiter
                self.slot
                    .thread_id
                    .get_mut_ptr()
                    .write(thread::current().id());

                let is_in_list = self.slot.is_in_list.get_const_ptr().read();

                // insert us into the waker list, or update our waker if we're already in the list
                match is_in_list {
                    true => {
                        // already in the list, update our waker
                        let mut waker_ptr = self.slot.waker.get_mut_ptr();
                        let waker = waker_ptr.as_mut_ref();
                        match &*waker {
                            // note that even if `will_wake` panics we leave everything in a clean state.
                            Some(existing_waker) if existing_waker.will_wake(cx.waker()) => {
                                // keep the existing waker
                            }
                            _ => {
                                // need to use a new waker.
                                //
                                // note that even if the the waker's `clone` impl panics we leave everything in a clean state.
                                *waker = Some(cx.waker().clone());
                            }
                        }
                    }
                    false => {
                        // we are currently not in the list

                        if self.was_registered_into_list {
                            // if we had registered ourselves into the list in a previous call to `poll`, and we are now no longer
                            // in the list, it means that someone woke us up. so, we're done.
                            return Poll::Ready(());
                        } else {
                            // first time being polled, register ourselves into the list
                            self.slot
                                .waker
                                .get_mut_ptr()
                                .write(Some(cx.waker().clone()));
                            self.slot.is_in_list.get_mut_ptr().write(true);

                            let head_opt = self.notify.waiters_list_head.get_const_ptr().read();
                            self.slot.next.get_mut_ptr().write(head_opt);
                            self.slot.pprev.get_mut_ptr().write(None);

                            if let Some(head_nonnull) = head_opt {
                                let head = head_nonnull.as_ref();
                                head.pprev.get_mut_ptr().write(Some(
                                    self.slot.next.get_mut_ptr().into_non_null_unchecked(),
                                ));
                            }

                            self.notify
                                .waiters_list_head
                                .get_mut_ptr()
                                .write(Some(NonNull::from_ref(&self.slot)));

                            // mark that we have registered ourselves into the list.
                            // this is later used to detect if we got removed from the list after registration, in which case someone
                            // woke us up.
                            self.as_mut().get_unchecked_mut().was_registered_into_list = true;
                        }
                    }
                }
            }
        }

        // before actually going to sleep, check since we last checked, during the time we inserted ourselves into the list,
        // someone had woke us up.
        // if we don't check this, we may miss a waker who woke us up before we were inside the list, but after we initially checked
        // the number of wakeups. missing this would cause us to incorrectly yield, even though we should wake up.
        let new_num_wakeups = self.notify.num_wakeups.load(
            // no ordering here, we instead use a fence only when an ordering is really needed
            atomic::Ordering::Relaxed,
        );
        if new_num_wakeups != self.num_wakeups_snapshot {
            // wake up was called since we started listening

            // need acquire ordering for the memory ordering guarantees chosen for this data structure.
            atomic::fence(atomic::Ordering::Acquire);

            return Poll::Ready(());
        }

        Poll::Pending
    }
}

unsafe impl<'a> Send for Notified<'a> {}
unsafe impl<'a> Sync for Notified<'a> {}

impl<'a> Notified<'a> {
    // drop logic for this notified object in the case where it was never registered into the list.
    fn drop_never_registered_into_list(&mut self) {
        // subtract ourselves from the num active waiters counter.
        // note that unlike the case where we were registered into the list, there's no chance that someone had already
        // subtracted us from the list.
        self.notify.dec_num_active_waiters(
            // no special ordering is needed here. see the load of num active waiters for more info.
            atomic::Ordering::Relaxed,
        );
    }

    // drop logic for this notified object in the case where it was registered into the list at some point.
    fn drop_was_registered_into_list(&mut self) {
        let _guard = self.notify.lock.lock();

        // SAFETY: all unsafe actions below assume exclusive access due to holding the lock.
        unsafe {
            let is_in_list = self.slot.is_in_list.get_const_ptr().read();
            if is_in_list {
                // remove ourselves from the list

                let pprev_opt = self.slot.pprev.get_mut_ptr().replace(None);
                let next_opt = self.slot.next.get_const_ptr().read();

                // set prev's next to our next
                match &pprev_opt {
                    Some(pprev_nonnull) => {
                        pprev_nonnull.write(next_opt);
                    }
                    None => {
                        // when we are in the list but pprev is `None`, it means that we are the head of the list
                        debug_assert_eq!(
                            self.notify.waiters_list_head.get_const_ptr().read(),
                            Some(NonNull::from_ref(&self.slot))
                        );

                        self.notify.waiters_list_head.get_mut_ptr().write(next_opt);
                    }
                }

                // set next's pprev to our pprev
                if let Some(next_nonnull) = next_opt {
                    let next = next_nonnull.as_ref();
                    next.pprev.get_mut_ptr().write(pprev_opt);
                }

                // subtract ourselves from the num active waiters counter
                self.notify.dec_num_active_waiters(
                    // no special ordering is needed here. see the load of num active waiters for more info.
                    atomic::Ordering::Relaxed,
                );
            } else {
                // in this case, we were once in the list, but were removed from the list.
                // in this case, someone had already notified us, and also already subtracted us from the num active
                // waiters counter, so we don't need to do anything.
            }
        }
    }
}
impl<'a> Drop for Notified<'a> {
    fn drop(&mut self) {
        if self.was_registered_into_list {
            self.drop_was_registered_into_list();
        } else {
            self.drop_never_registered_into_list();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::extract_string_panic_message;

    use std::{
        panic::AssertUnwindSafe,
        pin::pin,
        sync::Arc,
        task::{RawWaker, RawWakerVTable},
    };

    #[tokio::test]
    async fn basic() {
        struct State {
            notify: Notify,
            value: AtomicUsize,
        }
        let state = Arc::new(State {
            notify: Notify::new(),
            value: AtomicUsize::new(5),
        });

        // start listening to notifications before spawning the writer task to make sure we see his notification.
        let notified = state.notify.notified();

        let task = tokio::task::spawn({
            let state = state.clone();
            async move {
                state.value.store(12, atomic::Ordering::Relaxed);
                state.notify.notify();
            }
        });
        notified.await;
        assert_eq!(state.value.load(atomic::Ordering::Relaxed), 12);

        task.await.unwrap();
    }

    #[tokio::test]
    async fn multiple_wakers() {
        const NUM_WAKERS: usize = 32;

        struct State {
            notify: Notify,
            value: AtomicUsize,
        }
        let state = Arc::new(State {
            notify: Notify::new(),
            value: AtomicUsize::new(5),
        });

        // start listening to notifications before spawning the writer task to make sure we see his notification.
        let notified = state.notify.notified();

        let tasks: Vec<_> = (0..NUM_WAKERS)
            .map(|i| {
                tokio::task::spawn({
                    let state = state.clone();
                    async move {
                        state.value.store(1234 + i, atomic::Ordering::Relaxed);
                        state.notify.notify();
                    }
                })
            })
            .collect();

        notified.await;
        assert!((1234..1234 + NUM_WAKERS).contains(&state.value.load(atomic::Ordering::Relaxed)));

        for task in tasks {
            task.await.unwrap()
        }
    }

    #[tokio::test]
    async fn multiple_waiters_and_wakers() {
        const NUM_WAITERS: usize = 32;
        const NUM_WAKERS: usize = 32;

        struct State {
            num_done_setup: AtomicUsize,
            done_setup_notify: Notify,
            notify: Notify,
            value: AtomicUsize,
        }
        let state = Arc::new(State {
            num_done_setup: AtomicUsize::new(0),
            done_setup_notify: Notify::new(),
            notify: Notify::new(),
            value: AtomicUsize::new(5),
        });

        let done_setup = state.done_setup_notify.notified();

        let waiter_tasks: Vec<_> = (0..NUM_WAITERS)
            .map(|_| {
                tokio::task::spawn({
                    let state = state.clone();
                    async move {
                        let notified = state.notify.notified();
                        // release ordering paired with acquire for the leader thread is needed to make sure that before the
                        // leader thread calls notify, he sees all writes previously performed by any threads, thus guaranteeing that
                        // the setup is actually done for all threads once the done setup notify is notified.
                        if state.num_done_setup.fetch_add(1, atomic::Ordering::Release) + 1
                            == NUM_WAITERS
                        {
                            atomic::fence(atomic::Ordering::Acquire);
                            state.done_setup_notify.notify();
                        }
                        notified.await;
                        assert!(
                            (1234..1234 + NUM_WAKERS)
                                .contains(&state.value.load(atomic::Ordering::Relaxed))
                        );
                    }
                })
            })
            .collect();

        done_setup.await;

        let waker_tasks: Vec<_> = (0..NUM_WAKERS)
            .map(|i| {
                tokio::task::spawn({
                    let state = state.clone();
                    async move {
                        state.value.store(1234 + i, atomic::Ordering::Relaxed);
                        state.notify.notify();
                    }
                })
            })
            .collect();

        for task in waker_tasks.into_iter().chain(waiter_tasks.into_iter()) {
            task.await.unwrap()
        }
    }

    #[test]
    fn waker_wake_panic() {
        unsafe fn waker_clone(x: *const ()) -> RawWaker {
            RawWaker::new(x, &WAKER_VTABLE)
        }
        unsafe fn waker_wake(_x: *const ()) {
            panic!("waker wake called");
        }
        unsafe fn waker_wake_by_ref(_x: *const ()) {
            panic!("waker wake called");
        }
        unsafe fn waker_drop(_x: *const ()) {}

        const WAKER_VTABLE: RawWakerVTable =
            RawWakerVTable::new(waker_clone, waker_wake, waker_wake_by_ref, waker_drop);

        fn poll_notified(notified: Pin<&mut Notified>) -> Poll<()> {
            let waker = unsafe {
                Waker::new(
                    // this value doesn't matter
                    0x10 as *const (),
                    &WAKER_VTABLE,
                )
            };
            let mut ctx = std::task::Context::from_waker(&waker);
            notified.poll(&mut ctx)
        }

        let notify = Notify::new();

        // create one notified object which will remain throughout the panic.
        let notified_remaining = pin!(notify.notified());
        assert_eq!(poll_notified(notified_remaining), Poll::Pending);

        // extra scope to scope the lifetime of the pinned notified values.
        {
            // create 2 more notified objects, one will be removed by the failing notify call, and one will be dropped at
            // the end of the scope, to check both cases.
            let notified1 = pin!(notify.notified());
            let notified2 = pin!(notify.notified());

            assert_eq!(poll_notified(notified1), Poll::Pending);

            assert_eq!(poll_notified(notified2), Poll::Pending);

            // call `notify`. it should panic due to calling wake.
            // also, it should remove one of the notified objects from the list.
            let err = std::panic::catch_unwind(AssertUnwindSafe(|| {
                notify.notify();
            }))
            .unwrap_err();
            assert!(extract_string_panic_message(err).contains("waker wake called"));

            // at the end of this scope, both notified objects will be dropped.
            // make sure that dropping both of them, in both of their states, should work fine, even after the panic.
        }

        // notifying once more should once again panic, and should also remove the remaining notified object from the list,
        // making the list empty.
        let err = std::panic::catch_unwind(AssertUnwindSafe(|| {
            notify.notify();
        }))
        .unwrap_err();
        assert!(extract_string_panic_message(err).contains("waker wake called"));

        // the waiters list is now empty, so notifying should now work.
        notify.notify();

        // re-registering into the list should work fine.
        let notified_final = pin!(notify.notified());
        assert_eq!(poll_notified(notified_final), Poll::Pending);

        // after re-registering, notifying should once again fail due to the wake panic.
        let err = std::panic::catch_unwind(AssertUnwindSafe(|| {
            notify.notify();
        }))
        .unwrap_err();
        assert!(extract_string_panic_message(err).contains("waker wake called"));
    }

    #[test]
    fn waker_clone_panic() {
        unsafe fn waker_clone(x: *const ()) -> RawWaker {
            if x == (0xbad as *const ()) {
                panic!("waker clone called");
            }
            RawWaker::new(x, &WAKER_VTABLE)
        }
        unsafe fn waker_wake(_x: *const ()) {}
        unsafe fn waker_wake_by_ref(_x: *const ()) {}
        unsafe fn waker_drop(_x: *const ()) {}

        const WAKER_VTABLE: RawWakerVTable =
            RawWakerVTable::new(waker_clone, waker_wake, waker_wake_by_ref, waker_drop);

        fn poll_notified(notified: Pin<&mut Notified>, waker_ptr_value: usize) -> Poll<()> {
            let waker = unsafe { Waker::new(waker_ptr_value as *const (), &WAKER_VTABLE) };
            let mut ctx = std::task::Context::from_waker(&waker);
            notified.poll(&mut ctx)
        }

        let notify = Notify::new();

        // we use 2 "good" notified objects - one which remains across the panicking operation, and one that is discarded
        // immediately after it, so see that both the dropping logic works, and that waking it afterwards works.
        let mut good_notified_remaining = pin!(notify.notified());

        assert_eq!(
            poll_notified(good_notified_remaining.as_mut(), 0x10),
            Poll::Pending
        );

        // extra scope to scope the lifetime of the pinned notified values.
        {
            let good_notified_discarded = pin!(notify.notified());
            let bad_notified = pin!(notify.notified());

            assert_eq!(poll_notified(good_notified_discarded, 0x10), Poll::Pending);

            let err =
                std::panic::catch_unwind(AssertUnwindSafe(|| poll_notified(bad_notified, 0xbad)))
                    .unwrap_err();
            assert!(extract_string_panic_message(err).contains("waker clone called"));

            // when this scope ends, both notified values will be dropped.
            // the drop of the good notified should work just fine even though the previous operation just panicked.
        }

        // waking the remaining notify should work just fine, even after the panic.
        notify.notify();

        assert_eq!(
            poll_notified(good_notified_remaining.as_mut(), 0x10),
            Poll::Ready(())
        );

        // we should be able to insert new notified objects into the list, even after the panic.
        let mut notified_final = pin!(notify.notified());
        assert_eq!(poll_notified(notified_final.as_mut(), 0x10), Poll::Pending);
        notify.notify();
        assert_eq!(
            poll_notified(notified_final.as_mut(), 0x10),
            Poll::Ready(())
        );
    }
}
