use std::{
    cell::UnsafeCell,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    sync::atomic::{self, AtomicUsize},
    task::{Poll, Waker},
};

pub struct Notify {
    num_wakeups: AtomicUsize,
    lock: std::sync::Mutex<()>,
    waiters_list_head: UnsafeCell<Next>,
}
impl Notify {
    pub const fn new() -> Self {
        Self {
            num_wakeups: AtomicUsize::new(0),
            lock: std::sync::Mutex::new(()),
            waiters_list_head: UnsafeCell::new(None),
        }
    }

    /// provides acquire memory ordering against the waker when you are finished awaiting it.
    pub fn notified(&self) -> Notified<'_> {
        Notified::new(self)
    }

    /// provides release memory ordering when a waiter finishes awaiting and was woken up by you or any waker after you.
    pub fn notify(&self) {
        self.num_wakeups.fetch_add(
            1,
            // need release ordering for the memory ordering guarantees chosen for this data structure.
            // note that due to this operation being a RMW operation, it also preserves the existing release-sequence, without having to
            // use an acquire ordering here (for more info on release-sequences, see c++ memory model).
            atomic::Ordering::Release,
        );

        let _guard = self.lock.lock().unwrap();

        // SAFETY: the lock guarantees exclusivity
        let waiters_list_head = unsafe { &mut *self.waiters_list_head.get() };

        // "take" the list and leave an empty list behind.
        let mut cur_ptr_opt = std::mem::replace(waiters_list_head, None);

        while let Some(cur_ptr) = cur_ptr_opt {
            let slot = cur_ptr.as_ptr();

            // grab the next pointer in the list.
            // SAFETY: we have the lock, so we are the only one accessing this.
            // also, we don't create any references, so this is sound even if a mutable reference exists to the slot while we use it.
            let next_ptr_opt = unsafe { *UnsafeCell::raw_get(&raw mut (*slot).next) };

            // tell the node that he is no longer in the list, sinc we cleared the list.
            // SAFETY: we have the lock, so we are the only one accessing this.
            // also, we don't create any references, so this is sound even if a mutable reference exists to the slot while we use it.
            unsafe { *UnsafeCell::raw_get(&raw mut (*slot).is_in_list) = false };

            // SAFETY: we have the lock, so we are the only one accessing this.
            // also, we don't create any references, so this is sound even if a mutable reference exists to the slot while we use it.
            let waker_storage_ptr = unsafe { UnsafeCell::raw_get(&raw mut (*slot).waker) };
            let waker_opt = unsafe { std::ptr::replace(waker_storage_ptr, None) };
            if let Some(waker) = waker_opt {
                // TODO: what if this panics? what will happen to the state of the list?
                waker.wake();
            }

            cur_ptr_opt = next_ptr_opt;
        }
    }
}
unsafe impl Send for Notify {}
unsafe impl Sync for Notify {}

type Next = Option<NonNull<Slot>>;
struct Slot {
    pprev: UnsafeCell<Option<NonNull<Next>>>,
    next: UnsafeCell<Next>,
    waker: UnsafeCell<Option<Waker>>,
    is_in_list: UnsafeCell<bool>,

    // this makes sure that the compiler doesn't emit the llvm `noalias` attribute for `&mut Self` values.
    // without this, putting the future into the intrusive linked list is inherently UB, since calling poll on `Notified` requires
    // constructing a `&mut Notified`, and while that `&mut Notified` exists, someone may be iterating over the list and modifying
    // some fields. furthermore, since `Slot` is a field inside `Notified`, the `&mut Notified` basically implies `&mut Slot`.
    // so, in that case, we are reading/writing a pointer which points to data which is currently used as part of a mutable reference.
    // this is normally UB, but `PhantomPinned` currently provides an escape hatch.
    phantom: PhantomPinned,
}
impl Slot {
    fn new() -> Self {
        Self {
            pprev: UnsafeCell::new(None),
            waker: UnsafeCell::new(None),
            next: UnsafeCell::new(None),
            is_in_list: UnsafeCell::new(false),
            phantom: PhantomPinned,
        }
    }
}

pub struct Notified<'a> {
    slot: Slot,
    num_wakeups_snapshot: usize,
    notify: &'a Notify,
}
impl<'a> Notified<'a> {
    pub fn new(notify: &'a Notify) -> Self {
        Self {
            slot: Slot::new(),
            num_wakeups_snapshot: notify.num_wakeups.load(
                // the value loaded here does not need to be syncrhonized with.
                // it will not be directly used to decide whether we wake up or not.
                // only when comparing it with a value that is loaded with acquire ordering, with which we are actually ordering,
                // we will decide whether to wake up.
                atomic::Ordering::Relaxed,
            ),
            notify,
        }
    }
}
impl<'a> Future for Notified<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
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
            let _guard = self.notify.lock.lock().unwrap();

            // SAFETY: all unsafe actions below assume exclusive access due to holding the lock.
            unsafe {
                let is_in_list = *self.slot.is_in_list.get();

                // insert us into the waker list, or update our waker if we're already in the list
                match is_in_list {
                    true => {
                        // already in the list, update our waker
                        let waker = &mut *self.slot.waker.get();
                        match &*waker {
                            Some(existing_waker) if existing_waker.will_wake(cx.waker()) => {
                                // keep the existing waker
                            }
                            _ => {
                                // need to use a new waker
                                *waker = Some(cx.waker().clone());
                            }
                        }
                    }
                    false => {
                        // put ourselves inside the list

                        *self.slot.waker.get() = Some(cx.waker().clone());
                        *self.slot.is_in_list.get() = true;

                        let head_opt = *self.notify.waiters_list_head.get();
                        *self.slot.next.get() = head_opt;
                        *self.slot.pprev.get() = None;

                        if let Some(head_nonnull) = head_opt {
                            let head = head_nonnull.as_ptr();
                            let head_pprev = UnsafeCell::raw_get(&raw mut (*head).pprev);
                            *head_pprev = Some(NonNull::new_unchecked(self.slot.next.get()))
                        }

                        *self.notify.waiters_list_head.get() = Some(NonNull::from_ref(&self.slot))
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

impl<'a> Drop for Notified<'a> {
    fn drop(&mut self) {
        let _guard = self.notify.lock.lock().unwrap();

        // SAFETY: all unsafe actions below assume exclusive access due to holding the lock.
        unsafe {
            let is_in_list = *self.slot.is_in_list.get();
            if is_in_list {
                // remove ourselves from the list

                // set next's pprev to our pprev
                let next_opt = *self.slot.next.get();
                if let Some(next_nonnull) = next_opt {
                    let next = next_nonnull.as_ptr();
                    let next_pprev = UnsafeCell::raw_get(&raw mut (*next).pprev);
                    *next_pprev = *self.slot.pprev.get();
                }

                // set prev's next to our next
                let pprev_opt = *self.slot.pprev.get();
                match pprev_opt {
                    Some(pprev_nonnull) => {
                        let pprev = pprev_nonnull.as_ptr();
                        *pprev = next_opt;
                    }
                    None => {
                        // when we are in the list but pprev is `None`, it means that we are the head of the list
                        debug_assert_eq!(
                            *self.notify.waiters_list_head.get(),
                            Some(NonNull::from_ref(&self.slot))
                        );

                        *self.notify.waiters_list_head.get() = next_opt;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

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

        let waker_tasks: Vec<_> = (0..NUM_WAKERS)
            .map(|i| {
                tokio::task::spawn({
                    let state = state.clone();
                    async move {
                        state.done_setup_notify.notified().await;
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
}
