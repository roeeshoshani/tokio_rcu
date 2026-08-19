use std::{
    pin::Pin,
    ptr::null_mut,
    sync::atomic::{self, AtomicPtr, AtomicU8},
    task::{Poll, Waker},
};

const SLOT_STATE_IDLE: u8 = 0;
const SLOT_STATE_ABANDONED: u8 = 1;
const SLOT_STATE_NOTIFIED: u8 = 2;
const SLOT_STATE_DONE: u8 = 3;

struct Slot {
    // TODO: we often call some waker related function pointers while holding the spinlock, which is unwise and should be fixed.
    waker: spin::mutex::Mutex<Option<Waker>>,
    state: AtomicU8,
    next: AtomicPtr<Slot>,
}

pub struct Notify {
    slots_list_head: AtomicPtr<Slot>,
    // TODO: maybe save a freelist of old slot allocations for re-use? to avoid allocating on every call. can improve performance.
    // but, need to cap its size.
}
impl Notify {
    pub const fn new() -> Self {
        Self {
            slots_list_head: AtomicPtr::new(null_mut()),
        }
    }

    pub fn notify(&self) {
        let list_head = self.slots_list_head.swap(
            null_mut(),
            // for the load part, use acquire ordering paired with the release ordering when inserting a new entry into the list.
            // this makes sure that we see the slot's initialization before we see its pointer published, as well the initialization of
            // all slots following it.
            //
            // for the store part, we don't need any special ordering.
            atomic::Ordering::Acquire,
        );

        let mut cur_slot_ptr = list_head;
        while !cur_slot_ptr.is_null() {
            let cur_slot = unsafe { &*cur_slot_ptr };

            let next_slot_ptr = cur_slot.next.load(
                // we don't need any ordering.
                //
                // the next pointer never changes, and this slot is guaranteed to already be initialized due to the acquire load of the
                // list head pointer.
                atomic::Ordering::Relaxed,
            );

            // notify the slot
            let prev_state = cur_slot.state.swap(
                SLOT_STATE_NOTIFIED,
                // no ordering used directly here, we instead use fences as needed depending on the prev state
                atomic::Ordering::Relaxed,
            );
            if prev_state == SLOT_STATE_ABANDONED {
                // free this slot up

                // make the load part of the just performed swap operation have acquire ordering so that all uses of this slot by the
                // waiter happen before his final store to the slot's state, so that the slot is no longer used when we free it here.
                atomic::fence(atomic::Ordering::Acquire);

                let _ = unsafe { Box::from_raw(cur_slot_ptr) };
            } else {
                // slot hasn't been abandoned, continue the process of waking it.

                // in this case, we are notifying a live slot, and the notify operation of this data structure should have release
                // ordering. so, perform this store with release, coupled with the waiters acquire load, so that the waiting logic
                // provides proper memory ordering guarantees in relation to other data accessed by the threads.
                atomic::fence(atomic::Ordering::Release);

                // wake him up.
                // while doing so, we must make sure that we don't call the wake method while holding the lock.
                let maybe_waker = {
                    let mut waker_storage = cur_slot.waker.lock();
                    waker_storage.take()
                };
                if let Some(waker) = maybe_waker {
                    waker.wake();
                }

                // let him know that we're done using the slot and it can be freed.
                // the slot is no longer accessible from the `Notify` struct itself, it is only accessible to us, and we will no longer use
                // it from this point on.
                let prev_state = cur_slot.state.swap(
                    SLOT_STATE_DONE,
                    // no ordering used directly here, we instead use fences as needed depending on the prev state
                    atomic::Ordering::Relaxed,
                );

                if prev_state == SLOT_STATE_ABANDONED {
                    // if one again at this point the slot has already been abandoned, free it

                    // make the load part of the just performed swap operation have acquire ordering so that all uses of this slot by the
                    // waiter happen before his final store to the slot's state, so that the slot is no longer used when we free it here.
                    atomic::fence(atomic::Ordering::Acquire);

                    let _ = unsafe { Box::from_raw(cur_slot_ptr) };
                } else {
                    // we gracefully finished waking this slot up and finished using it.
                    // it is up to the waiter to free it once he sees that we are done with it.

                    // make the store part of the just performed swap operation have release ordering so that all of our previous uses
                    // of this slot happen before that final store to the slot's state, so that when the notifier frees it, it is no
                    // longer in use by us.
                    //
                    // also, the notify operation of this data structure should have release ordering.
                    atomic::fence(atomic::Ordering::Release);
                }
            }

            cur_slot_ptr = next_slot_ptr;
        }
    }

    fn alloc_slot(&self) -> &Slot {
        let new_slot = Box::leak(Box::new(Slot {
            waker: spin::mutex::Mutex::new(None),
            state: AtomicU8::new(SLOT_STATE_IDLE),
            next: AtomicPtr::new(null_mut()),
        }));

        self.slots_list_head.update(
            // for the store part, use release ordering so that our initialization of the slot is visible once others see the pointer
            // published
            atomic::Ordering::Release,
            // for the load part, use acquire ordering to make sure that at this point we see all initialization performed to the
            // last slot. this is not important for us, but it does important to whoever than loads our pointer and goes over the list.
            // we want to make sure that he doesn't only see our initialization, but all initialization of all other pointers following
            // us in the list.
            // this acquire ordering paired with the release ordering of the store essentially crates a chain of ordering between all
            // slots in the list, guaranteeing that if anyone sees our pointer published, he sees our initialization of the slot and the
            // initialization of all slots after us in the list.
            atomic::Ordering::Acquire,
            |cur_list_head| {
                new_slot.next = AtomicPtr::new(cur_list_head);
                new_slot
            },
        );

        new_slot
    }

    pub fn notified(&self) -> Notified<'_> {
        Notified {
            slot: self.alloc_slot(),
        }
    }
}
impl Drop for Notify {
    fn drop(&mut self) {
        // at this point we have a mutable reference to the data structure.
        // all waiters are holding an immutable reference, so at this point we are guaranteed to not have any remaining waiters.
        // thus, there's no memory that needs to be cleaned up by us or anything like that.
    }
}

pub struct Notified<'a> {
    slot: &'a Slot,
}
impl<'a> Future for Notified<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // fast-path: before doing any waker registration, check if we have already been awakened.
        let cur_state = self.slot.state.load(
            // ordering is only needed if we were notified, so a fence is used.
            atomic::Ordering::Relaxed,
        );

        if cur_state >= SLOT_STATE_NOTIFIED {
            // when we are notified, we want acquire ordering since the data structure's wait operation should provide acquire ordering
            // to synchronize other data accessed by the threads using this notify instance.
            atomic::fence(atomic::Ordering::Acquire);

            return Poll::Ready(());
        }

        // sanity
        debug_assert_eq!(cur_state, SLOT_STATE_IDLE);

        // we have not yet been awakened. register our waker.
        {
            let mut waker_storage = self.slot.waker.lock();
            let new_waker = cx.waker();
            match &*waker_storage {
                Some(old_waker) if old_waker.will_wake(new_waker) => {
                    // can re-use the old waker
                }
                _ => {
                    // use the new waker
                    *waker_storage = Some(new_waker.clone());
                }
            }
        }

        // we need to re-check if we have been awakened instead of immediately going to sleep.
        //
        // this is needed to avoid a race condition where between the time we initially checked the state, and before we registered our
        // waker, someone woke us up. in that case, if we yield now, we will sleep forever.
        //
        // in that case, the notifier thread must have grabbed the waker lock before we grabbed it (otherwise he would have seen our
        // waker), and the notifier performs the store to the state before grabbing the lock. the acquire ordering used by us when
        // acquiring the lock, and the release ordering used by the notifier when he released the lock, guaranteed that in such a scenario
        // we are now guaranteed to see the notifier's store to the state, regardless of the ordering of the below store.
        let cur_state = self.slot.state.load(
            // ordering is only needed if we were notified, so a fence is used.
            atomic::Ordering::Relaxed,
        );

        if cur_state >= SLOT_STATE_NOTIFIED {
            // when we are notified, we want acquire ordering since the data structure's wait operation should provide acquire ordering.
            atomic::fence(atomic::Ordering::Acquire);

            return Poll::Ready(());
        }

        // sanity
        debug_assert_eq!(cur_state, SLOT_STATE_IDLE);

        Poll::Pending
    }
}

impl<'a> Drop for Notified<'a> {
    fn drop(&mut self) {
        let prev_value = self.slot.state.swap(
            SLOT_STATE_ABANDONED,
            // the memory ordering depends on the previous value, and fences are used to apply those memory orderings where appropriate.
            atomic::Ordering::Relaxed,
        );

        if prev_value == SLOT_STATE_DONE {
            // we can free this slot

            // make the load part of the just performed swap operation have acquire ordering so that all uses of this slot by the
            // notifier happen before his final store to the slot's state, so that the slot is no longer used when we free it here.
            atomic::fence(atomic::Ordering::Acquire);

            let slot_ptr = self.slot as *const Slot as *mut Slot;
            let _ = unsafe { Box::from_raw(slot_ptr) };
        } else {
            // we can't free this slot yet, it may be in use.
            // the waker will free it for us when he sees the abandoned state.

            // make the store part of the just performed swap operation have release ordering so that all of our previous uses of this
            // slot happen before that final store to the slot's state, so that when the notifier frees it, it is no longer in use by us.
            atomic::fence(atomic::Ordering::Release);
        }
    }
}
