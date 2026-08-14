use std::{
    pin::Pin,
    ptr::null_mut,
    sync::atomic::{self, AtomicPtr, AtomicU8},
    task::{Poll, Waker},
};

const SLOT_STATE_EMPTY: u8 = 0;
const SLOT_STATE_TAKEN: u8 = 1;
const SLOT_STATE_AWAKENED: u8 = 2;
const SLOT_STATE_DONE: u8 = 3;
const SLOT_STATE_ABANDONED: u8 = 4;

struct Slot {
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

            // awake him
            let prev_state = cur_slot.state.swap(
                SLOT_STATE_AWAKENED,
                // TODO: ordering
                atomic::Ordering::Relaxed,
            );
            if prev_state == SLOT_STATE_ABANDONED {
                // free this slot up
                let _ = unsafe { Box::from_raw(cur_slot_ptr) };
            } else {
                // wake him up
                {
                    let mut waker_storage = cur_slot.waker.lock();
                    if let Some(waker) = waker_storage.take() {
                        waker.wake();
                    }
                }

                // let him know that we're done using the slot and it can be freed.
                // the slot is no longer accessible from the `Notify` struct itself, it is only accessible to us, and we will no longer use
                // it from this point on.
                let prev_state = cur_slot.state.swap(
                    SLOT_STATE_DONE,
                    // TODO: ordering
                    atomic::Ordering::Relaxed,
                );

                if prev_state == SLOT_STATE_ABANDONED {
                    // if one again at this point the slot has already been abandoned, free it
                    let _ = unsafe { Box::from_raw(cur_slot_ptr) };
                }
            }

            cur_slot_ptr = next_slot_ptr;
        }
    }

    fn alloc_slot(&self) -> &'_ Slot {
        let new_slot = Box::leak(Box::new(Slot {
            waker: spin::mutex::Mutex::new(None),
            state: AtomicU8::new(SLOT_STATE_EMPTY),
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
        todo!("clean up chunk memory")
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
            // TODO: ordering
            atomic::Ordering::Relaxed,
        );
        if cur_state >= SLOT_STATE_AWAKENED {
            return Poll::Ready(());
        }

        // sanity
        debug_assert_eq!(cur_state, SLOT_STATE_TAKEN);

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
        let cur_state = self.slot.state.load(
            // TODO: ordering
            atomic::Ordering::Relaxed,
        );
        if cur_state >= SLOT_STATE_AWAKENED {
            return Poll::Ready(());
        }

        // sanity
        debug_assert_eq!(cur_state, SLOT_STATE_TAKEN);

        Poll::Pending
    }
}

impl<'a> Drop for Notified<'a> {
    fn drop(&mut self) {
        let prev_value = self.slot.state.swap(
            SLOT_STATE_ABANDONED,
            // TODO: ordering
            atomic::Ordering::Relaxed,
        );

        if prev_value == SLOT_STATE_DONE {
            // we can free this slot
            let slot_ptr = self.slot as *const Slot as *mut Slot;
            let _ = unsafe { Box::from_raw(slot_ptr) };
        } else {
            // we can't free this slot yet, it may be in use.
            // the waker will free it for us when he sees the abandoned state.
        }
    }
}
