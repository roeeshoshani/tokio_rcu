use spin::mutex::FairMutex;

// TODO: documentation for almost everything in this module is missing

const NO_OWNER: u8 = 2;

struct State {
    group_counts: [usize; 2],
    cur_owner_group_index: u8,
}

/// a fair group mutual exclusion lock for exactly 2 groups.
pub struct FairGmeLock {
    state: FairMutex<State>,
}
impl FairGmeLock {
    pub const fn new() -> Self {
        Self {
            state: FairMutex::new(State {
                group_counts: [0; _],
                cur_owner_group_index: NO_OWNER,
            }),
        }
    }

    fn lock_group(&self, group_index: u8) -> GmeLockGuard<'_> {
        let mut already_incremented = false;
        loop {
            // re-lock the state every attempt.
            //
            // this, combined with the fact that the mutex is fair, should give access to other concurrent threads trying to access the
            // state.
            let mut state = self.state.lock();
            if state.cur_owner_group_index == group_index {
                // lock is currently locked by our group.
                // we can grab it as well, unless people from the other group are waiting.
                // in that case, we must wait for the other group to grab it first, for the mutex to be fair

                if already_incremented {
                    // in this case, we have started waiting when the lock belonged to the other group, and it now moved to our group,
                    // but we have already incremented and counted ourselves in, so we can just use the lock.
                    //
                    // we are not starving the other group since the waiters from that other group have only arrived after we started
                    // waiting.
                    return GmeLockGuard {
                        lock: self,
                        group_index,
                    };
                }

                let other_group_index = group_index ^ 1;
                if state.group_counts[other_group_index as usize] > 0 {
                    // can't join, need to wait until other group grabs the lock first
                } else {
                    // no-one from the other group wants the lock, join the current lockers from our group
                    // NOTE: in this case we haven't yet incremented the group count, the already incremented case was handled above.
                    state.group_counts[group_index as usize] += 1;
                    return GmeLockGuard {
                        lock: self,
                        group_index,
                    };
                }
            } else if state.cur_owner_group_index == NO_OWNER {
                // lock is unlocked, grab it
                if !already_incremented {
                    state.group_counts[group_index as usize] += 1;
                }
                state.cur_owner_group_index = group_index;
                return GmeLockGuard {
                    lock: self,
                    group_index,
                };
            } else {
                // lock is currently locked by other group, let them know we're waiting
                if !already_incremented {
                    state.group_counts[group_index as usize] += 1;
                    already_incremented = true;
                }
            }

            drop(state);
            core::hint::spin_loop();
        }
    }

    pub fn lock_group_a(&self) -> GmeLockGuard<'_> {
        self.lock_group(0)
    }

    pub fn lock_group_b(&self) -> GmeLockGuard<'_> {
        self.lock_group(1)
    }
}

pub struct GmeLockGuard<'a> {
    lock: &'a FairGmeLock,
    group_index: u8,
}
impl<'a> Drop for GmeLockGuard<'a> {
    fn drop(&mut self) {
        let mut state = self.lock.state.lock();
        state.group_counts[self.group_index as usize] -= 1;
        if state.group_counts[self.group_index as usize] == 0 {
            let other_group_index = self.group_index ^ 1;
            if state.group_counts[other_group_index as usize] > 0 {
                // the other group is waiting, give it ownership directly for fairness.
                // if we just release the lock, someone from our group may grab it, thus breaking the fairness guarantee.
                state.cur_owner_group_index = other_group_index;
            } else {
                // no-one wants the lock, just release it
                state.cur_owner_group_index = NO_OWNER;
            }
        }
    }
}
