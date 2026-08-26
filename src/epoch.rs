use std::sync::atomic;

use crate::atomic_type::Atomic;

/// an epoch id. valid epoch id values are all even integers greater than 0 (2,4,6,8,...).
///
/// the reason for these constraints is to allow for memory-efficient encoding of the thread state.
///
/// the fact that all epoch ids are even allows us to encode information in the least significant bit.
/// and, the fact that the epoch id is non-zero allows us to encode an empty thread state value as just a 0 value.
#[cfg(not(test))]
pub type EpochId = u32;

// for testing, use a small type for the epoch id so that we actually experience overflows and check that code path.
#[cfg(test)]
pub type EpochId = u16;

/// the minimum valid epoch id value.
pub const EPOCH_ID_MIN: EpochId = 2;

/// the maximum practical value the epoch id will ever have.
/// the epoch id grows in increments of 2 and starts at a value of 2, so it is always even.
/// thus, its max value is not the underlying integer type's max value, instead it is 1 less.
pub const EPOCH_ID_MAX: EpochId = EpochId::MAX - 1;

/// the current global epoch id.
/// its value must always be a valid epoch id value.
///
/// used to synchronize threads that are waiting for a grace period with all other threads, by making all threads constantly load this
/// value and publish their last seen epoch id.
/// a waiter can then increment it and wait until all threads see his increment in their last seen epoch id value.
static CUR_EPOCH_ID: Atomic<EpochId> = Atomic::<EpochId>::new(EPOCH_ID_MIN);

/// an error returned when an overflow is detected while trying to increment the epoch id.
#[derive(Debug)]
pub struct EpochIdOverflowErr {
    /// when an overflow occurs, we enter a heavy synchronization state where we restore the epoch id back to its minimum.
    /// during this period, before we enter that heavy synchronization mode, more threads may try to increment the epoch id even further.
    /// but, only one thread will be the first thread to increment the epoch id past the max allowed value. this thread is called the
    /// leader.
    /// for every overflow of the epoch id, there is exactly one leader.
    pub am_i_the_leader: bool,
}

/// loads the current epoch id atomically with the given ordering. this only performs a single atomic load operation.
#[inline]
pub fn epoch_id_get(ordering: atomic::Ordering) -> EpochId {
    CUR_EPOCH_ID.load(ordering)
}

/// increments the epoch id atomically, returning the new epoch id after the increment.
///
/// if the epoch id would overflow due to the increment, this function returns an error, and the epoch id is guaranteed to be set to its
/// max value until reset.
///
/// in case this function succeeds, the increment has release ordering, guaranteeing that the swap of the rcu protected pointer happens
/// before the increment of the epoch id whenever the epoch id is loaded with acquire ordering when the threads pass through a quiescent
/// state.
///
/// in the failure case, the increment may or many not happen, and if it does, it happens with release ordering.
#[inline]
pub fn epoch_id_inc() -> Result<EpochId, EpochIdOverflowErr> {
    match CUR_EPOCH_ID.try_update(
        // for the success case, we want release ordering to guarantee that the swapping of the rcu protected pointer happens before
        // the increment of the epoch id, otherwise someone may see the increment before the swap of the pointer, causing us to release
        // the data, where he would then try using that freed data.
        atomic::Ordering::Release,
        // for the load part we don't need any ordering since we are not synchronizing with the previous value in any way.
        //
        // the only relevance of the previous value is for determining whether we are the leader of the overflow in case of overflow,
        // but that only requires atomicity, not any special ordering.
        //
        // furthermore, note that due to this being a rmw operation, this does not break the existing release-sequence of this variable,
        // so anyone loading our specific store will still synchronize with all previous incrementors.
        atomic::Ordering::Relaxed,
        |epoch_id| epoch_id.checked_add(2),
    ) {
        Ok(prev_epoch_id) => {
            // SAFETY: in this case we know that adding 2 does not overflow, since this is the success case
            let new_epoch_id = unsafe { prev_epoch_id.unchecked_add(2) };

            if new_epoch_id > EpochId::MAX - 2 {
                // sanity
                debug_assert_eq!(new_epoch_id, EPOCH_ID_MAX);

                // in this case, we got to the max possible value of the epoch id.
                // this is treated as an "overflow" state of the epoch id, and the epoch id now requires a reset.
                //
                // all quiescent states will see this value and understand that they need to reset their last seen epoch id.
                //
                // all new waiters will fail the increment and will thus also enter the reset state.
                return Err(EpochIdOverflowErr {
                    am_i_the_leader: true,
                });
            }

            Ok(new_epoch_id)
        }
        Err(prev_epoch_id) => {
            // sanity
            debug_assert_eq!(prev_epoch_id, EPOCH_ID_MAX);

            Err(EpochIdOverflowErr {
                am_i_the_leader: false,
            })
        }
    }
}

/// directly sets the epoch id to the given value with the given ordering.
pub fn epoch_id_set(new_value: EpochId, ordering: atomic::Ordering) {
    CUR_EPOCH_ID.store(new_value, ordering);
}
