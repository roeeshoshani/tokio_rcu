use std::sync::atomic;

use crate::atomic_type::Atomic;

/// an epoch id. valid epoch id values are all even integers greater than 0 (2,4,6,8,...).
pub type EpochId = u32;

static CUR_EPOCH_ID: Atomic<EpochId> = Atomic::<EpochId>::new(2);

#[inline]
pub fn epoch_id_get_cur(ordering: atomic::Ordering) -> EpochId {
    CUR_EPOCH_ID.load(ordering)
}

/// increments the epoch id, returning the new epoch id after the increment.
pub fn epoch_id_inc() -> EpochId {
    // TODO: ordering
    let orig = CUR_EPOCH_ID.fetch_add(2, atomic::Ordering::Relaxed);

    if orig != (EpochId::MAX - 1) {
        // SAFETY: in this case, the value is not equal to `EpochId::MAX - 1`, and any value other than
        // `EpochId::MAX - 1` will not overflow.
        return unsafe { orig.unchecked_add(2) };
    }

    // in this case, the increment caused the epoch id to become 0.
    // epoch id 0 is reserved, so try incrementing again if we reach 0.
    // TODO: ordering
    let orig = CUR_EPOCH_ID.fetch_add(2, atomic::Ordering::Relaxed);
    if orig == (EpochId::MAX - 1) {
        // in this case, we reached an epoch id of 0 again.
        //
        // this means that during the short time between the first increment and the second, so many increments
        // were performed that the epoch id fully wrapped around.
        //
        // this is extremely unlikely since it requires an unreasonable large amount of increments in a very short
        // amount of time, so we just expect it to never happen.
        panic!(
            "too many concurrent incremenets to the epoch id, failed to increment it beyond the reserved value of 0"
        );
    }

    // SAFETY: in this case, the value is not equal to `EpochId::MAX - 1`, and any value other than
    // `EpochId::MAX - 1` will not overflow.
    unsafe { orig.unchecked_add(2) }
}
