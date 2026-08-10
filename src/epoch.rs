use std::sync::atomic;

use crate::atomic_type::Atomic;

/// an epoch id. valid epoch id values are all even integers greater than 0 (2,4,6,8,...).
pub type EpochId = u32;

/// the minimum valid epoch id value.
pub const MIN_EPOCH_ID: EpochId = 2;

static CUR_EPOCH_ID: Atomic<EpochId> = Atomic::<EpochId>::new(MIN_EPOCH_ID);

/// loads the current epoch id atomically with the given ordering. this only performs a single atomic load operation.
#[inline]
pub fn epoch_id_get_cur(ordering: atomic::Ordering) -> EpochId {
    CUR_EPOCH_ID.load(ordering)
}

/// increments the epoch id atomically, returning the new epoch id after the increment.
/// the increment has [`Release`](atomic::Ordering::Release) ordering.
pub fn epoch_id_inc() -> EpochId {
    let orig = CUR_EPOCH_ID.fetch_add(
        2,
        // for the load part, we don't need any ordering since we don't need to synchronize with other waiters that increment the epoch
        // id, we are only synchronizing with users of the rcu protected pointer.
        //
        // for the store part, we want release ordering to guarantee that the swapping of the rcu protected pointer happens before
        // the increment of the epoch id, otherwise someone may see the increment before the swap of the pointer, causing us to release
        // the data, where he would then try using that freed data.
        atomic::Ordering::Release,
    );

    if orig != (EpochId::MAX - 1) {
        // SAFETY: in this case, the value is not equal to `EpochId::MAX - 1`, and any value other than
        // `EpochId::MAX - 1` will not overflow.
        return unsafe { orig.unchecked_add(2) };
    }

    // in this case, the increment caused the epoch id to become 0.
    // epoch id 0 is reserved, so try incrementing again if we reach 0.
    let orig = CUR_EPOCH_ID.fetch_add(
        2,
        // for an explanation of why we use release ordering here, see the previous epoch id increment operation.
        atomic::Ordering::Release,
    );
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
