use crate::atomic_type::Atomic;
use crate::loom_or_std::sync::atomic;

/// an epoch id. valid epoch id values are all even integers greater than 0 (2,4,6,8,...).
///
/// the reason for these constraints is to allow for memory-efficient encoding of the thread state.
///
/// the fact that all epoch ids are even allows us to encode information in the least significant bit.
/// and, the fact that the epoch id is non-zero allows us to encode an empty thread state value as just a 0 value.
pub type EpochId = u16;

/// the minimum valid epoch id value.
pub const EPOCH_ID_MIN: EpochId = 2;

/// the maximum practical value the epoch id will ever have.
/// the epoch id grows in increments of 2 and starts at a value of 2, so it is always even.
/// thus, its max value is not the underlying integer type's max value, instead it is 1 less.
pub const EPOCH_ID_MAX: EpochId = EpochId::MAX - 1;

#[cfg(not(loom))]
static CUR_EPOCH_ID: Atomic<EpochId> = Atomic::<EpochId>::new(EPOCH_ID_MIN);

// loom's `Atomic*::new` function is not const, so we can't directly initialize an atomic variable like in the non loom case.
// so, we must use some other mechanism.
#[cfg(loom)]
static CUR_EPOCH_ID: std::sync::OnceLock<Atomic<EpochId>> = std::sync::OnceLock::new();

/// an error returned when an overflow is detected while trying to increment the epoch id.
#[derive(Debug)]
pub struct EpochIdOverflowErr {
    is_leader: bool,
}
impl EpochIdOverflowErr {
    /// returns whether you are the leader of the overflow of the epoch id, that is, whether you are the exact increment that brought
    /// the epoch id to its max value.
    ///
    /// for every overflow of the epoch id, there is exactly one leader.
    pub fn am_i_the_leader(&self) -> bool {
        self.is_leader
    }
}

/// provides access to the variable containing the current epoch id.
/// this functions exists to hide the details of how the epoch id is implemented in loom mode versus regular mode.
fn access_cur_epoch_id() -> &'static Atomic<EpochId> {
    #[cfg(not(loom))]
    {
        &CUR_EPOCH_ID
    }

    #[cfg(loom)]
    {
        CUR_EPOCH_ID.get_or_init(|| Atomic::<EpochId>::new(EPOCH_ID_MIN))
    }
}

/// loads the current epoch id atomically with the given ordering. this only performs a single atomic load operation.
#[inline]
pub fn epoch_id_get(ordering: atomic::Ordering) -> EpochId {
    access_cur_epoch_id().load(ordering)
}

/// a function which performs a [`try_update`](std::sync::atomic::AtomicUsize::try_update) operation on the epoch id.
/// selects between the loom and the std API depending on the current configuration.
/// this is needed since loom's `try_update` function has a different name than the stdlib one.
#[inline]
fn epoch_id_try_update<F>(
    set_order: atomic::Ordering,
    fetch_order: atomic::Ordering,
    f: F,
) -> Result<EpochId, EpochId>
where
    F: FnMut(EpochId) -> Option<EpochId>,
{
    #[cfg(loom)]
    {
        access_cur_epoch_id().fetch_update(set_order, fetch_order, f)
    }

    #[cfg(not(loom))]
    {
        access_cur_epoch_id().try_update(set_order, fetch_order, f)
    }
}

/// increments the epoch id atomically, returning the new epoch id after the increment.
///
/// if the epoch id would overflow due to the increment, this function returns an error, and the epoch id is guaranteed to be set to its
/// max value until reset.
///
/// in case this function succeeds, the increment has release ordering, guaranteeing that the swap of the
/// rcu protected pointer happens before the increment of the epoch id whenever the epoch id is loaded with acquire ordering when the
/// threads pass through a quiescent state.
///
/// in the failure case, the increment may or many not happen, and if it does, it happens with relaxed ordering.
#[inline]
pub fn epoch_id_inc() -> Result<EpochId, EpochIdOverflowErr> {
    match epoch_id_try_update(
        // for the success case, we may want release ordering, but only if the value is still below the overflow threshold.
        //
        // so, we will use conditionally apply an atomic fence later if we determine that we need to, while here we just use a
        // relaxed ordering.
        atomic::Ordering::Relaxed,
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
                return Err(EpochIdOverflowErr { is_leader: true });
            }

            // for the success case, we want release ordering to guarantee that the swapping of the rcu protected pointer happens before
            // the increment of the epoch id, otherwise someone may see the increment before the swap of the pointer, causing us to release
            // the data, where he would then try using that freed data.
            atomic::fence(atomic::Ordering::Release);
            Ok(new_epoch_id)
        }
        Err(prev_epoch_id) => {
            // sanity
            debug_assert_eq!(prev_epoch_id, EPOCH_ID_MAX);

            Err(EpochIdOverflowErr { is_leader: false })
        }
    }
}

/// directly sets the epoch id to the given value with the given ordering.
pub fn epoch_id_set(new_value: EpochId, ordering: atomic::Ordering) {
    access_cur_epoch_id().store(new_value, ordering);
}
