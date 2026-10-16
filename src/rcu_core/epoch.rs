use crate::loom::std::sync::atomic;
use crate::rcu_core::qs_read_mostly_data::cur_epoch_id;
use crate::utils::unlikely;

/// an epoch id. valid epoch id values are all even integers greater than 0 (2,4,6,8,...).
///
/// the reason for these constraints is to allow for memory-efficient encoding of the thread state.
///
/// the fact that all epoch ids are even allows us to encode information in the least significant bit.
/// and, the fact that the epoch id is non-zero allows us to encode an empty thread state value as just a 0 value.
pub type EpochId = cfg_select! {
    feature = "small_epoch_id" => {
        // the small epoch id config is used for the integration tests, for testing the logic handling the epoch id overflow.
        // the default epoch id is too big to overflow during the tests (it would take a lot of time to overflow it).
        // this config shrinks the size of the epoch id so that it becomes reasonable to overflow it during the tests, for making sure
        // that the overflow handling logic works fine.
        //
        // we just use the smallest type possible, to make sure that we get as many resets as possible during the tests.
        u8
    }
    _ => {
        // for the default case, use a u32.
        //
        // the choice of the size of the epoch id affects 2 things:
        // - it affects the memory usage of this crate, since it determines the size of the per-thread state storage.
        // - it affects how frequently the epoch id overflows, which determines how frequently we need to perform a full synchronization.
        //
        // on the one hand, we want it to be as small as possible, to avoid wasting too much memory on the per thread state storage.
        // on the other hand, we want it to be as big as possible to make overflow synchronizations as infrequent as possible, since those
        // synchronization operations are very expensive.
        //
        // using a u32 seems to be a good balance between the two constraints.
        u32
    }
};

/// the minimum valid epoch id value.
pub const EPOCH_ID_MIN: EpochId = 2;

/// the maximum practical value the epoch id will ever have.
/// the epoch id grows in increments of 2 and starts at a value of 2, so it is always even.
/// thus, its max value is not the underlying integer type's max value, instead it is 1 less.
pub const EPOCH_ID_MAX: EpochId = EpochId::MAX - 1;

/// loads the current epoch id atomically with the given ordering. this only performs a single atomic load operation.
#[inline(always)]
pub fn epoch_id_get(ordering: atomic::Ordering) -> EpochId {
    cur_epoch_id().load(ordering)
}

/// directly sets the epoch id to the given value with the given ordering.
#[inline(always)]
pub fn epoch_id_set(new_value: EpochId, ordering: atomic::Ordering) {
    cur_epoch_id().store(new_value, ordering);
}
