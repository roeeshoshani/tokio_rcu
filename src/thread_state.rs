use crate::epoch::EpochId;

/// the encoded state of a thread.
///
/// we use an integer with the same size as the epoch id, since the encoded state is essentially just the epoch id with extra
/// information stored in its lsb.
pub type EncodedThreadState = EpochId;

/// the state of a thread needed for the rcu book-keeping.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ThreadState {
    /// the last epoch id seen by this thread.
    pub last_seen_epoch_id: EpochId,

    /// is this thread currently busy running code, or is it parked waiting for io events.
    pub is_busy: bool,
}
impl ThreadState {
    /// an encoded thread state value which represents an invalid thread state, used to represent a vacant slot in the per-thread
    /// storage array.
    pub const NONE_ENCODED_VALUE: EncodedThreadState = 0;

    /// decodes the given encoded thread state.
    ///
    /// if the thread state is invalid, returns `None`. this is used to represent a vacant slot in the storage array.
    #[inline]
    pub fn decode(encoded: EncodedThreadState) -> Option<Self> {
        if encoded == Self::NONE_ENCODED_VALUE {
            None
        } else {
            let last_seen_epoch_id = (encoded & (!1)) as EpochId;
            debug_assert_ne!(last_seen_epoch_id, 0);

            Some(Self {
                last_seen_epoch_id,
                is_busy: (encoded & 1) != 0,
            })
        }
    }

    /// encodes the given thread state into its packed form.
    #[inline]
    pub fn encode(self) -> EncodedThreadState {
        debug_assert_ne!(self.last_seen_epoch_id, 0);
        debug_assert_eq!(self.last_seen_epoch_id & 1, 0);

        let result =
            self.last_seen_epoch_id as EncodedThreadState | EncodedThreadState::from(self.is_busy);

        debug_assert!(result != Self::NONE_ENCODED_VALUE);

        result
    }
}
