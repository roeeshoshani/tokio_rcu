use crate::epoch::EpochId;

pub type EncodedThreadState = u32;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ThreadState {
    pub last_seen_epoch_id: EpochId,
    pub is_busy: bool,
}
impl ThreadState {
    pub const NONE_ENCODED_VALUE: EncodedThreadState = 0;

    #[inline]
    pub fn decode(encoded: EncodedThreadState) -> Option<Self> {
        if encoded == Self::NONE_ENCODED_VALUE {
            None
        } else {
            let last_seen_epoch_id = encoded & (!1);
            debug_assert_ne!(last_seen_epoch_id, 0);

            Some(Self {
                last_seen_epoch_id,
                is_busy: (encoded & 1) != 0,
            })
        }
    }

    #[inline]
    pub fn encode(self) -> EncodedThreadState {
        debug_assert_ne!(self.last_seen_epoch_id, 0);
        debug_assert_eq!(self.last_seen_epoch_id & 1, 0);

        let result = self.last_seen_epoch_id | EncodedThreadState::from(self.is_busy);

        debug_assert!(result != Self::NONE_ENCODED_VALUE);

        result
    }
}
