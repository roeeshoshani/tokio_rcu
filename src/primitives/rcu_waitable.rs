//! abstractions over values which require waiting an rcu grace period for them to be transformed into their output values.

use crate::synchronize_rcu;

/// represents a value which requires waiting an rcu grace period in order to transform it into its output value.
///
/// for example, this may represent the old pointer of a swapped rcu pointer, which requires waiting an rcu grace period in order for it to be
/// reclaimed.
///
/// rcu waitable values can be waited for using the [`rcu_wait_for`] function.
pub trait RcuWaitable: Sized {
    /// the output value that will be produced after waiting the rcu grace period on this source value.
    type Output;

    /// transforms this source value into its output value, assuming that an rcu grace period has been waited for since this source value was created.
    ///
    /// this function should normally not be used directly, you should use the safe [`rcu_wait_for`] function instead.
    ///
    /// # Safety
    ///
    /// this must only be called after waiting for a grace period that started after `self` was created.
    unsafe fn into_output(self) -> Self::Output;
}

/// waits an rcu grace period, and then transofmrs the provided value into its post-grace-period output value.
pub async fn rcu_wait_for<T: RcuWaitable>(value: T) -> T::Output {
    synchronize_rcu().await;

    // SAFETY: we waited an rcu grace period, so the value can now be transformed
    unsafe { value.into_output() }
}
