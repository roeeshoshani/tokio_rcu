//! abstractions over values which require waiting an rcu grace period for them to be transformed into their output values.

use std::task::Poll;

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

    /// waits an rcu grace period, and then transforms `self` into its post-grace-period output value.
    ///
    /// this function is semantically equivalent to `async fn wait(self) -> T::Output`, but a custom future is used instead since using `async fn` in
    /// trait methods does not allow providing `Send` and `Sync` guarantees on the returned future, while using a named type allows it to automatically
    /// be deduced based on whether self is `Send` and `Sync`.
    fn wait(self) -> RcuWaitableWait<Self, impl Future<Output = ()>> {
        RcuWaitableWait {
            src_value: Some(self),
            synchronize_rcu: synchronize_rcu(),
        }
    }
}

/// the future which represents the operation of waiting on an [`RcuWaitable`] object, returned when calling [`RcuWaitable::wait`].
pub struct RcuWaitableWait<T: RcuWaitable, S: Future<Output = ()>> {
    src_value: Option<T>,
    synchronize_rcu: S,
}
impl<T: RcuWaitable, S: Future<Output = ()>> Future for RcuWaitableWait<T, S> {
    type Output = T::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        // SAFETY: we are accessing a field of `self` and we never move out of it.
        let synchronize_rcu =
            unsafe { self.as_mut().map_unchecked_mut(|x| &mut x.synchronize_rcu) };
        match synchronize_rcu.poll(cx) {
            Poll::Ready(()) => {
                // SAFETY: we never move out of self, we only modify the `src_value` field.
                let this = unsafe { self.as_mut().get_unchecked_mut() };

                let src_value = this
                    .src_value
                    .take()
                    .expect("future polled after completion");

                // SAFETY: we just finished waiting a grace period, so we can now transform the source value into its output value
                let output = unsafe { src_value.into_output() };

                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// waits an rcu grace period, and then transforms the provided value into its post-grace-period output value.
pub async fn rcu_wait_for<T: RcuWaitable>(value: T) -> T::Output {
    synchronize_rcu().await;

    // SAFETY: we waited an rcu grace period, so the value can now be transformed
    unsafe { value.into_output() }
}
