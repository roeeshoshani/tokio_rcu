//! abstractions over values which require waiting an rcu grace period for them to be transformed into their output values.
//!
//! the main trait provided by this module is the [`RcuWaitable`] trait, which represents an object which requires waiting an rcu grace period for
//! it to be transformed into its output value.

use std::task::Poll;

use crate::synchronize_rcu;

/// represents a value which requires waiting an rcu grace period in order to transform it into its output value.
///
/// for example, this may represent the old pointer of a swapped rcu pointer, which requires waiting an rcu grace period in order for it to be
/// reclaimed.
///
/// rcu waitable values can be waited for using the [`wait`](Self::wait) function.
///
/// note that this trait is automatically implemented for tuples in which all the items implement [`RcuWaitable`]. this allows performing batch wait
/// operations to wait for multiple [`RcuWaitable`] objects using only a single rcu grace period. see [`wait`](Self::wait) for more info.
pub trait RcuWaitable: Sized {
    /// the output value that will be produced after waiting the rcu grace period on this source value.
    type Output;

    /// transforms this source value into its output value, assuming that an rcu grace period has been waited for since this source value was created.
    ///
    /// this function should normally not be used directly, you should use the safe [`wait`](Self::wait) function instead.
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
    ///
    /// # example
    ///
    /// ```rust
    /// # use tokio_rcu::{rcu_block_on, primitives::{rcu_box::RcuBox, rcu_waitable::RcuWaitable}};
    /// # rcu_block_on(async {
    /// let value = RcuBox::new(Box::new(vec![1, 2, 3]));
    /// let old_data = value.swap(Box::new(vec![4, 88, 12, 59, 33])).wait().await;
    /// assert_eq!(*old_data, vec![1, 2, 3]);
    /// # })
    /// ```
    ///
    /// # combined wait
    ///
    /// this function can be used to wait for multiple [`RcuWaitable`] objects at once while only performing a single rcu grace period.
    /// this can be achieved by combining them into a tuple and then calling [`wait`](Self::wait) on the entire tuple.
    ///
    /// ```rust
    /// # use tokio_rcu::{rcu_block_on, primitives::{rcu_box::RcuBox, rcu_waitable::RcuWaitable}};
    /// # rcu_block_on(async {
    /// let rcu_a = RcuBox::new(Box::new("a"));
    /// let rcu_b = RcuBox::new(Box::new(vec![1, 2, 3]));
    /// let rcu_c = RcuBox::new(Box::new(78));
    ///
    /// let (old_a, old_b, old_c) = (
    ///     rcu_a.swap(Box::new("aaaa")),
    ///     rcu_b.swap(Box::new(vec![4, 88, 12, 59, 33])),
    ///     rcu_c.swap(Box::new(9120))
    /// ).wait().await;
    ///
    /// assert_eq!(*old_a, "a");
    /// assert_eq!(*old_b, vec![1, 2, 3]);
    /// assert_eq!(*old_c, 78);
    /// # })
    /// ```
    ///
    /// # cancellation safety
    ///
    /// function is generally not cancellation safe.
    ///
    /// specific implementors of this trait may allow it to be cancelled, but most implementations are not cancellation safe, so you should just assume
    /// that it is not cancellation safe.
    ///
    /// rcu waitable objects usually require you to wait for a grace period before they can be accessed in any way.
    /// cancelling this future means dropping the rcu waitable object before finishing the grace period wait.
    /// so, in that case, the rcu waitable object can't even be properly dropped, since it may still be accessed by concurrent readers.
    /// types that can't be dropped before the grace period will panic in their drop implementation if this future is cancelled.
    fn wait(self) -> RcuWaitableWait<Self, impl Future<Output = ()>> {
        RcuWaitableWait {
            src_value: Some(self),
            synchronize_rcu: synchronize_rcu(),
        }
    }
}

/// the future which represents the operation of waiting on an [`RcuWaitable`] object, returned when calling [`RcuWaitable::wait`].
///
/// # cancellation safety
///
/// this future is not cancellation safe. see [`RcuWaitable::wait`] for more info.
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

/// a helper macro used to implement the [`RcuWaitable`] for tuples made of types that all implement [`RcuWaitable`], so that you can perform a single
/// grace period while transforming multiple [`RcuWaitable`] objects into their outputs at once.
macro_rules! impl_rcu_waitable_for_tuple {
    { $(($index: tt, $t: ident)),+ } => {
        impl<$($t: RcuWaitable),+> RcuWaitable for ($($t),+) {
            type Output = ($(<$t as RcuWaitable>::Output),+);

            unsafe fn into_output(self) -> Self::Output {
                (
                    $(
                        // SAFETY: caller guarantees that a grace period has been waited for
                        unsafe { self.$index.into_output() }
                    ),+
                )
            }
        }
    };
}
impl_rcu_waitable_for_tuple! { (0, A), (1, B) }
impl_rcu_waitable_for_tuple! { (0, A), (1, B), (2, C) }
impl_rcu_waitable_for_tuple! { (0, A), (1, B), (2, C), (3, D) }
impl_rcu_waitable_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E) }
impl_rcu_waitable_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E), (5, F) }
impl_rcu_waitable_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G) }
impl_rcu_waitable_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H) }
impl_rcu_waitable_for_tuple! { (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H), (8, I) }
impl_rcu_waitable_for_tuple! {
    (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H), (8, I), (9, J)
}
impl_rcu_waitable_for_tuple! {
    (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H), (8, I), (9, J), (10, K)
}
impl_rcu_waitable_for_tuple! {
    (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H), (8, I), (9, J), (10, K), (11, L)
}
