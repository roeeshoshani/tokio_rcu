//! tokio related logic and extension traits.

use std::task::Poll;

use tokio::runtime::RuntimeFlavor;

use crate::{
    rcu_core::{
        on_after_task_poll, on_before_task_poll, on_thread_park, on_thread_stop, on_thread_unpark,
    },
    utils::unlikely,
};

/// extension methods for tokio's runtime builder.
pub trait TokioRuntimeBuilderExt {
    /// enable rcu support for this tokio runtime.
    /// must be called when constructing the runtime in order to use any rcu related primitive inside the runtime.
    ///
    /// # Safety
    ///
    /// when used, in order to use any of the rcu primitives safely, you must wrap the [`rcu_block_on`](TokioRuntimeExt::rcu_block_on)
    /// function to run the main future on the runtime. using [`block_on`](tokio::runtime::Runtime::block_on) directly is forbidden.
    ///
    /// furthermore, after calling this function, you must not register any tokio hooks of your own, since this functions registers the
    /// rcu hooks needed for book-keeping. overriding any of those hooks will lead to undefined behaviour.
    unsafe fn enable_rcu(&mut self) -> &mut Self;
}

#[cfg(not(loom))]
impl TokioRuntimeBuilderExt for tokio::runtime::Builder {
    unsafe fn enable_rcu(&mut self) -> &mut Self {
        self.on_before_task_poll(|_| {
            on_before_task_poll();
        })
        .on_thread_stop(|| {
            on_thread_stop();
        })
        .on_thread_park(|| {
            on_thread_park();
        })
        .on_thread_unpark(|| {
            on_thread_unpark();
        })
        .on_after_task_poll(|_| {
            on_after_task_poll();
        })
    }
}

/// extension methods for tokio's runtime.
pub trait TokioRuntimeExt {
    /// runs a future to completion on the tokio runtime, with RCU support.
    ///
    /// this can only be used with multi-threaded runtimes.
    ///
    /// # Safety
    ///
    /// to use this, you must first call [`enable_rcu`](TokioRuntimeBuilderExt::enable_rcu) when building the runtime.
    unsafe fn rcu_block_on<F: Future>(&self, future: F) -> F::Output;
}
impl TokioRuntimeExt for tokio::runtime::Runtime {
    unsafe fn rcu_block_on<F: Future>(&self, future: F) -> F::Output {
        // rcu is only supported for multithreaded runtimes
        assert_eq!(self.handle().runtime_flavor(), RuntimeFlavor::MultiThread);

        self.block_on(unsafe {
            // SAFETY: we pass the wrapped future directly to `block_on`
            RcuRootFuture::new(future)
        })
    }
}

#[cfg(not(loom))]
/// runs the provided future inside a new multi-threaded tokio runtime with all features enabled and with rcu support.
pub fn rcu_block_on<F: Future>(future: F) -> F::Output {
    unsafe {
        // SAFETY: we use `rcu_block_on`
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .enable_rcu()
            .build()
            .unwrap();

        // SAFETY: we called `enable_rcu`
        rt.rcu_block_on(future)
    }
}

/// a wrapper around the root future of a tokio `block_on` call.
///
/// this is required since tokio's hooks only apply to tokio's worker threads, but not to the main thread which initially calls `block_on`.
///
/// but, we need the main thread to also perform the book-keeping needed by the rcu primitive, in order for it to be able use the rcu
/// primitives and to interact with the other threads using the rcu primitives.
///
/// so, we wrap the main future passed to `block_on` in a custom wrapper which emulates the call to the different worker hooks.
/// this lets the main thread participate in the book-keeping like any other worker thread.
#[derive(Debug, Clone, Copy)]
struct RcuRootFuture<F> {
    inner_future: F,
    has_already_been_polled: bool,
}
impl<F> RcuRootFuture<F> {
    /// wraps the provided future with the rcu root future logic.
    ///
    /// # Safety
    ///
    /// may only be used to wrap the future provided to tokio's `block_on` function on a multithreaded runtime.
    /// using this incorrectly will lead to undefined behaviour.
    unsafe fn new(inner_future: F) -> Self {
        Self {
            inner_future,
            has_already_been_polled: false,
        }
    }
}
impl<F: Future> Future for RcuRootFuture<F> {
    type Output = F::Output;

    // #[inline] is important here since it increases the chance that some of the redundant branches performed in this function
    // will be eliminated, for example the `has_already_been_polled` branch, which is only used to track the first call to
    // `poll` and can easily be eliminated by unrolling the first iteration of the poll loop.
    #[inline]
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        if unlikely(!self.has_already_been_polled) {
            // first time being polled on the main thread.

            // SAFETY: we don't move out of anything
            unsafe { self.as_mut().get_unchecked_mut().has_already_been_polled = true }
        } else {
            // we have already been polled in the previous iteration.
            //
            // we have returned `Poll::Pending` in the previous iteration, so the main thread parked itself and went to sleep,
            // and now we are being polled again.
            //
            // this is basically an unpark.
            on_thread_unpark();
        }

        // before polling the task
        on_before_task_poll();

        // SAFETY: we do not move out of anything, we just project a field, which is safe
        let inner_future = unsafe { self.map_unchecked_mut(|x| &mut x.inner_future) };

        let res = inner_future.poll(cx);

        // just finished polling the task.
        on_after_task_poll();

        match res {
            Poll::Ready(_) => {
                // in this case, the main future is done, so the main thread is also done.
                on_thread_stop();
            }
            Poll::Pending => {
                // if we return `Poll::Pending`, the main thread will park itself until an IO event occurs and wakes it up.
                //
                // so this is basically a thread park.
                on_thread_park();
            }
        }

        res
    }
}
