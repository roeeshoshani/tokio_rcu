//! operations related to performing an rcu read lock operation for reading rcu protected data.

use crate::{is_rcu_tracked_thread, utils::PhantomUnsendUnsync};

/// an rcu read lock guard, providing access to reading rcu protected data.
///
/// this guard can be only be created on an rcu-tracked thread (see [`is_rcu_tracked_thread`]), so that only tracked threads can access rcu protected
/// data.
///
/// furthermore, this guard can't be sent or shared with other threads, otherwise it could have been shared with non-rcu tracked threads, thus allowing
/// them to read rcu protected data, which is not allowed. this is enforced at compile time by the guard being [`!Send`](Send) and [`!Sync`](Sync).
///
/// additionally, this guard must not be held across await points, must not be held after the future that acquired it finishes, and must not escape
/// that future's context (e.g. must not be saved inside a global variable and held across an await point or longer than the future's lifetime).
///
/// as soon as the future that acquired the guard gets to a point where it `await`s or finishes execution (basically any point which voluntarily
/// yields the future), the guard must have already been dropped.
///
/// this type should generally not be used directly. you should instead use the safe [`rcu_read_lock`] API.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RcuReadLockGuard {
    /// the guard must not be sent to other threads, otherwise it could be safely constructed on an rcu tracked thread, and then sent to a
    /// non rcu tracked thread, thus allowing non rcu tracked threads to read rcu protected data.
    /// the guard must also not be shared for the exact same reason - it would allow non rcu tracked threads to read rcu protected data.
    /// the guard is basically pinned to the thread that created it.
    ///
    /// this also means that on async runtimes that require their futures to be [`Send`], this guard can't be held across await points, which a nice
    /// bonus to this limitation, but is not strictly required for this type.
    /// this crate takes different measures to ensure the guard truly can't be held across await points, even on runtimes that allow non-[`Send`]
    /// futures, to prevent users from violating the rcu reader contract.
    phantom: PhantomUnsendUnsync,
}
impl RcuReadLockGuard {
    /// creates a new rcu read lock guard, without checking if the current thread is an rcu tracked thread.
    ///
    /// for a safe alternative, consider using [`rcu_read_lock`].
    ///
    /// # Performance
    ///
    /// this function is completely zero cost, it does absolutely nothing at runtime. it is purely semantic.
    ///
    /// # Safety
    ///
    /// this function must be called from an rcu tracked thread (see [`is_rcu_tracked_thread`]).
    ///
    /// additionally, this guard must not be held across await points, must not be held after the future that acquired it finishes, and must not
    /// escape that future's context (e.g. must not be saved inside a global variable and held across an await point or longer than the future's
    /// lifetime).
    ///
    /// as soon as the future that acquired the guard gets to a point where it `await`s or finishes execution (basically any point which voluntarily
    /// yields the future), the guard must have already been dropped.
    #[inline(always)]
    pub unsafe fn new() -> Self {
        Self {
            phantom: PhantomUnsendUnsync::new(),
        }
    }
}

/// acquires an rcu read lock guard, and provides access to it within the provided callback.
///
/// the usage of the guard is limited to the provided callback to prevent it from being used across await points, and to prevent it
/// from escaping the calling function. this is needed to guarantee correct use of the rcu read lock guard.
///
/// this function specifically passes a reference to the guard to the callback, and specifically one with an unknown lifetime, to ensure that the guard
/// can't escape the callback.
///
/// # Panics
///
/// this function must only be called from an rcu tracked thread, otherwise it will panic (see [`is_rcu_tracked_thread`]).
///
/// # Performance
///
/// this function is very fast and cheap. other than calling the callback, it only checks that the calling thread is an rcu tracked thread, by calling
/// [`is_rcu_tracked_thread`].
///
/// if your code is EXTREMELY performance sensitive, consider using [`rcu_read_lock_unchecked`], which is faster due to skipping the rcu tracked thread
/// check, at the cost of being unsafe.
#[inline(always)]
pub fn rcu_read_lock<F, R>(f: F) -> R
where
    F: FnOnce(&RcuReadLockGuard) -> R,
{
    assert!(
        is_rcu_tracked_thread(),
        "attempted to acquire an rcu read lock guard inside of a non rcu tracked thread"
    );

    // SAFETY: we checked that the current thread is an rcu tracked thread
    unsafe { rcu_read_lock_unchecked(f) }
}

/// acquires an rcu read lock guard, and provides access to it within the provided callback.
///
/// this function is the unsafe variant of acquiring an rcu read lock. for a safe alternative, see [`rcu_read_lock`].
/// but, note that this function slightly faster than [`rcu_read_lock`], at the cost of being unsafe.
///
/// the usage of the guard is limited to the provided callback to prevent it from being used across await points, and to prevent it
/// from escaping the calling function. this is needed to guarantee correct use of the rcu read lock guard.
///
/// this function specifically passes a reference to the guard to the callback, and specifically one with an unknown lifetime, to ensure that the guard
/// can't escape the callback.
///
/// # Safety
///
/// this function must be called from an rcu tracked thread (see [`is_rcu_tracked_thread`]).
///
/// # Performance
///
/// this function is completely zero cost, it does absolutely nothing at runtime other than calling the callback. it is purely semantic.
#[inline(always)]
pub unsafe fn rcu_read_lock_unchecked<F, R>(f: F) -> R
where
    F: FnOnce(&RcuReadLockGuard) -> R,
{
    // SAFETY:
    // - caller guarantees that we are in an rcu tracked thread
    // - the guard only lives throughout the current function, so the calling future can't yield while holding it.
    // - the guard can't escape since the callback function F is an HRTB, so it can't assume anything about the lifetime of
    //   the provided reference.
    let guard = unsafe { RcuReadLockGuard::new() };
    f(&guard)
}
