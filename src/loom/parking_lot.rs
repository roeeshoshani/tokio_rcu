use crate::loom::fn_const_if_not_loom;

#[cfg(not(loom))]
type InnerMutex<T> = parking_lot::Mutex<T>;
#[cfg(loom)]
type InnerMutex<T> = loom::sync::Mutex<T>;

/// a loom/std abstraction over parking lot's [`Mutex`], providing a unified API specifically tied to the use of [`Mutex`] in this crate.
///
/// note that like parking lot's mutex, this lock does not support poisoning, it releases normally on panic.
///
/// [`Mutex`]: parking_lot::Mutex
pub struct Mutex<T>(InnerMutex<T>);
impl<T> Mutex<T> {
    fn_const_if_not_loom! {
        /// creates a new mutex with the given initial value.
        #[inline(always)]
        pub const fn new(value: T) -> Self {
            Self(InnerMutex::new(value))
        }
    }

    pub fn lock(&self) -> InnerMutexGuard<'_, T> {
        #[cfg(not(loom))]
        {
            self.0.lock()
        }

        #[cfg(loom)]
        {
            // ignore poisoning to emulate parking lot's mutex behaviour
            match self.0.lock() {
                Ok(x) => x,
                Err(x) => x.into_inner(),
            }
        }
    }
}

#[cfg(not(loom))]
type InnerMutexGuard<'a, T> = parking_lot::MutexGuard<'a, T>;
#[cfg(loom)]
type InnerMutexGuard<'a, T> = loom::sync::MutexGuard<'a, T>;

/// a loom/std abstraction over parking lot's [`MutexGuard`](parking_lot::MutexGuard).
pub struct MutexGuard<'a, T> {
    _inner: InnerMutexGuard<'a, T>,
}

#[cfg(not(loom))]
type InnerRwLock<T> = parking_lot::RwLock<T>;
#[cfg(loom)]
type InnerRwLock<T> = loom::sync::RwLock<T>;

/// a loom/std abstraction over parking lot's [`RwLock`], providing a unified API specifically tied to the use of [`RwLock`] in this crate.
///
/// note that the loom version of this rwlock is not fair, unlike the parking lot version. that's unfortunate, but loom doesn't provide a fair rwlock
/// primitive, and implementing one it just too much work for now.
///
/// [`RwLock`]: parking_lot::RwLock
pub struct RwLock<T>(InnerRwLock<T>);
impl<T> RwLock<T> {
    fn_const_if_not_loom! {
        /// creates a new rwlock with the given initial value.
        #[inline(always)]
        pub const fn new(value: T) -> Self {
            Self(InnerRwLock::new(value))
        }
    }

    /// locks this rwlock for reading.
    #[inline(always)]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        #[cfg(not(loom))]
        {
            RwLockReadGuard {
                _inner: self.0.read(),
            }
        }

        #[cfg(loom)]
        {
            RwLockReadGuard {
                _inner: self.0.read().unwrap(),
            }
        }
    }

    /// locks this rwlock for writing.
    #[inline(always)]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        #[cfg(not(loom))]
        {
            RwLockWriteGuard {
                _inner: self.0.write(),
            }
        }

        #[cfg(loom)]
        {
            RwLockWriteGuard {
                _inner: self.0.write().unwrap(),
            }
        }
    }
}

#[cfg(not(loom))]
type InnerRwLockReadGuard<'a, T> = parking_lot::RwLockReadGuard<'a, T>;
#[cfg(loom)]
type InnerRwLockReadGuard<'a, T> = loom::sync::RwLockReadGuard<'a, T>;

/// a loom/std abstraction over parking lot's [`RwLockReadGuard`](parking_lot::RwLockReadGuard).
pub struct RwLockReadGuard<'a, T> {
    _inner: InnerRwLockReadGuard<'a, T>,
}

#[cfg(not(loom))]
type InnerRwLockWriteGuard<'a, T> = parking_lot::RwLockWriteGuard<'a, T>;
#[cfg(loom)]
type InnerRwLockWriteGuard<'a, T> = loom::sync::RwLockWriteGuard<'a, T>;

/// a loom/std abstraction over parking lot's [`RwLockWriteGuard`](parking_lot::RwLockWriteGuard).
pub struct RwLockWriteGuard<'a, T> {
    _inner: InnerRwLockWriteGuard<'a, T>,
}
