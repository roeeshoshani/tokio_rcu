#[cfg(loom)]
pub use loom as std;

#[cfg(not(loom))]
pub use std;

pub mod parking_lot;

/// a loom/std abstraction over [`UnsafeCell`], providing a unified API specifically tied to the use of [`UnsafeCell`] in this crate.
///
/// [`UnsafeCell`]: std::cell::UnsafeCell
pub struct UnsafeCell<T>(std::cell::UnsafeCell<T>);
impl<T> UnsafeCell<T> {
    fn_const_if_not_loom! {
        /// creates a new unsafe cell containing the given value.
        #[inline(always)]
        pub const fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }
    }

    /// returns a const pointer to the wrapped value.
    #[inline(always)]
    pub fn get_const_ptr(&self) -> CellDataConstPtr<T> {
        #[cfg(not(loom))]
        {
            CellDataConstPtr { raw: self.0.get() }
        }

        #[cfg(loom)]
        {
            CellDataConstPtr { raw: self.0.get() }
        }
    }

    /// returns a mutable pointer to the wrapped value.
    #[inline(always)]
    pub fn get_mut_ptr(&self) -> CellDataMutPtr<T> {
        #[cfg(not(loom))]
        {
            CellDataMutPtr { raw: self.0.get() }
        }

        #[cfg(loom)]
        {
            CellDataMutPtr {
                raw: self.0.get_mut(),
            }
        }
    }
}

/// a loom/std abstraction over a non-null pointer ([`NonNull<T>`](std::ptr::NonNull)) to the data contained inside a cell.
///
/// for the std case, this actually stores a [`NonNull`](std::ptr::NonNull) to provide niche optimizations.
///
/// for the loom case, there's no need to optimize anything, so this has the same layout as a regular cell data pointer.
///
/// so, this is only used as an optimization for the std case.
pub struct CellDataNonNullPtr<T> {
    #[cfg(not(loom))]
    raw: std::ptr::NonNull<T>,
    #[cfg(loom)]
    raw: loom::cell::MutPtr<T>,
}
impl<T> CellDataNonNullPtr<T> {
    /// writes the given value to the pointer.
    ///
    /// # Safety
    ///
    /// same safety requirements as [`std::ptr::write`].
    #[inline(always)]
    pub unsafe fn write(&self, value: T) {
        #[cfg(not(loom))]
        unsafe {
            *self.raw.as_ptr() = value
        }
        #[cfg(loom)]
        unsafe {
            self.raw.with(|ptr| *ptr = value)
        }
    }
}

/// a loom/std abstraction over a mutable pointer (`*mut T`) to the data contained inside a cell.
pub struct CellDataMutPtr<T> {
    #[cfg(not(loom))]
    raw: *mut T,
    #[cfg(loom)]
    raw: loom::cell::MutPtr<T>,
}
impl<T> CellDataMutPtr<T> {
    /// converts this pointer to a non-null pointer.
    ///
    /// # Safety
    ///
    /// the pointer must be non-null.
    #[inline(always)]
    pub unsafe fn into_non_null_unchecked(self) -> CellDataNonNullPtr<T> {
        #[cfg(not(loom))]
        unsafe {
            CellDataNonNullPtr {
                raw: std::ptr::NonNull::new_unchecked(self.raw),
            }
        }
        #[cfg(loom)]
        {
            CellDataNonNullPtr { raw: self.raw }
        }
    }

    /// converts the pointer to a mutable reference.
    ///
    /// # Safety
    ///
    /// pointer must be valid and must be allowed to be converted to a mut ref according to the regular aliasing rules.
    ///
    /// this basically has the same safety requirements as doing `&mut *ptr` on this pointer, if it were a regular pointer.
    #[inline(always)]
    pub unsafe fn as_mut_ref(&mut self) -> &mut T {
        #[cfg(not(loom))]
        unsafe {
            &mut *self.raw
        }
        #[cfg(loom)]
        unsafe {
            self.raw.deref()
        }
    }

    /// writes the given value to the pointer.
    ///
    /// # Safety
    ///
    /// same safety requirements as [`std::ptr::write`].
    #[inline(always)]
    pub unsafe fn write(&self, value: T) {
        #[cfg(not(loom))]
        unsafe {
            *self.raw = value
        }
        #[cfg(loom)]
        unsafe {
            self.raw.with(|ptr| *ptr = value)
        }
    }

    /// replaces the value at `self` with `src`, returning the old value, without dropping either.
    ///
    /// # Safety
    ///
    /// same safety requirements as [`std::ptr::replace`].
    #[inline(always)]
    pub unsafe fn replace(&self, value: T) -> T {
        #[cfg(not(loom))]
        unsafe {
            self.raw.replace(value)
        }
        #[cfg(loom)]
        unsafe {
            self.raw.with(|ptr| ptr.replace(value))
        }
    }
}

/// a loom/std abstraction over a const pointer (`*const T`) to the data contained inside a cell.
pub struct CellDataConstPtr<T> {
    #[cfg(not(loom))]
    raw: *const T,
    #[cfg(loom)]
    raw: loom::cell::ConstPtr<T>,
}
impl<T> CellDataConstPtr<T> {
    /// converts the pointer to an immutable reference.
    ///
    /// # Safety
    ///
    /// pointer must be valid and must be allowed to be converted to an immutable ref according to the regular aliasing rules.
    ///
    /// this basically has the same safety requirements as doing `&*ptr` on this pointer, if it were a regular pointer.
    #[inline(always)]
    pub unsafe fn as_ref(&self) -> &T {
        #[cfg(not(loom))]
        unsafe {
            &*self.raw
        }
        #[cfg(loom)]
        unsafe {
            self.raw.deref()
        }
    }
}
impl<T: Copy> CellDataConstPtr<T> {
    /// reads the data pointed at by this pointer and copies its contents.
    ///
    /// # Safety
    ///
    /// pointer must be valid for reading.
    ///
    /// this basically has the same safety requirements as doing `*ptr` on this pointer, if it were a regular pointer.
    #[inline(always)]
    pub unsafe fn read(&self) -> T {
        #[cfg(not(loom))]
        unsafe {
            *self.raw
        }
        #[cfg(loom)]
        unsafe {
            self.raw.with(|ptr| *ptr)
        }
    }
}
/// makes the given function constant only when not running under loom.
///
/// this is needed since several types in this crate have a `const` constructor which is required for the statics which hold them,
/// but loom's synchronization primitives cannot be constructed in a const context, so their constructors must not be `const` when
/// running under loom.
///
/// the provided body must be valid in both cases, which it usually is, since the only difference is the const-ness of the
/// functions it calls.
macro_rules! fn_const_if_not_loom {
    (
        $(
            $(#[$attr:meta])* $vis:vis const fn $name:ident($($args: tt)*) -> $ret:ty $body:block
        )+
    ) => {
        $(
            $(#[$attr])*
            #[cfg(not(loom))]
            $vis const fn $name($($args)*) -> $ret $body

            $(#[$attr])*
            #[cfg(loom)]
            $vis fn $name($($args)*) -> $ret $body
        )+
    };
}
pub(crate) use fn_const_if_not_loom;

/// makes the given static a static in `cfg(not(loom))`, and makes it a loom lazy static in `cfg(loom)`.
///
/// this is needed to allow loom to properly model the static variables used in this crate, while not hurting performance in the non-loom case
/// by forcing them to be lazy statics when not needed.
macro_rules! static_or_loom_lazy_static {
    (
        $(
            $(#[$attr:meta])* static $name:ident : $ty:ty = $value:expr;
        )+
    ) => {
        $(
            $(#[$attr])*
            #[cfg(not(loom))]
            static $name: $ty = $value;

            #[cfg(loom)]
            ::loom::lazy_static! {
                $(#[$attr])*
                static ref $name: $ty = $value;
            }
        )+
    };
}
pub(crate) use static_or_loom_lazy_static;
