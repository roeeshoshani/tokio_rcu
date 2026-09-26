#[cfg(loom)]
pub use loom::*;

#[cfg(not(loom))]
pub use std::*;

pub struct UnsafeCell<T>(cell::UnsafeCell<T>);
impl<T> UnsafeCell<T> {
    fn_const_if_not_loom! {
        pub const fn new(value: T) -> Self {
            Self(cell::UnsafeCell::new(value))
        }
    }

    pub unsafe fn get(&self) -> CellDataMutPtr<T> {
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

/// a loom/std abstraction over a non-null pointer to the data contained inside a cell.
pub struct CellDataNonNullPtr<T> {
    #[cfg(not(loom))]
    raw: std::ptr::NonNull<T>,
    #[cfg(loom)]
    raw: loom::cell::MutPtr<T>,
}
impl<T> CellDataNonNullPtr<T> {
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

/// a loom/std abstraction over a mutable pointer to the data contained inside a cell.
pub struct CellDataMutPtr<T> {
    #[cfg(not(loom))]
    raw: *mut T,
    #[cfg(loom)]
    raw: loom::cell::MutPtr<T>,
}
impl<T> CellDataMutPtr<T> {
    pub unsafe fn to_non_null_unchecked(self) -> CellDataNonNullPtr<T> {
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
    pub unsafe fn to_mut_ref(&self) -> &mut T {
        #[cfg(not(loom))]
        unsafe {
            &mut *self.raw
        }
        #[cfg(loom)]
        unsafe {
            self.raw.deref()
        }
    }

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
impl<T: Copy> CellDataMutPtr<T> {
    pub unsafe fn read(&self) -> T {
        #[cfg(not(loom))]
        unsafe {
            self.raw.read()
        }
        #[cfg(loom)]
        unsafe {
            self.raw.with(|ptr| ptr.read())
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
            loom::lazy_static! {
                $(#[$attr])*
                static ref $name: $ty = $value;
            }
        )+
    };
}
pub(crate) use static_or_loom_lazy_static;
