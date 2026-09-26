#[cfg(loom)]
pub use loom::*;

#[cfg(not(loom))]
pub use std::*;

pub struct UnsafeCell<T> {
    #[cfg(not(loom))]
    inner: cell::UnsafeCell<T>,
    #[cfg(loom)]
    inner: cell::UnsafeCell<T>,
}
impl<T> UnsafeCell<T> {
    fn_const_if_not_loom! {
        pub fn new(value: T) -> Self {
            Self(cell::UnsafeCell::new(value))
        }
    }

    pub fn get(&self) -> CellDataMutPtr<T> {
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

    pub fn raw_get(this: *const UnsafeCell<T>) -> *mut T {
        #[cfg(not(loom))]
        {
            UnsafeCell::raw_get(this)
        }

        #[cfg(loom)]
        {
            UnsafeCell
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
            $(#[$attr:meta])* $vis:vis fn $name:ident($($args: tt)*) -> $ret:ty $body:block
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
