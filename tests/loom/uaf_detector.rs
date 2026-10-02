//! this module implements the [`UafDetector`] type, which lets you detect use-after-free scenarios safely.
//!
//! it works by defining a custom global allocator, which adds an extra piece of data per allocation to track whether it is a
//! uaf detector allocation or not. when a uaf detector is deallocated (e.g. by dropping a `Box<UafDetector>`), the allocator
//! does not really free the memory, and instead puts it in a reclamation list. a uaf detector allocation is never actually
//! freed, so accessing a [`UafDetector`] allocation after it has been de-allocated doesn't actually cause any UB in safe rust.
//!
//! # tags
//!
//! when a uaf detector allocation is deallocated, future uaf detector allocations may re-use that allocation, but when
//! re-using, they will increment the allocation's tag, which is used to track the different versions of the same allocation.
//!
//! when a uaf detector is allocated (using [`UafDetector::new`]), you get back a uaf detector allocation, and its corresponding
//! tag. when you later access this allocation, you must provide the tag that you saw when allocating it. the tag is then
//! compared to the current tag of the allocation to check if it has been re-allocated (the tag is incremented when the slot
//! is re-allocated).

use std::{
    alloc::{GlobalAlloc, Layout, LayoutError},
    cell::RefCell,
};

/// the type used to store the allocation prefix.
type AllocPrefix = usize;

/// the atomic version of the [`AllocPrefix`] type.
type AllocPrefixAtomic = std::sync::atomic::AtomicUsize;

/// a bit in the allocation prefix which marks an allocation as a tracked uaf detector allocation.
const ALLOC_PREFIX_UAF_DETECTOR_ALLOC_BIT: AllocPrefix = 1 << (size_of::<AllocPrefix>() * 8 - 1);

/// a key used to detect when a UAF detector is deallocated.
///
/// a key is always associated with a specific allocation. it can then be used to detect if that allocation was freed, and
/// can even detect if it is being re-used.
#[derive(Debug, Clone, Copy)]
pub struct UafDetectorKey(AllocPrefix);

/// the global pool of uaf detector allocations.
struct GlobalPool {
    /// allocations that can be reallocated.
    /// when a thread dies, its reallocation pool gets collected into this global reallocation pool.
    /// other threads can then take allocations from this pool when their reallocation pool is empty, instead of allocating new
    /// memory from the system allocator.
    realloc: Vec<Box<UafDetector>>,

    /// allocations that can no longer be reallocated due to reaching the max tag value, and are now leaked to prevent a real
    /// use after free on these slots.
    leaked: Vec<Box<UafDetector>>,
}

/// a thread-local reallocation pool.
struct ThreadReallocPool {
    slots: Vec<Box<UafDetector>>,
}
impl ThreadReallocPool {
    pub const fn new() -> Self {
        Self { slots: Vec::new() }
    }
}
impl Drop for ThreadReallocPool {
    fn drop(&mut self) {
        // push all slots into the global realloc pool
        if !self.slots.is_empty() {
            {
                let mut uaf_detector_allocs = GLOBAL_ALLOCATOR.global_pool.lock();
                uaf_detector_allocs.realloc.append(&mut self.slots);
                GLOBAL_ALLOCATOR.does_realloc_pool_contain_items.store(
                    true,
                    // use release ordering to make sure the reader sees the items before seeing the flag.
                    std::sync::atomic::Ordering::Release,
                );
            }
        }
    }
}

thread_local! {
    /// the per-thread local realloc pool.
    ///
    /// when a uaf detector is deallocated, it is put in the realloc pool of the thread of the thread on which the deallocation
    /// was performed.
    ///
    /// then, when handling a new uaf detector allocation request, we re-use the allocations in this pool.
    ///
    /// this serves the purpose of keeping the uaf detector allocation alive at all times, so that accessing it after it was
    /// freed does not cause actual language level UAF. furthermore, it allows the memory to be re-used instead of just leaked
    /// forever, to avoid wasting huge amounts of memory.
    ///
    /// when the thread dies, the allocations in this thread local realloc pool move to the global realloc pool.
    static REALLOC_POOL: RefCell<ThreadReallocPool> = RefCell::new(ThreadReallocPool::new());
}

/// a global allocator which provides the necessary support for the [`UafDetector`] object to work properly without causing UB
/// when accessed after freed.
struct UafDetectorSupportingAllocator {
    /// a global pool of allocations for different purposes.
    /// this can't use std's mutex as it may allocate.
    global_pool: parking_lot::Mutex<GlobalPool>,

    /// a hint on whether the global realloc pool contains any items. this is used only as an optimization, to avoid taking
    /// the lock in the fast path where the global realloc pool is empty.
    ///
    /// when set to `false`, the global realloc pool is guaranteed to be empty at the point of sampling.
    /// when set to `true`, the global realloc pool is probably not empty, but may still be empty in some cases.
    ///
    /// must only be written too while holding the global pool mutex, but can be read without holding the mutex.
    does_realloc_pool_contain_items: std::sync::atomic::AtomicBool,
}
impl UafDetectorSupportingAllocator {
    /// the data-offset of the uaf detector inside a tagged uaf detector allocation.
    const UAF_DETECTOR_ALLOC_DATA_OFF: usize = {
        let Ok((_alloc_layout, data_off)) = Self::calc_alloc_layout(Layout::new::<UafDetector>())
        else {
            panic!();
        };
        data_off
    };

    const fn new() -> Self {
        Self {
            global_pool: parking_lot::Mutex::new(GlobalPool {
                realloc: Vec::new(),
                leaked: Vec::new(),
            }),
            does_realloc_pool_contain_items: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn alloc_uaf_detector(&self, id: usize) -> (Box<UafDetector>, UafDetectorKey) {
        let realloc_slot = REALLOC_POOL.with(|realloc_pool| {
            let mut realloc_pool = realloc_pool.borrow_mut();
            match realloc_pool.slots.pop() {
                Some(slot) => Some(slot),
                None => {
                    // try grabbing from the global re-alloc pool
                    if self.does_realloc_pool_contain_items.load(
                        // no ordering here, we use a fence instead, only when needed
                        std::sync::atomic::Ordering::Relaxed,
                    ) {
                        // in this case, use an acquire fence to make sure that we see the items in the pool once we see
                        // the `does_realloc_pool_contain_items` flag.
                        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);

                        // the global realloc pool MAY have some items, grab them
                        let mut uaf_detector_allocs = self.global_pool.lock();
                        if !uaf_detector_allocs.realloc.is_empty() {
                            // the global realloc pool indeed has some slots, use one for this allocation, and move
                            // the rest to the local pool
                            let reused_slot = uaf_detector_allocs.realloc.pop();
                            realloc_pool.slots.append(&mut uaf_detector_allocs.realloc);

                            // the global re-alloc pool is now empty
                            self.does_realloc_pool_contain_items.store(
                                false,
                                // no ordering is needed here, ordering is only needed when storing `true`, to make sure
                                // the reader sees the items before seeing the flag.
                                std::sync::atomic::Ordering::Relaxed,
                            );

                            reused_slot
                        } else {
                            // the global pool is empty
                            None
                        }
                    } else {
                        // nothing in the global re-alloc pool as well
                        None
                    }
                }
            }
        });

        match realloc_slot {
            Some(mut existing_allocation) => {
                *existing_allocation = UafDetector::new_noalloc(id);
                let prefix_ref = Self::uaf_detector_get_prefix_ref(&existing_allocation);
                let prefix = prefix_ref.load(
                    // ordering doesn't matter, this is only used to check for UAF, and is not used to synchronize with any
                    // other data.
                    std::sync::atomic::Ordering::Relaxed,
                );
                (existing_allocation, UafDetectorKey(prefix))
            }
            None => self.alloc_uaf_detector_no_realloc(id),
        }
    }

    /// allocate a new uaf detector from the system allocated. used in the case where nothing can be reallocated.
    fn alloc_uaf_detector_no_realloc(&self, id: usize) -> (Box<UafDetector>, UafDetectorKey) {
        let uaf_detector = Box::new(UafDetector::new_noalloc(id));

        let prefix_ref = Self::uaf_detector_get_prefix_ref(&uaf_detector);
        prefix_ref.store(
            ALLOC_PREFIX_UAF_DETECTOR_ALLOC_BIT,
            // ordering doesn't matter, this is only used to check for UAF, and is not used to synchronize with any
            // other data.
            std::sync::atomic::Ordering::Relaxed,
        );

        (
            uaf_detector,
            UafDetectorKey(ALLOC_PREFIX_UAF_DETECTOR_ALLOC_BIT),
        )
    }

    /// calculate the tagged allocation layout of the given layout.
    ///
    /// returns the tagged allocation layout, and the offset of the original layout inside the tagged layout.
    const fn calc_alloc_layout(layout: Layout) -> Result<(Layout, usize), LayoutError> {
        match std::alloc::Layout::new::<AllocPrefixAtomic>().extend(layout) {
            Ok((combined_layout, data_off)) => Ok((combined_layout, data_off)),
            Err(err) => Err(err),
        }
    }

    /// returns the a pointer to the prefix (AKAK tag) of the given uaf detector allocation.
    fn uaf_detector_get_prefix_ref(uaf_detector: &Box<UafDetector>) -> &AllocPrefixAtomic {
        // SAFETY: the uaf detector is boxed, so it is heap allocated
        unsafe { Self::uaf_detector_get_prefix_ref_byref(&uaf_detector) }
    }

    /// returns the a pointer to the prefix (AKAK tag) of the given uaf detector allocation, by reference.
    ///
    /// this is an unsafe version which takes a reference as argument.
    /// see [`uaf_detector_get_prefix_ref`](Self::uaf_detector_get_prefix_ref) a safe alternative.
    ///
    /// # Safety
    ///
    /// the provided uaf detector reference must be a reference into a heap allocation allocated using this allocator.
    unsafe fn uaf_detector_get_prefix_ref_byref(uaf_detector: &UafDetector) -> &AllocPrefixAtomic {
        // SAFETY: the provided uaf detector is heap allocated, so it must have a valid prefix field before it.
        // furthermore, we return an immutable reference, so we don't need to worry about aliasing.
        unsafe {
            &*(uaf_detector as *const UafDetector)
                .byte_sub(Self::UAF_DETECTOR_ALLOC_DATA_OFF)
                .cast::<AllocPrefixAtomic>()
        }
    }

    fn dealloc_uaf_detector(
        &self,
        ptr: *mut u8,
        prefix_ref: &AllocPrefixAtomic,
        prefix: AllocPrefix,
    ) {
        // SAFETY: ptr is a heap allocation marked as a uaf detector allocation, so it is safe to reconstruct it into a box.
        let reconstructed_box = unsafe { Box::from_raw(ptr.cast::<UafDetector>()) };

        if prefix == AllocPrefix::MAX {
            // can't re-alloc this slot anymore, it will lead to re-use of keys, so leak it forever.
            let mut uaf_detector_allocs = self.global_pool.lock();
            uaf_detector_allocs.leaked.push(reconstructed_box);
        } else {
            // increment the prefix, so that this slot can see that it was freed.
            prefix_ref.store(
                prefix + 1,
                // ordering doesn't matter, this is only used to check for UAF, and is not used to synchronize with any
                // other data.
                std::sync::atomic::Ordering::Relaxed,
            );

            // can re-alloc this slot
            REALLOC_POOL
                .with(|realloc_poll| realloc_poll.borrow_mut().slots.push(reconstructed_box))
        }
    }
}
unsafe impl GlobalAlloc for UafDetectorSupportingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let (alloc_layout, data_off) = Self::calc_alloc_layout(layout).unwrap();

        // SAFETY: the layout is not zero sized since it at least contains the allocation prefix
        let ptr = unsafe { std::alloc::System.alloc(alloc_layout) };

        // SAFETY: the calculated alloc layout makes the returned pointer a valid pointer to a [`AllocPrefixAtomic`].
        // furthermore, we are currently the only one with access to this allocation, so we can initialize the atomic
        // directly and don't need an atomic write.
        unsafe {
            ptr.cast::<AllocPrefixAtomic>()
                .write(AllocPrefixAtomic::new(0))
        };

        // SAFETY: we move the pointer within the allocation
        unsafe { ptr.byte_add(data_off) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        let (alloc_layout, data_off) = Self::calc_alloc_layout(layout).unwrap();

        // SAFETY: every pointer we allocate has a prefix, and moving back to that preifx is just moving within the allocation,
        // so it should be safe.
        let alloc_ptr = unsafe { ptr.byte_sub(data_off) };

        // SAFETY: when allocating, we allocate using the calculated alloc layout, so the raw allocation pointer must be a valid
        // pointer to a prefix value.
        let prefix_ref = unsafe { &*alloc_ptr.cast::<AllocPrefixAtomic>() };

        let prefix = prefix_ref.load(
            // ordering doesn't matter, this is only used to check for UAF, and is not used to synchronize with any
            // other data.
            std::sync::atomic::Ordering::Relaxed,
        );
        if (prefix & ALLOC_PREFIX_UAF_DETECTOR_ALLOC_BIT) != 0 {
            // this is uaf detector allocation
            debug_assert_eq!(layout, Layout::new::<UafDetector>());
            self.dealloc_uaf_detector(ptr, prefix_ref, prefix);
        } else {
            // regular non uaf-detector allocation
            //
            // SAFETY: caller must guarantee validity of ptr and layout.
            unsafe { std::alloc::System.dealloc(alloc_ptr, alloc_layout) }
        }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: UafDetectorSupportingAllocator = UafDetectorSupportingAllocator::new();

/// a type used to detect use after free scenarios.
pub struct UafDetector {
    id: usize,
}
impl UafDetector {
    /// creates a new uaf detector value without allocating it on the heap.
    fn new_noalloc(id: usize) -> UafDetector {
        Self { id }
    }

    /// creates a new UAF detector with the given id.
    pub fn new(id: usize) -> (Box<UafDetector>, UafDetectorKey) {
        GLOBAL_ALLOCATOR.alloc_uaf_detector(id)
    }

    /// returns the id of this UAF detector, or `None` if this UAF detector has already been freed.
    ///
    /// this is an unsafe version which takes a reference as argument. see [`try_id`](Self::try_id) for a safe alternative.
    ///
    /// # Safety
    ///
    /// `&self` must be a reference pointing to a heap allocated [`UafDetector`] (e.g. a `Box<UafDetector>`).
    pub unsafe fn try_id_byref(&self, key: UafDetectorKey) -> Option<usize> {
        // SAFETY: self is guaranteed to be heap allocated, since the only way to get a `UafDetector` outside of this module is
        // using the
        let prefix_ref =
            unsafe { UafDetectorSupportingAllocator::uaf_detector_get_prefix_ref_byref(self) };

        let prefix = prefix_ref.load(
            // ordering doesn't matter, this is only used to check for UAF, and is not used to synchronize with any
            // other data.
            std::sync::atomic::Ordering::Relaxed,
        );

        if prefix == key.0 {
            Some(self.id)
        } else if prefix < key.0 {
            // the key represents an old snapshot of the prefix and the prefix only grows.
            // if the prefix is less than the key, a wrong key is used.
            panic!("UAF detector key mismatch");
        } else {
            // in this situation, this UAF detector has already been freed since allocated, so this is a UAF situation.
            None
        }
    }

    /// returns the id of this UAF detector, or `None` if this UAF detector has already been freed.
    #[allow(unused)]
    pub fn try_id(self: &Box<UafDetector>, key: UafDetectorKey) -> Option<usize> {
        // SAFETY: `self` is a heap allocation
        unsafe { self.try_id_byref(key) }
    }

    /// returns the id of this UAF detector.
    /// if this UAF detector has already been freed, this function safely detects the UAF and panic with a corresponding error
    /// message.
    ///
    /// this is an unsafe version which takes a reference as argument. see [`id`](Self::id) for a safe alternative.
    ///
    /// # Safety
    ///
    /// `&self` must be a reference pointing to a heap allocated [`UafDetector`] (e.g. a `Box<UafDetector>`).
    pub unsafe fn id_byref(&self, key: UafDetectorKey) -> usize {
        let id_opt = unsafe { self.try_id_byref(key) };
        id_opt.expect("detected use after free")
    }

    /// returns the id of this UAF detector.
    /// if this UAF detector has already been freed, this function safely detects the UAF and panic with a corresponding error
    /// message.
    pub fn id(self: &Box<UafDetector>, key: UafDetectorKey) -> usize {
        // SAFETY: `self` is a heap allocation
        unsafe { self.id_byref(key) }
    }
}
