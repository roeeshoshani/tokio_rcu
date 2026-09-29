use std::{
    alloc::{GlobalAlloc, Layout, LayoutError},
    cell::RefCell,
};

/// a key used to detect when a UAF detector is deallocated.
///
/// a key is always associated with a specific allocation. it can then be used to detect if that allocation was freed, and
/// can even detect if it is being re-used.
#[derive(Debug, Clone, Copy)]
pub struct UafDetectorKey(u64);

struct UafDetectorAllocs {
    realloc: Vec<Box<UafDetector>>,
    leaked: Vec<Box<UafDetector>>,
}

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
                let mut uaf_detector_allocs = GLOBAL_ALLOCATOR.uaf_detector_allocs.lock();
                uaf_detector_allocs.realloc.append(&mut self.slots);
            }
            GLOBAL_ALLOCATOR
                .does_realloc_pool_contain_items
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

thread_local! {
    static REALLOC_POOL: RefCell<ThreadReallocPool> = RefCell::new(ThreadReallocPool::new());
}

struct UafDetectorSupportingAllocator {
    // can't use std::mutex as it may allocate.
    uaf_detector_allocs: parking_lot::Mutex<UafDetectorAllocs>,
    does_realloc_pool_contain_items: std::sync::atomic::AtomicBool,
}
unsafe impl Sync for UafDetectorSupportingAllocator {}
impl UafDetectorSupportingAllocator {
    const UAF_DETECTOR_ALLOC_DATA_OFF: usize = {
        let Ok((_alloc_layout, data_off)) = Self::calc_alloc_layout(Layout::new::<UafDetector>())
        else {
            panic!();
        };
        data_off
    };

    const fn new() -> Self {
        Self {
            uaf_detector_allocs: parking_lot::Mutex::new(UafDetectorAllocs {
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
                    if self
                        .does_realloc_pool_contain_items
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        // the global realloc pool MAY have some items, grab them
                        let mut uaf_detector_allocs = self.uaf_detector_allocs.lock();
                        if !uaf_detector_allocs.realloc.is_empty() {
                            // the global realloc pool indeed has some slots, use one for this allocation, and move
                            // the rest to the local pool
                            let reused_slot = uaf_detector_allocs.realloc.pop();
                            realloc_pool.slots.append(&mut uaf_detector_allocs.realloc);

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

                let prefix_ptr =
                    unsafe { Self::uaf_detector_get_prefix_ptr(&mut existing_allocation) };
                let old_prefix = unsafe { prefix_ptr.read() };
                debug_assert!(old_prefix & (1 << 63) != 0);
                debug_assert!(old_prefix != u64::MAX);

                let new_prefix = old_prefix + 1;

                unsafe { prefix_ptr.write(new_prefix) };

                (existing_allocation, UafDetectorKey(new_prefix))
            }
            None => self.alloc_uaf_detector_no_realloc(id),
        }
    }

    fn alloc_uaf_detector_no_realloc(&self, id: usize) -> (Box<UafDetector>, UafDetectorKey) {
        let mut uaf_detector = Box::new(UafDetector::new_noalloc(id));
        unsafe { Self::uaf_detector_get_prefix_ptr(&mut uaf_detector).write(1 << 63) };
        (uaf_detector, UafDetectorKey(1 << 63))
    }

    const fn calc_alloc_layout(layout: Layout) -> Result<(Layout, usize), LayoutError> {
        match std::alloc::Layout::new::<u64>().extend(layout) {
            Ok((combined_layout, data_off)) => Ok((combined_layout, data_off)),
            Err(err) => Err(err),
        }
    }

    unsafe fn uaf_detector_get_prefix_ptr(uaf_detector: &mut Box<UafDetector>) -> *mut u64 {
        unsafe {
            Box::as_mut_ptr(uaf_detector)
                .byte_sub(Self::UAF_DETECTOR_ALLOC_DATA_OFF)
                .cast::<u64>()
        }
    }

    fn dealloc_uaf_detector(&self, ptr: *mut u8, prefix: u64) {
        let reconstructed_box = unsafe { Box::from_raw(ptr.cast::<UafDetector>()) };
        if prefix == u64::MAX {
            // can't re-alloc this slot anymore, it will lead to re-use of keys, so leak it forever.
            let mut uaf_detector_allocs = self.uaf_detector_allocs.lock();
            uaf_detector_allocs.leaked.push(reconstructed_box);
        } else {
            // can re-alloc this slot
            REALLOC_POOL
                .with(|realloc_poll| realloc_poll.borrow_mut().slots.push(reconstructed_box))
        }
    }
}
unsafe impl GlobalAlloc for UafDetectorSupportingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let (alloc_layout, data_off) = Self::calc_alloc_layout(layout).unwrap();
        let ptr = unsafe { std::alloc::System.alloc(alloc_layout) };
        unsafe { ptr.cast::<u64>().write(0) };
        unsafe { ptr.add(data_off) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        let (alloc_layout, data_off) = Self::calc_alloc_layout(layout).unwrap();
        let alloc_ptr = unsafe { ptr.byte_sub(data_off) };
        let prefix = unsafe { alloc_ptr.cast::<u64>().read() };
        if (prefix & (1u64 << 63)) != 0 {
            // this is uaf detector allocation
            debug_assert_eq!(layout, Layout::new::<UafDetector>());
            self.dealloc_uaf_detector(ptr, prefix);
        } else {
            // regular non uaf-detector allocation
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
    pub fn try_id(&self, key: UafDetectorKey) -> Option<usize> {
        let ptr = self as *const UafDetector;
        let prefix: u64 = unsafe {
            ptr.byte_sub(UafDetectorSupportingAllocator::UAF_DETECTOR_ALLOC_DATA_OFF)
                .cast::<u64>()
                .read()
        };
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

    /// returns the id of this UAF detector.
    /// if this UAF detector has already been freed, this function safely detects the UAF and prints a corresponding error message.
    pub fn id(&self, key: UafDetectorKey) -> usize {
        self.try_id(key).unwrap()
    }
}
