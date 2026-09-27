use std::{alloc::GlobalAlloc, cell::RefCell};

struct UafDetectorAllocs {
    live: Vec<*mut u8>,
    freed: Vec<*mut u8>,
}

thread_local! {
    static DEALLOC_PASSTHROUGH_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
}

fn with_dealloc_passthrough<R, F: FnOnce() -> R>(f: F) -> R {
    DEALLOC_PASSTHROUGH_COUNT.with(|x| {
        x.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let res = f();
        x.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        res
    })
}

struct UafDetectorSupportingAllocator {
    // can't use std::mutex as it may allocate.
    uaf_detector_allocs: parking_lot::ReentrantMutex<RefCell<UafDetectorAllocs>>,
}
unsafe impl Sync for UafDetectorSupportingAllocator {}
impl UafDetectorSupportingAllocator {
    const fn new() -> Self {
        Self {
            uaf_detector_allocs: parking_lot::ReentrantMutex::new(RefCell::new(
                UafDetectorAllocs {
                    live: Vec::new(),
                    freed: Vec::new(),
                },
            )),
        }
    }

    fn alloc_uaf_detector(&self, id: usize) -> Box<UafDetector> {
        let mut res = Box::new(UafDetector::new_noalloc(id));

        with_dealloc_passthrough(|| {
            let uaf_detector_allocs_guard = self.uaf_detector_allocs.lock();
            let mut uaf_detector_allocs = uaf_detector_allocs_guard.borrow_mut();
            uaf_detector_allocs
                .live
                .push(Box::as_mut_ptr(&mut res).cast::<u8>());
        });

        res
    }
}
impl Drop for UafDetectorSupportingAllocator {
    fn drop(&mut self) {
        with_dealloc_passthrough(|| {
            let uaf_detector_allocs_guard = self.uaf_detector_allocs.lock();
            let mut uaf_detector_allocs = uaf_detector_allocs_guard.borrow_mut();
            for ptr in uaf_detector_allocs.freed.drain(..) {
                let _ = unsafe { Box::from_raw(ptr.cast::<UafDetector>()) };
            }
        })
    }
}
unsafe impl GlobalAlloc for UafDetectorSupportingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        unsafe { std::alloc::System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        let passthrough_count =
            DEALLOC_PASSTHROUGH_COUNT.with(|x| x.load(std::sync::atomic::Ordering::SeqCst));
        if passthrough_count == 0 {
            with_dealloc_passthrough(|| {
                let uaf_detector_allocs_guard = self.uaf_detector_allocs.lock();
                let mut uaf_detector_allocs = uaf_detector_allocs_guard.borrow_mut();
                if let Some(live_alloc_index) =
                    uaf_detector_allocs.live.iter().position(|x| *x == ptr)
                {
                    // move this allocation from the live list to the free list, but don't free it completely just yet.
                    uaf_detector_allocs.live.swap_remove(live_alloc_index);
                    uaf_detector_allocs.freed.push(ptr);
                    return;
                }
            })
        }
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: UafDetectorSupportingAllocator = UafDetectorSupportingAllocator::new();

/// a type used to detect use after free scenarios.
pub struct UafDetector {
    id: usize,
    was_freed: bool,
}
impl UafDetector {
    /// creates a new uaf detector value without allocating it on the heap.
    fn new_noalloc(id: usize) -> UafDetector {
        Self {
            id,
            was_freed: false,
        }
    }

    /// creates a new UAF detector with the given id.
    pub fn new(id: usize) -> Box<UafDetector> {
        GLOBAL_ALLOCATOR.alloc_uaf_detector(id)
    }

    /// checks if this UAF detector had already been freed, in which case this is a UAF access.
    pub fn was_freed(&self) -> bool {
        self.was_freed
    }

    /// returns the id of this UAF detector, or `None` if this UAF detector has already been freed.
    pub fn try_id(&self) -> Option<usize> {
        if self.was_freed { None } else { Some(self.id) }
    }

    /// returns the id of this UAF detector.
    /// if this UAF detector has already been freed, this function safely detects the UAF and prints a corresponding error message.
    pub fn id(&self) -> usize {
        self.try_id()
            .expect("attempted to use a UAF detector object after it was freed")
    }
}
impl Drop for UafDetector {
    fn drop(&mut self) {
        self.was_freed = true;
    }
}
