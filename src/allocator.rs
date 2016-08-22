// vim : set ts=4 sw=4 et :

use std::mem;
use alloc::heap;

use super::error::DBError;

/// Allocator trait, used through out the operations in dbkit.
///
///
/// Allocators have to maintain their own synchornization
pub trait Allocator : Sync {
    fn allocate(&mut self, size: usize) -> Result<RawChunk, DBError>;
    fn allocate_aligned(&mut self, size: usize, align: usize) -> Result<RawChunk, DBError>;
}

pub struct RawChunk<'a> {
    parent: &'a mut Allocator,
    data: *mut u8,
    size: usize,
    align: usize,
}


/// Simple heap allocator with no memory tracking
pub struct HeapAllocator {

}

/// Minimum alignment for platform.
///
/// Takes into account SIMD types that will used for operations.
const MIN_ALIGN: usize = mem::size_of::<usize>;

impl<'a> Drop for RawChunk<'a> {
    fn drop(&mut self) {
        if !self.data.is_null() {
            unsafe {
                heap::deallocate(self.data, self.size, self.align)
            }
        }
    }
}

unsafe impl Sync for HeapAllocator {}

/// Simple heap allocator that delegates to alloc::heap
impl Allocator for HeapAllocator {
    fn allocate(&self, size: usize) -> Result<RawChunk, DBError> {
        self.allocate_align(size, MIN_ALIGN);
    }

    fn allocate_aligned(&self, size: usize, align: usize) -> Result<RawChunk, DBError> {
        unsafe {
            let data = heap::allocate(size, align);
            if data {
                Ok(RawChunk { data: data, size: size, align: align});
            } else {
                Err(DBError::Memory)
            }
        }
    }
}

static mut globalHeap : HeapAllocator = HeapAllocator{};

impl HeapAllocator {
    fn global() -> &'static mut HeapAllocator {
        &globalHeap
    }
}
