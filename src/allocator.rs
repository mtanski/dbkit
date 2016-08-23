// vim : set ts=4 sw=4 et :

use std::ptr;
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
    pub data: *mut u8,
    pub size: usize,
    pub align: usize,
}


/// Simple heap allocator with no memory tracking
pub struct HeapAllocator {

}

/// Minimum alignment for platform.
///
/// Takes into account SIMD types that will used for operations.
///
/// RUST SUCKS: mem::size_of is not consnt
/// const MIN_ALIGN: usize = mem::size_of::<usize>();
// AVX2
const MIN_ALIGN: usize = 32;

impl<'a> RawChunk<'a> {
    pub fn empty() -> RawChunk<'a> {
        return RawChunk {
            parent: HeapAllocator::global(),
            data: ptr::null_mut(),
            size: 0,
            align: MIN_ALIGN,
        }
    }

    fn is_null(&self) -> bool {
        self.data.is_null()
    }
}

impl<'a> Drop for RawChunk<'a> {
    fn drop(&mut self) {
        if !self.is_null() {
            unsafe {
                heap::deallocate(self.data, self.size, self.align)
            }
        }
    }
}

unsafe impl Sync for HeapAllocator {}

/// Simple heap allocator that delegates to alloc::heap
impl Allocator for HeapAllocator {
    fn allocate(&mut self, size: usize) -> Result<RawChunk, DBError> {
        self.allocate_aligned(size, MIN_ALIGN)
    }

    fn allocate_aligned(&mut self, size: usize, align: usize) -> Result<RawChunk, DBError> {
        unsafe {
            let data = heap::allocate(size, align);
            if !data.is_null() {
                return Ok(RawChunk { parent: self, data: data, size: size, align: align});
            } else {
                return Err(DBError::Memory)
            }
        }
    }
}

static mut globalHeap : HeapAllocator = HeapAllocator{};

impl HeapAllocator {
    pub fn global() -> &'static mut HeapAllocator {
        unsafe {
            &mut globalHeap
        }
    }
}
