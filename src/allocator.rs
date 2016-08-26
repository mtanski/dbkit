// vim : set ts=4 sw=4 et :

use std::ptr;
use alloc::heap;

use super::error::DBError;

/// Allocator trait, used through out the operations in dbkit.
///
///
/// Allocators have to maintain their own synchornization
pub trait Allocator : Send + Sync {
    fn allocate(&self, size: usize) -> Result<RawChunk, DBError>;
    fn allocate_aligned(&self, size: usize, align: usize) -> Result<RawChunk, DBError>;

    // Resize; will try to resize in place if possible
    unsafe fn resize(&self, prev: *mut u8, prev_size: usize, size: usize)
        -> Result<*mut u8, DBError>;
    // Resize respecting alignment; will try to to resize in place if possible.
    unsafe fn resize_aligned(&self, prev: *mut u8, prev_size: usize, size: usize, align: usize)
        -> Result<*mut u8, DBError>;

    fn putback(&self, size: usize);
}

pub struct RawChunk<'a> {
    parent: Option<&'a Allocator>,
    pub data: *mut u8,
    pub size: usize,
    pub align: usize,
}


/// Simple heap allocator without memory tracking
pub struct HeapAllocator { }

/// Minimum alignment for platform.
///
/// Takes into account SIMD (AVX2) types that will used for operations.
///
// RUST IS FRUSTRATING:
// mem::size_of is not const
// const MIN_ALIGN: usize = mem::size_of::<usize>();
const MIN_ALIGN: usize = 32;

impl<'a> RawChunk<'a> {
    pub fn empty() -> RawChunk<'a> {
        return RawChunk {
            parent: None,
            data: ptr::null_mut(),
            size: 0,
            align: MIN_ALIGN,
        }
    }

    pub fn is_null(&self) -> bool {
       self.data.is_null()
    }

    pub fn resize(&mut self, size: usize) -> Option<DBError> {
        unsafe {
            self.parent.as_ref()
                .ok_or(DBError::Memory)
                .and_then(|ref p| p.resize_aligned(self.data.clone(), self.size, self.size, self.align))
                .and_then(|ptr| {self.data = ptr; self.size = size; Ok(()) })
                .err()
        }
    }
}

impl<'a> Drop for RawChunk<'a> {
    fn drop(&mut self) {
        if !self.is_null() {
            unsafe {
                heap::deallocate(self.data, self.size, self.align);
            }

            if let Some(ref mut p) = self.parent {
                p.putback(self.size);
            }
        }
    }
}

unsafe impl Send for HeapAllocator{}
unsafe impl Sync for HeapAllocator{}

/// Simple heap allocator that delegates to alloc::heap
impl Allocator for HeapAllocator {
    fn allocate(&self, size: usize) -> Result<RawChunk, DBError> {
        self.allocate_aligned(size, MIN_ALIGN)
    }

    fn allocate_aligned(&self, size: usize, align: usize) -> Result<RawChunk, DBError> {
        unsafe {
            let data = heap::allocate(size, align);
            if !data.is_null() {
                // There's no tracking of memory here
                return Ok(RawChunk { parent: None, data: data, size: size, align: align});
            } else {
                return Err(DBError::Memory)
            }
        }
    }

    unsafe fn resize(&self, prev: *mut u8, prev_size: usize, size: usize)
        -> Result<*mut u8, DBError>
    {
        self.resize_aligned(prev, prev_size, size, MIN_ALIGN)
    }

    unsafe fn resize_aligned(&self, prev: *mut u8, prev_size: usize, size: usize, align: usize)
        -> Result<*mut u8, DBError>
    {
        unsafe {
            let nlen = heap::reallocate_inplace(prev, prev_size, size, align);

            if nlen == size {
                return Ok(prev)
            }

            let data = heap::reallocate(prev, prev_size, size, align);

            if data.is_null() {
                Ok(data)
            } else {
                Err(DBError::Memory)
            }
        }
    }

    fn putback(&self, size: usize) {
        panic!("Global heap doesn't keep track of memory usage")
    }
}

pub static GLOBAL: HeapAllocator = HeapAllocator{};

