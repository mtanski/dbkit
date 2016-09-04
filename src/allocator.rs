// vim : set ts=4 sw=4 et :

use std::ptr;
use std::slice;
use alloc::heap;

use super::error::DBError;

/// Minimum alignment for platform.
///
/// Takes into account SIMD (AVX2) types that will used for operations.
///
// RUST IS FRUSTRATING:
// mem::size_of is not const
// const MIN_ALIGN: usize = mem::size_of::<usize>();
const MIN_ALIGN: usize = 32;

/// Allocator trait, used through out the operations in dbkit.
///
/// Allocators have to maintain their own synchronization
pub trait Allocator : Send + Sync {
    fn allocate(&self, size: usize) -> Result<OwnedChunk, DBError>;
    fn allocate_aligned(&self, size: usize, align: usize) -> Result<OwnedChunk, DBError>;

    /// Resize; will try to resize in place if possible
    unsafe fn resize<'a>(&self, prev: &mut OwnedChunk<'a>, size: usize) -> Option<DBError>;

    // TODO: take in ChunkData & align
    fn putback(&self, data: &mut OwnedChunk);
}

pub type RefChunk<'a> = &'a mut [u8];

/// Chunk with an allocator owner
pub struct OwnedChunk<'a> {
    parent: Option<&'a Allocator>,
    pub data: Option<&'a mut[u8]>,
    pub align: usize,
}

impl<'a> OwnedChunk<'a> {
    pub fn empty() -> OwnedChunk<'static> {
        return OwnedChunk {
            parent: None,
            data: None,
            align: MIN_ALIGN,
        }
    }

    pub fn is_null(&self) -> bool {
        self.data.is_none()
    }

    pub fn len(&self) -> usize {
        self.data.as_ref()
            .map(|ref slice| slice.len())
            .unwrap_or(0)
    }

    pub unsafe fn as_ptr(&self) -> *const u8 {
        self.data.as_ref()
            .map(|ref slice| slice.as_ptr())
            .unwrap_or(ptr::null())
    }

    pub unsafe fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut()
            .map(|ref mut slice| slice.as_mut_ptr())
            .unwrap_or(ptr::null_mut())
    }

    /// Attempt to resize the chunk. If possible it will attempt to resize in-place, if not possible
    /// it will create new alloc and copy the old data.
    pub fn resize(&mut self, size: usize) -> Option<DBError> {
        unsafe {
            if let Some(allocator) = self.parent {
                return allocator.resize(self, size);
            }

            Some(DBError::Memory)
        }
    }
}

impl<'a> Drop for OwnedChunk<'a> {
    fn drop(&mut self) {
        unsafe {
            if self.data.is_none() {
                return;
            }

            let parent = self.parent.take();
            if let Some(p) = parent {
                p.putback(self);
            } else {
                // Optimization for HeapAllocator
                heap::deallocate(self.as_mut_ptr(), self.len(), self.align);
            }
        }
    }
}

/// Simple heap allocator without memory tracking
pub struct HeapAllocator { }


unsafe impl Send for HeapAllocator{}
unsafe impl Sync for HeapAllocator{}

/// A instance of default allocator when you don't care memory accounting, limitation
pub static GLOBAL: HeapAllocator = HeapAllocator{};

/// Simple heap allocator that delegates to alloc::heap
impl Allocator for HeapAllocator {
    fn allocate(&self, size: usize) -> Result<OwnedChunk, DBError> {
        self.allocate_aligned(size, MIN_ALIGN)
    }

    fn allocate_aligned(&self, size: usize, align: usize) -> Result<OwnedChunk, DBError> {
        unsafe {
            let data = heap::allocate(size, align);

            if data.is_null() {
                return Err(DBError::Memory);
            }

            let slice = slice::from_raw_parts_mut::<u8>(data, size);

            Ok(OwnedChunk {
                // There's no tracking of memory here
                parent: None,
                data: Some(slice),
                align: align,
            })
        }
    }

    unsafe fn resize<'a>(&self, prev: &mut OwnedChunk<'a>, size: usize) -> Option<DBError>
    {
        let mut data = prev.as_mut_ptr();

        let nlen = heap::reallocate_inplace(data, prev.len(), size, prev.align);

        if nlen != size {
            data = heap::reallocate(data, prev.len(), size, prev.align);
            if data.is_null() {
                return Some(DBError::Memory)
            }
        }

        prev.data = Some(slice::from_raw_parts_mut::<u8>(data, size));
        None
    }

    fn putback(&self, _: &mut OwnedChunk) {
        panic!("Global heap doesn't keep track of memory usage")
    }
}

