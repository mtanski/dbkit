// vim : set ts=4 sw=4 et :

use alloc::heap;
use std::mem;
use std::ptr;
use std::slice;
use std::cmp::min;

use super::error::DBError;

/// Minimum alignment for platform.
///
/// Takes into account SIMD (AVX2) types that will used for operations.
///
// RUST IS FRUSTRATING:
// mem::size_of is not const
// const MIN_ALIGN: usize = mem::size_of::<usize>();
pub const MIN_ALIGN: usize = 32;

/// Allocator trait, used through out the operations in dbkit.
///
/// Allocators have to maintain their own synchronization
pub trait Allocator : Send + Sync {
    fn allocate(&self, size: usize) -> Result<OwnedChunk, DBError>;
    fn allocate_aligned(&self, size: usize, align: usize) -> Result<OwnedChunk, DBError>;

    /// Resize; will try to resize in place if possible
    unsafe fn resize<'a>(&self, prev: &mut OwnedChunk<'a>, size: usize) -> Option<DBError>;

    fn putback(&self, data: &mut OwnedChunk);

    fn putback_raw(&self, ptr: *mut u8, size: usize, align: usize);
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
        OwnedChunk {
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
            .map_or(0, |slice| slice.len())
    }

    pub unsafe fn as_ptr(&self) -> *const u8 {
        self.data.as_ref()
            .map_or(ptr::null(), |slice| slice.as_ptr())
    }

    pub unsafe fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut()
            .map_or(ptr::null_mut(), |ref mut slice| slice.as_mut_ptr())
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
        let parent = self.parent.take();
        if let Some(p) = parent {
            p.putback(self);
        }
    }
}

/// Simple heap allocator without memory tracking
pub struct HeapAllocator { }


unsafe impl Send for HeapAllocator{}
unsafe impl Sync for HeapAllocator{}

/// A instance of default allocator when you don't care memory accounting, limitation
pub static GLOBAL: HeapAllocator = HeapAllocator{};

/// Simple heap allocator that delegates to `alloc::heap`
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
                parent: Some(self),
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

    fn putback(&self, c: &mut OwnedChunk) {
        if let Some(ref mut data) = c.data {
            self.putback_raw(data.as_mut_ptr(), data.len(), c.align)
        }
    }

    fn putback_raw(&self, ptr: *mut u8, size: usize, align: usize) {
        // Just deallocate, no heap tracking
        unsafe { heap::deallocate(ptr, size, align); }
    }
}

/// Result of arena append
/// Chunk offset & pointer
pub struct ArenaAppend(pub usize, pub *mut u8);

/// Arena styled allocator. Stores data in non-relocatable/non-movable arenas.
///
/// Policy is to increase allocation blocks 2X compare to previous block.
pub struct ChainedArena<'a> {
    parent: &'a Allocator,
    chunks: Vec<&'a mut [u8]>,
    min_size: usize,
    max_size: usize,
    pos: usize,
}

/// Helper for creating the next Arena using allocator. Unwraps from `OwnedChunk` since
/// `ChainedArena` managed deallocation for the whole container.
unsafe fn make_arena<'a>(alloc: &'a Allocator, size: usize) -> Result<&'a mut [u8], DBError> {
    alloc.allocate_aligned(size, MIN_ALIGN)
        .map(|mut c| {
            let mut out: &'a mut [u8] = mem::uninitialized();
            mem::swap(&mut out, c.data.as_mut().unwrap());
            mem::forget(c);
            out
        })
}

impl<'a> ChainedArena<'a> {

    pub fn new(alloc: &'a Allocator, min_size: usize, max_size: usize) -> ChainedArena<'a> {
        ChainedArena {
            parent: alloc,
            chunks: Vec::new(),
            min_size: min_size,
            max_size: max_size,
            pos: 0,
        }
    }

    pub unsafe fn allocate(&mut self, size: usize) -> Result<*mut u8, DBError> {
        if size > self.max_size {
            return Err(DBError::MemoryLimit);
        }

        let new_size = if let Some(ref mut arena) = self.chunks.last_mut() {
            if arena.len() - self.pos >= size {
                let ptr = arena.as_mut_ptr().offset(size as isize);
                self.pos += size;
                return Ok(ptr);
            }

            min(arena.len() * 2, self.max_size)
        } else {
            self.min_size
        };

        let new_arena = make_arena(self.parent, new_size)?;
        let ptr = new_arena.as_mut_ptr();

        self.chunks.push(new_arena);
        Ok(ptr)
    }

    pub fn append(&mut self, data: &[u8]) -> Result<ArenaAppend, DBError> {
        unsafe {
            let ptr = self.allocate(data.len())?;
            ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
            Ok(ArenaAppend(self.chunks.len(), ptr))
        }
    }
}

impl<'a> Drop for ChainedArena<'a> {
    fn drop(&mut self) {
        let mut arenas = Vec::new();
        mem::swap(&mut arenas, &mut self.chunks);
        for ref mut a in arenas {
            self.parent.putback_raw(a.as_mut_ptr(), a.len(), MIN_ALIGN);
        }
    }
}

