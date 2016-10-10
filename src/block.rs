// vim : set ts=4 sw=4 et :

// libstd
use std::mem;
use std::slice;
use std::ops::{Index, IndexMut};

// DBKit
use ::allocator::{Allocator, OwnedChunk, ChainedArena, MIN_ALIGN};
use ::types::ValueInfo;
use ::schema::{Attribute, Schema};
use ::error::DBError;
use ::row::{RowOffset, RowRange};
use ::util::math::*;

pub type BoolBitmap<'a> = &'a [u8];
pub type MutBoolBitmap<'a> = &'a mut [u8];

/// Starting size for the VARLEN arena
const ARENA_MIN_SIZE : usize = MIN_ALIGN;

/// Limit on arena chunk size. This is also on the largest VARLEN value in Columns.
/// Currently the limit for large blobs / text is up to 16MB.
const ARENA_MAX_SIZE : usize = 16 * 1024 * 1024;

pub struct ColumnRows<'a, T: ValueInfo>
    where <T as ValueInfo>::Store: 'a
{
    pub values: &'a [T::Store],
    pub nulls: BoolBitmap<'a>,
}

pub struct ColumnRowsMut<'a, T: ValueInfo>
    where <T as ValueInfo>::Store: 'a
{
    pub values: &'a mut [T::Store],
    pub nulls: MutBoolBitmap<'a>,
}

/// Trait representing a reference to column data.
/// Data can be owned by current object or references from another one.
pub trait RefColumn<'re> {
    fn attribute(&self) -> &Attribute;
    fn capacity(&self) -> usize;

    /// Will panic if there's no row data
    fn rows_raw_slice(&'re self) -> &'re [u8];
    /// Will panic if there's no null data
    fn nulls_raw_slice(&'re self) -> &'re [u8];

    /// Pointer to the beginning of the raw row data.
    /// ptr can be nil
    unsafe fn rows_ptr(&self) -> *const u8;
    /// Pointer to the beginning of the raw row data.
    /// ptr can be nil
    unsafe fn nulls_ptr(&self) -> *const u8;
}

/// Helper badness for converting raw column data into a typed slice of rows.
// It's not really 'static, but we don't have enough context in thi
#[inline]
unsafe fn rows_from_rawptr<'a, T>(ptr: *mut u8, elems: usize) -> &'a mut [T] {
    let typed_ptr: *mut T = mem::transmute(ptr);
    if !typed_ptr.is_null() {
        slice::from_raw_parts_mut(typed_ptr, elems)
    } else {
        &mut []
    }
}

/// Helper badness for converting raw column data into a slice of row's null bitmap.
// It's not really 'static, but we don't have enough context in thi
#[inline]
unsafe fn rows_from_rawptr_const<'a, T>(ptr: *const u8, elems: usize) -> &'a [T] {
    let typed_ptr: *const T = mem::transmute(ptr);
    if !typed_ptr.is_null() {
        slice::from_raw_parts(typed_ptr, elems)
    } else {
        &[]
    }
}

/// Two slices. One representing the column value vector (row data). Second representing the column
/// null vector (row data).
// RUST FRUSTRATION: wish this could be part of `RefColumn`.
// Can't have generics methods be part of the trait... even if there's default trait implementation.
//
// Inline: so we can optimize away a bunch of code (like we're not using nulls in context)
#[inline]
pub fn column_row_data<'c, T: ValueInfo>(col: &'c RefColumn) -> Result<ColumnRows<'c, T>, DBError> {
    let attr = col.attribute();
    let rows = col.capacity();

    if attr.dtype != T::ENUM {
        return Err(DBError::AttributeType(attr.name.clone()))
    }

    unsafe {
        Ok(ColumnRows{
            values: rows_from_rawptr_const::<T::Store>(col.rows_ptr(), rows),
            nulls: rows_from_rawptr_const::<u8>(col.nulls_ptr(), rows),
        })
    }
}

/// Typed Data Column. Contains a vector of column rows, and optionally a nul vector.
///
/// Knows its capacity but not size, has no concept of current. Those properties are fulfilled by
/// it's parent container (types such as Block).
pub struct Column<'alloc> {
    allocator: &'alloc Allocator,
    attr: Attribute,
    raw_nulls: OwnedChunk<'alloc>,
    raw: OwnedChunk<'alloc>,
    /// Used to store varlen column values
    arena: ChainedArena<'alloc>
}

/// Typed Data Column that references another column
#[derive(Clone)]
pub struct AliasColumn<'parent> {
    attr: Attribute,
    raw_nulls: &'parent [u8],
    raw: &'parent [u8],
}

/// Create another read only alias of a column
///
/// If no range is specified, aliases the whole column source column.
pub fn alias_column<'a>(src: &'a RefColumn<'a>, range: Option<RowRange>)
    -> Result<AliasColumn<'a>, DBError>
{
    let (offset, rows) = range.map_or((0, src.capacity()), |r| (r.offset, r.rows));

    let size_of = src.attribute().dtype.size_of();
    let start = offset * size_of;
    let len = rows + size_of;

    if offset + rows > src.capacity() {
        return Err(DBError::RowOutOfBounds)
    }

    let raw = src.rows_raw_slice();
    let col = &raw[start .. start + len];

    let nulls = if src.attribute().nullable {
        let raw = src.nulls_raw_slice();
        &raw[offset .. offset + rows]
    } else {
        &[]
    };

    Ok(AliasColumn {
        attr: src.attribute().clone(),
        raw: col,
        raw_nulls: nulls,
    })
}

impl<'parent> RefColumn<'parent> for AliasColumn<'parent> {
    fn attribute(&self) -> &Attribute {
        &self.attr
    }

    /// Row capacity
    fn capacity(&self) -> usize {
        self.raw.len() / self.attr.dtype.size_of()
    }

    /// Pointer to the beginning of the raw row data
    unsafe fn rows_ptr(&self) -> *const u8 {
        self.raw.as_ptr()
    }

    /// Pointer to the beginning of the raw row data
    unsafe fn nulls_ptr(&self) -> *const u8 {
        self.raw_nulls.as_ptr()
    }

    fn rows_raw_slice(&'parent self) -> &'parent [u8] {
        self.raw
    }

    fn nulls_raw_slice(&'parent self) -> &'parent [u8] {
        self.raw_nulls
    }
}

impl<'alloc> RefColumn<'alloc> for Column<'alloc> {
    fn attribute(&self) -> &Attribute {
        &self.attr
    }

    /// Row capacity
    fn capacity(&self) -> usize {
        self.raw.len() / self.attr.dtype.size_of()
    }

    /// Pointer to the beginning of the raw row data
    unsafe fn rows_ptr(&self) -> *const u8 {
        self.raw.as_ptr()
    }

    /// Pointer to the beginning of the raw row data
    unsafe fn nulls_ptr(&self) -> *const u8 {
        self.raw_nulls.as_ptr()
    }

    fn rows_raw_slice(&'alloc self) -> &'alloc [u8] {
        self.raw.data.as_ref()
            .map_or(&[], |f| f as &'alloc [u8])
    }

    fn nulls_raw_slice(&'alloc self) -> &'alloc [u8] {
        self.raw_nulls.data.as_ref()
            .map_or(&[], |f| f as &'alloc [u8])
    }
}

impl<'alloc> Column<'alloc> {
    fn new(a: &'alloc Allocator, attr: Attribute) -> Column<'alloc> {
        Column {
            allocator: a,
            attr: attr,
            raw_nulls: OwnedChunk::empty(),
            raw: OwnedChunk::empty(),
            arena: ChainedArena::new(a, ARENA_MIN_SIZE, ARENA_MAX_SIZE),
        }
    }

    pub fn arena(&mut self) -> &mut ChainedArena<'alloc> {
        &mut self.arena
    }

    pub fn nulls_mut(&mut self) -> Result<MutBoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name.clone()))
        }

        let out: MutBoolBitmap = match self.raw_nulls.data {
            Some(ref mut slice) => slice,
            _ => &mut[],
        };

        Ok(out)
    }

    pub fn rows_mut<T: ValueInfo>(&mut self) -> Result<&mut [T::Store], DBError> {
        if self.attr.dtype != T::ENUM {
            return Err(DBError::AttributeType(self.attr.name.clone()))
        }

        unsafe {
            let ptr: *mut T::Store = mem::transmute(self.raw.as_mut_ptr());
            let out = if ptr.is_null() {
                &mut []
            } else {
                slice::from_raw_parts_mut(ptr, self.capacity())
            };

            Ok(out)
        }
    }

    pub fn row_data_mut<T: ValueInfo>(&mut self) -> Result<ColumnRowsMut<T>, DBError> {
        if self.attr.dtype != T::ENUM {
            return Err(DBError::AttributeType(self.attr.name.clone()))
        }

        unsafe {
            let ptr: *mut T::Store = mem::transmute(self.raw.as_mut_ptr());
            let rows = if ptr.is_null() {
                &mut []
            } else {
                slice::from_raw_parts_mut(ptr, self.capacity())
            };

            let nulls: MutBoolBitmap = match self.raw_nulls.data {
                Some(ref mut slice) => slice,
                _ => &mut[],
            };

            Ok(ColumnRowsMut{ values: rows, nulls: nulls})
        }
    }

    /// Change the capacity of the Column
    pub fn set_capacity(&mut self, rows: RowOffset) -> Option<DBError> {
        let new_size = rows * self.attr.dtype.size_of();

        if self.raw.is_null() {
            match self.allocator.allocate(new_size) {
                Ok(chunk) => self.raw = chunk,
                Err(e) => return Some(e)
            }

            if self.attr.nullable {
                match self.allocator.allocate(rows) {
                    Ok(chunk) => self.raw_nulls = chunk,
                    Err(e) => return Some(e)
                }
            }
        } else {
            let status = self.raw.resize(new_size);
            if status.is_some() {
                return status;
            }

            if self.attr.nullable {
                let nulls_status = self.raw_nulls.resize(rows);
                if nulls_status.is_some() {
                    return nulls_status;
                }
            }
        }

        None
    }
}

/// A read-only view into data conforming to a pre-defined schema. This view may be backed by a
/// container that owns it data, borrows or aliases somebody elses data.
pub trait View<'v> {
    fn schema(&'v self) -> &'v Schema;
    fn column(&'v self, pos: usize) -> Option<&'v RefColumn<'v>>;

    /// Number of rows
    fn rows(&self) -> RowOffset;
}

/// An implementation of a View that doesn't "own" the data but aliases it
#[derive(Default)]
pub struct RefView<'a> {
    schema: Schema,
    columns: Vec<AliasColumn<'a>>,
    rows: RowOffset,
}

/// Take a view and create a vector of column aliases
pub fn alias_columns<'a>(src: &'a View<'a>, range: Option<RowRange>)
    -> Result<Vec<AliasColumn<'a>>, DBError>
{
    let count = src.schema().count();
    let mut out: Vec<AliasColumn> = Vec::with_capacity(count);

    for pos in 0 .. count {
        let col = alias_column(src.column(pos).unwrap(), range)?;
        out.push(col);
    }

    Ok(out)
}

impl<'a> View<'a> for RefView<'a> {
    fn schema(&'a self) -> &'a Schema {
        &self.schema
    }

    fn column(&'a self, pos: usize) -> Option<&RefColumn> {
        self.columns.get(pos)
            .map(|c| c as &RefColumn)
    }

    fn rows(&self) -> RowOffset {
        self.rows
    }
}

/// Create window into another view
pub fn window_alias<'a>(src: &'a View<'a>, range: Option<RowRange>)
    -> Result<RefView<'a>, DBError>
{
    let (offset, rows) = range.map_or((0, src.rows()), |r| (r.offset, r.rows));

    if offset + rows > src.rows() {
        Err(DBError::RowOutOfBounds)
    } else {
        let schema = src.schema();

        Ok(RefView {
            schema: schema.clone(),
            rows: rows,
            columns: alias_columns(src, range)?,
        })
    }
}

impl<'a> RefView<'a> {
    pub fn new(schema: Schema, columns: Vec<AliasColumn<'a>>, rows: RowOffset) -> RefView<'a> {
        RefView { schema: schema, columns: columns, rows: rows }
    }
}

/// A container for column data conforming to a pre-defined schema. This container is the owner of
/// the columns (and their data)
pub struct Block<'b> {
    allocator: &'b Allocator,
    schema: Schema,
    columns: Vec<Column<'b>>,
    rows: RowOffset,
    capacity: RowOffset,
}

impl<'b> View<'b> for Block<'b> {
    fn schema(&'b self) -> &'b Schema {
        &self.schema
    }

    fn column(&'b self, pos: usize) -> Option<&RefColumn> {
        self.columns.get(pos)
            .map(|c| c as &RefColumn)
    }

    fn rows(&self) -> RowOffset {
        self.rows
    }
}

impl<'b> Block<'b> {
    pub fn new(alloc: &'b Allocator, schema: &Schema) -> Block<'b> {
        let mut b = Block {
            allocator: alloc,
            schema: schema.clone(),
            rows: 0,
            capacity: 0,
            columns: Vec::new()
        };

        for attr in schema.iter() {
            b.columns.push(Column::new(b.allocator, attr.clone()))
        }

        b
    }

    /// Number of rows the Block can currently grow to without re-allocating column data.
    pub fn capacity(&self) -> RowOffset {
        self.capacity
    }

    /// Grow possible row space for each column
    pub fn set_capacity(&mut self, row_cap: RowOffset) -> Option<DBError> {
        for ref mut col in &mut self.columns {
            let status = col.set_capacity(row_cap);
            if status.is_some() {
                return status;
            }
        }

        self.capacity = row_cap;
        if row_cap < self.rows {
            self.rows = row_cap;
        }

        None
    }

    /// Returns rowid of the added row
    pub fn add_row(&mut self) -> Result<RowOffset, DBError> {
        if self.capacity > self.rows {
            let rowid = self.rows;
            self.rows += 1;
            Ok(rowid)
        } else {
            let rowid = self.rows;
            let new_cap = self.capacity + 1024;

            if let Some(err) = self.set_capacity(new_cap) {
                Err(err)
            } else {
                self.rows += 1;
                Ok(rowid)
            }
        }
    }

    /// Add a slew of uninitialized rows
    pub fn add_rows(&mut self, rows: RowOffset) -> Result<RowOffset, DBError> {
        if self.capacity > self.rows + rows {
            let rowid = self.rows + rows;
            self.rows += rows;
            Ok(rowid)
        } else {
            let rowid = self.rows;
            let mut new_cap = self.capacity + rows;
            new_cap = round_up(new_cap, 1024);

            if let Some(err) = self.set_capacity(new_cap) {
                Err(err)
            } else {
                self.rows += rows;
                Ok(rowid)
            }
        }
    }

    /// Mutable reference to column and its data.
    pub fn column_mut(&mut self, pos: usize) -> Option<&mut Column<'b>> {
        self.columns.get_mut(pos)
    }
}

impl<'a> Index<usize> for Block<'a> {
    type Output = Column<'a>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.columns[index]
    }
}

/// Address mutable column by its inde
impl<'a> IndexMut<usize> for Block<'a> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.columns[index]
    }
}