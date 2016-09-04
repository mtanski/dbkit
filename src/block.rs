// vim : set ts=4 sw=4 et :

// libstd
use std::mem;
use std::slice;

// DBKit
use super::allocator::{Allocator, OwnedChunk};
use super::types::TypeInfo;
use super::schema::{Attribute, Schema};
use super::error::DBError;
use super::row::{RowOffset, RowRange};

pub type BoolBitmap<'a> = &'a [u8];
pub type MutBoolBitmap<'a> = &'a mut [u8];

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

/// Slice representing the data row data
///
/// RUST FRUSTRATION: wish this could be part of RefColumn
pub fn column_rows<'c, T: TypeInfo>(col: &'c RefColumn) -> Result<&'c [T::Store], DBError> {
    let attr = col.attribute();
    let rows = col.capacity();

    if attr.dtype != T::ENUM {
        return Err(DBError::AttributeType(attr.name.clone()))
    }

    unsafe {
        let col_ptr = col.rows_ptr();
        let typed_ptr: *const T::Store = mem::transmute(col_ptr);

        let out = if typed_ptr.is_null() {
            &[]
        } else {
            slice::from_raw_parts(typed_ptr, rows)
        };

        Ok(out)
    }
}

pub fn column_nulls<'c>(col: &'c RefColumn) ->  Result<BoolBitmap<'c>, DBError> {
    let attr = col.attribute();
    let rows = col.capacity();

    if !attr.nullable {
        return Err(DBError::AttributeNullability(attr.name.clone()))
    }

    unsafe {
        let nulls_ptr = col.nulls_ptr();

        let out = if nulls_ptr.is_null() {
            &[]
        } else {
            slice::from_raw_parts(nulls_ptr, rows)
        };

        Ok(out)
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
    let (offset, rows) = range.map(|r| (r.offset, r.rows)).unwrap_or((0, src.capacity()));

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
            .map(|f| f as &'alloc [u8])
            .unwrap_or(&[])
    }

    fn nulls_raw_slice(&'alloc self) -> &'alloc [u8] {
        self.raw_nulls.data.as_ref()
            .map(|f| f as &'alloc [u8])
            .unwrap_or(&[])
    }
}

impl<'alloc> Column<'alloc> {
    fn new(a: &'alloc Allocator, attr: Attribute) -> Column<'alloc> {
        Column {
            allocator: a,
            attr: attr,
            raw_nulls: OwnedChunk::empty(),
            raw: OwnedChunk::empty(),
        }
    }

    pub fn mut_nulls(&mut self) -> Result<MutBoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name.clone()))
        }

        let out: MutBoolBitmap = match self.raw_nulls.data {
            Some(ref mut slice) => slice,
            _ => &mut[],
        };

        Ok(out)
    }

    pub fn rows_mut<T: TypeInfo>(&mut self) -> Result<&mut [T::Store], DBError> {
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

// Create window into another view
pub fn window_alias<'a>(src: &'a View<'a>, range: Option<RowRange>)
    -> Result<RefView<'a>, DBError>
{
    let (offset, rows) = range.map(|r| (r.offset, r.rows)).unwrap_or((0, src.rows()));

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

    pub fn capacity(&self) -> RowOffset {
        self.capacity
    }

    /// Grow possible row space for each column
    pub fn set_capacity(&mut self, row_cap: RowOffset) -> Option<DBError> {
        for ref mut col in self.columns.iter_mut() {
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

    /// Mutable reference to column and its data.
    pub fn column_mut(&mut self, pos: usize) -> Option<&mut Column<'b>> {
        self.columns.get_mut(pos)
    }
}

