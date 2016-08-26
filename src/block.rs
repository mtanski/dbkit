// vim : set ts=4 sw=4 et :

// libstd
use std::mem;
use std::slice;

// DBKit
use super::allocator::{Allocator, RawChunk};
use super::types::{TypeInfo};
use super::schema::{Attribute, Schema};
use super::error::DBError;

pub type BoolBitmap<'a> = &'a [u8];
pub type MutBoolBitmap<'a> = &'a mut [u8];
pub type RowOffset = usize;


pub struct Column<'alloc> {
    allocator: &'alloc Allocator,
    attr: Attribute,
    raw_nulls: RawChunk<'alloc>,
    raw: RawChunk<'alloc>,
}

/// Typed Data Column. Contains a vector of column rows, and optionally a nul vector.
///
/// Knows its capacity but not size (that's up to the parent container). Has no concept of current
/// position.
impl<'alloc> Column<'alloc> {
    fn new(a: &'alloc Allocator, attr: Attribute) -> Column<'alloc> {
        Column {
            allocator: a,
            attr: attr,
            raw_nulls: RawChunk::empty(),
            raw: RawChunk::empty(),
        }
    }

    pub fn attribute(&self) -> &Attribute {
        &self.attr
    }

    pub fn capacity(&self) -> usize {
        println!("cap: {:?} {:?}", self.raw.size, self.attr.dtype.size_of());

        self.raw.size / self.attr.dtype.size_of()
    }

    pub fn nulls(&self) -> Result<BoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name.clone()))
        }

        unsafe {
            return Ok(slice::from_raw_parts(self.raw_nulls.data, self.capacity()));
        }
    }

    pub fn rows<T: TypeInfo>(&self) -> Result<&[T::Store], DBError>  {
        if self.attr.dtype != T::ENUM {
            return Err(DBError::AttributeType(self.attr.name.clone()))
        }

        unsafe {
            let ptr: *const T::Store = mem::transmute(self.raw.data);
            return Ok(slice::from_raw_parts(ptr, self.capacity()));
        }
    }

    pub fn mut_nulls(&mut self) -> Result<MutBoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name.clone()))
        }

        unsafe {
            return Ok(slice::from_raw_parts_mut(self.raw_nulls.data, self.capacity()));
        }
    }

    pub fn rows_mut<T: TypeInfo>(&mut self) -> Result<&mut [T::Store], DBError> {
        if self.attr.dtype != T::ENUM {
            return Err(DBError::AttributeType(self.attr.name.clone()))
        }

        unsafe {
            let ptr: *mut T::Store = mem::transmute(self.raw.data);
            return Ok(slice::from_raw_parts_mut(ptr, self.capacity()));
        }
    }

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

    pub unsafe fn raw_data(&mut self) -> *mut u8 {
        self.raw.data
    }
}

pub trait View<'v> {
    fn schema(&self) -> &Schema;
    fn column(&'v self, pos: usize) -> Option<&'v Column>;
    fn rows(&self) -> RowOffset;
}

pub struct Block<'b> {
    allocator: &'b Allocator,
    schema: Schema,
    columns: Vec<Column<'b>>,
    rows: RowOffset,
    capacity: RowOffset,
}

impl<'b> View<'b> for Block<'b> {
    fn schema(& self) -> &Schema {
        &self.schema
    }

    fn column(&'b self, pos: usize) -> Option<&'b Column> {
        if pos < self.columns.len() {
            let col: &Column = &self.columns[pos];
            Some(col)
        } else {
            None
        }
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

    /// panics on out of bounds column
    pub fn column_mut(&mut self, pos: usize) -> Option<&mut Column<'b>> {
        self.columns.get_mut(pos)
    }
}

