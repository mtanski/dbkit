// vim : set ts=4 sw=4 et :

use std::mem;
use std::slice;

use super::allocator::{self, Allocator, RawChunk};
use super::types::{self, Type, TypeInfo};
use super::schema::{Attribute, Schema};
use super::error::DBError;

pub type BoolBitmap<'a> = &'a [u8];
pub type MutBoolBitmap<'a> = &'a mut [u8];
pub type RowOffset = usize;


pub struct Column<'alloc> {
    allocator: &'alloc mut Allocator,
    attr: Attribute,
    raw_nulls: RawChunk<'alloc>,
    raw: RawChunk<'alloc>,
}

impl<'alloc> Column<'alloc> {
    fn new(a: &'alloc mut Allocator, attr: Attribute) -> Column<'alloc> {
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

    pub fn nulls(&self) -> Result<BoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name.clone()))
        }

        unsafe {
            return Ok(slice::from_raw_parts(self.raw_nulls.data, self.raw_nulls.size));
        }
    }

    pub fn mut_nulls(&mut self) -> Result<MutBoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name.clone()))
        }

        unsafe {
            return Ok(slice::from_raw_parts_mut(self.raw_nulls.data, self.raw_nulls.size));
        }
    }

    pub fn rows<T: TypeInfo>(&self) -> Result<&[T::Store], DBError>  {
        if self.attr.dtype != T::ENUM {
            return Err(DBError::AttributeType(self.attr.name.clone()))
        }

        unsafe {
            let ptr: *const T::Store = mem::transmute(self.raw.data);
            return Ok(slice::from_raw_parts(ptr, self.raw.size));
        }
    }

    pub fn rows_mut<T: TypeInfo>(&mut self) -> Result<&mut [T::Store], DBError> {
        if self.attr.dtype != T::ENUM {
            return Err(DBError::AttributeType(self.attr.name.clone()))
        }

        unsafe {
            let ptr: *mut T::Store = mem::transmute(self.raw.data);
            return Ok(slice::from_raw_parts_mut(ptr, self.raw.size));
        }
    }

    pub unsafe fn raw_data(&mut self) -> *mut u8 {
        self.raw.data
    }
}

pub trait View<'v> {
    fn schema(&'v self) -> &'v Schema;
    fn column(&'v self, pos: usize) -> Option<&'v Column>;
    fn rows(&self) -> RowOffset;
}

pub struct Block<'alloc> {
    allocator: &'alloc mut Allocator,
    schema: Schema,
    columns: Vec<Column<'alloc>>,
    rows: RowOffset,
    capacity: RowOffset,
}

impl<'alloc> View<'alloc> for Block<'alloc> {
    fn schema(&'alloc self) -> &'alloc Schema {
        &self.schema
    }

    fn column(&'alloc self, pos: usize) -> Option<&'alloc Column> {
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

impl<'alloc> Block<'alloc> {
    pub fn new(alloc: &'alloc mut Allocator, schema: &Schema) -> Block<'alloc> {
        Block {
            allocator: alloc,
            schema: schema.clone(),
            rows: 0,
            capacity: 0,
            columns: Vec::new(),
        }
/*
        columns: schema.iter()
        .map(|attr| Column::new<'alloc>(alloc, attr))
        .collect()

        b;
        */
    }

    pub fn capacity(&self) -> RowOffset {
        self.capacity
    }

    pub fn set_capacity(&mut self, size: RowOffset) -> Option<DBError> {
        if size < self.capacity {
            for col in self.columns.iter_mut() {
                // TODO: Resize
            }
        }

        None
    }

    pub fn expand(&mut self) -> Option<RowOffset> {
        if self.capacity > self.rows {
            let rowid = self.rows;
            self.rows += 1;
            Some(rowid)
        } else {
            let rowid = self.rows;
            let new_cap = self.capacity + 1024;
            self.set_capacity(new_cap);
            self.rows += 1;
            Some(rowid)
        }
    }

    /// panics on out of bounds column
    pub fn column_mut(&mut self, pos: usize) -> Option<&mut Column<'alloc>> {
        self.columns.get_mut(pos)
    }
}

