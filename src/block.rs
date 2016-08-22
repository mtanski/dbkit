// vim : set ts=4 sw=4 et :

use std::mem;
use std::slice;


use super::allocator::{Allocator, RawChunk};
use super::types::{self, Type, TypeInfo};
use super::schema::{Attribute, Schema};
use super::error::DBError;

type BoolBitmap<'a> = &'a [u8];
type MutBoolBitmap<'a> = &'a mut [u8];
type RowOffset = usize;


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

    fn attribute(&self) -> &Attribute {
        &self.attr
    }

    fn nulls(&self) -> Result<BoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name.clone()))
        }

        unsafe {
            return Ok(slice::from_raw_parts(self.raw_nulls.data, self.raw_nulls.size));
        }
    }

    fn mut_nulls(&mut self) -> Result<MutBoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name.clone()))
        }

        unsafe {
            return Ok(slice::from_raw_parts_mut(self.raw_nulls.data, self.raw_nulls.size));
        }
    }

    fn rows<T: TypeInfo>(&self) -> Result<&[T::Store], DBError>  {
        if self.attr.dtype != T::ENUM {
            return Err(DBError::AttributeType(self.attr.name.clone()))
        }

        unsafe {
            let ptr: *const T::Store = mem::transmute(self.raw.data);
            return Ok(slice::from_raw_parts(ptr, self.raw.size));
        }
    }

    fn mut_rows<T: TypeInfo>(&mut self) -> Result<&mut [T::Store], DBError> {
        if self.attr.dtype != T::ENUM {
            return Err(DBError::AttributeType(self.attr.name.clone()))
        }

        unsafe {
            let ptr: *mut T::Store = mem::transmute(self.raw.data);
            return Ok(slice::from_raw_parts_mut(ptr, self.raw.size));
        }
    }

    unsafe fn raw_data(&mut self) -> *mut u8 {
        self.raw.data
    }
}

trait View<'v> {
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

pub struct Table<'alloc> {
    block: Option<Block<'alloc>>,
}

impl<'alloc> View<'alloc> for Table<'alloc> {
    fn schema(&'alloc self) -> &'alloc Schema {
        self.block.as_ref().unwrap().schema()
    }

    fn column(&'alloc self, pos: usize) -> Option<&'alloc Column> {
        self.block.as_ref().unwrap().column(pos)
    }

    fn rows(&self) -> RowOffset {
        self.block.as_ref().unwrap().rows()
    }
}

impl<'alloc> Table<'alloc> {
    pub fn new(alloc: &'alloc mut Allocator, schema: &Schema, capacity: Option<RowOffset>) -> Table<'alloc> {
        Table {
            block: Some(Block::new(alloc, schema))
        }
    }

    pub fn add_row(&mut self) -> Result<RowOffset, DBError> {
        self.block.as_mut().unwrap().expand().ok_or(DBError::Unknown)
    }

    pub fn block_ref(&self) -> &Block<'alloc> {
        self.block.as_ref().unwrap()
    }

    pub fn block_ref_mut(&mut self) -> &'alloc mut Block {
        self.block.as_mut().unwrap()
    }

    pub fn take(&mut self) -> Option<Block<'alloc>> {
        self.block.take()
    }

    /// panics on out of bounds column
    pub fn column_mut(&mut self, pos: usize) -> Option<&mut Column<'alloc>> {
        self.block.as_mut().unwrap().column_mut(pos)
    }
}

/// TableAppender is a convenient way to pragmatically build a Table/Block.
///
/// TableAppender assumes that the Table owns the Block. If the Table does not own the block (eg.
/// it was been taken) then the use of TableAppender will result in a panic!
pub struct TableAppender<'a> {
    table: &'a mut Table<'a>,
    // Current row offset
    row: RowOffset,
    // Current column offset
    col: usize,
    error: Option<DBError>,
}

impl<'a> TableAppender<'a> {
    pub fn new(table: &'a mut Table<'a>) -> TableAppender<'a> {
        return TableAppender {
            row: table.rows(),
            table: table,
            col: 0,
            error: None,
        }
    }

    /// Result of append operation
    pub fn status(&self) -> Option<&DBError> {
        self.error.as_ref()
    }

    pub fn done(&mut self) -> Option<DBError> {
        self.error.take()
    }

    pub fn add_row(mut self) -> TableAppender<'a> {
        if self.error.is_some() {
            return self;
        }

        self.col = 0;
        // Panics if this failed
        self.row = self.table.add_row().unwrap();

        self
    }

    pub fn set_null(mut self, value: bool) -> TableAppender<'a> {
        if self.error.is_some() {
            return self
        }

        fn is_nullable<'a>(c: &'a mut Column<'a>) -> Result<&mut Column<'a>, DBError> {
            match c.attr.nullable {
                true => Ok(c),
                _ => Err(DBError::makeColumnNotNullable(c.attr.name.clone())),
            }
        }

        let col = self.col;
        let row = self.row;

        let err = self.table
            .column_mut(col)
            .ok_or(DBError::makeColumnUnknownPos(col))
            .and_then(|c| c.mut_nulls())
            .and_then(|nulls| { nulls[row] = value as u8; Ok(()) })
            .err();

        self.error = err;
        self.col += 1;
        return self
    }

    pub fn set_u32(mut self, value: u32) -> TableAppender<'a> {
        if self.error.is_some() {
            return self
        }

	// TODO: 
	self
    }
}

// Append one row to table via TableAppender, verify that underlying block has one row.
#[test]
fn appender_row()
{
    let schema = Schema::make_one_attr("test_column", true, Type::UINT32);
    let mut table = Table::new(&schema, None);

    {
        let mut appender = TableAppender::new(&mut table);

        let status = appender
            .add_row()
                .set_null(true)
            .status();

        assert!(status.is_none(), "{}", status.unwrap());
    }

    match table.take() {
        Some(block) => assert_eq!(block.rows(), 1 as RowOffset),
        None => panic!("No block inside the table"),
    };
}



