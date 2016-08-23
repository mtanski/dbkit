
use super::allocator::{self, Allocator};
use super::block::*;
use super::error::DBError;
use super::schema::Schema;
use super::types::Type;

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
pub struct TableAppender<'alloc: 't, 't> {
    table: &'t mut Table<'alloc>,
    // Current row offset
    row: RowOffset,
    // Current column offset
    col: usize,
    error: Option<DBError>,
}

impl<'alloc: 't, 't> TableAppender<'alloc, 't> {
    pub fn new(table: &'t mut Table<'alloc>) -> TableAppender<'alloc, 't> {
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

    pub fn add_row(mut self) -> TableAppender<'alloc, 't> {
        if self.error.is_some() {
            return self;
        }

        self.col = 0;
        // Panics if this failed
        self.row = self.table.add_row().unwrap();

        self
    }

    pub fn set_null(mut self, value: bool) -> TableAppender<'alloc, 't> {
        if self.error.is_some() {
            return self
        }

        fn is_nullable<'a>(c: &'a mut Column<'a>) -> Result<&mut Column<'a>, DBError> {
            match c.attribute().nullable {
                true => Ok(c),
                _ => Err(DBError::makeColumnNotNullable(c.attribute().name.clone())),
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

    pub fn set_u32(mut self, value: u32) -> TableAppender<'alloc, 't> {
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
    let alloc = allocator::HeapAllocator::global();
    let schema = Schema::make_one_attr("test_column", true, Type::UINT32);
    let mut table = Table::new(alloc, &schema, None);

    {
        let mut appender = TableAppender::new(&mut table);

        let status = appender
            .add_row()
            .set_null(true)
            .done();

        assert!(status.is_none(), "{}", status.unwrap());
    }

    match table.take() {
        Some(block) => assert_eq!(block.rows(), 1 as RowOffset),
        None => panic!("No block inside the table"),
    };
}

