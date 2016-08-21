// vim : set ts=4 sw=4 et :

use super::allocator::{Allocator, RawChunk};
use super::types::{Type, TypeInfo};
use super::schema::{Attribute, Schema};
use super::error::DBError;

use std::io;
use std::ptr;
use std::mem;

type BoolBitmap<'a> = &'a [u8];
type MutBoolBitmap<'a> = &'a mut [u8];
type RowOffset = usize;


trait Column<'a> {
    fn attribute(&self) -> &Attribute;
    fn nulls(&self) -> Option<BoolBitmap>;
    fn rows<T: TypeInfo>(&self) -> Result<&'a [T::Store], DBError> ;
}

struct OwnedColumn<'a> {
    attr: Attribute,
    raw_nulls: Option<RawChunk<'a>>,
    raw: Option<RawChunk<'a>>,
}

fn emptyRows<T: TypeInfo>(a: Attribute) -> Result<&'static mut [T::Store], DBError> {
    match a.dtype {
        T::ENUM => Ok([T::Store, 0]),
        _       => Err(DBError::AttributeType(a.name)),
    }
}

impl<'a> OwnedColumn<'a> {
    fn new(a: &mut Allocator, attr: Attribute) -> OwnedColumn {
        OwnedColumn {
            attr: attr,
            raw_nulls: None,
            raw: None,
        }
    }

    fn mut_nulls(&mut self) -> Result<MutBoolBitmap, DBError> {
        if !self.attr.nullable {
            return Err(DBError::AttributeNullability(self.attr.name))
        }

        self.raw_nulls
            .map(|rc| -> MutBoolBitmap { unsafe { mem::transmute(rc) }})
            .ok_or(DBError::Unknown)
    }

    fn mut_rows<T: TypeInfo>(&mut self) -> Result<&'a mut [T::Store], DBError> {
        match self.raw {
            None    => emptyRows::<T>(self.attr),
        }
    }

    unsafe fn raw_data(&mut self) -> *mut u8 {
        match self.data {
            None    => ptr::null_mut(),
            Some(c) => c.raw,
        }
    }
}

impl<'a> Column<'a> for OwnedColumn<'a> {
    fn attribute(&self) -> &Attribute {
        &self.attr
    }

    fn nulls(&self) -> Option<BoolBitmap> {
        match self.attr.nullable {
            false => None,
            true  => None
        }
    }

    fn rows<T: TypeInfo>(&self) -> Result<&'a mut [T::Store], DBError>  {
        match self.raw {
            None    => emptyRows::<T>(self.attr),
        }
    }
}

trait View {
    fn schema(&self) -> &Schema;
    fn column(&self, pos: usize) -> Option<&Column>;
    fn rows(&self) -> RowOffset;
}

struct Block<'a> {
    schema: Schema,
    columns: Vec<OwnedColumn<'a>>,
    rows: RowOffset,
    capacity: RowOffset,
}

impl<'a> View for Block<'a> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn column(&self, pos: usize) -> Option<&Column> {
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

impl<'a> Block<'a> {
    pub fn new(schema: &Schema) -> Block {
        Block {
            schema: schema.clone(),
            columns: Block::make_columns(schema),
            rows: 0,
            capacity: 0,
        }
    }

    pub fn capacity(&self) -> RowOffset {
        self.capacity
    }

    pub fn set_capacity(&mut self, size: RowOffset) -> Option<io::Error> {
        if size > self.capacity {
            for col in self.columns.iter_mut() {
                col.nulls.resize(size, 0)
            }
            None
        } else if size < self.capacity {
            None
        } else {
            None
        }
    }

    pub fn mut_column(&mut self, pos: usize) -> Option<&mut OwnedColumn> {
        if pos < self.columns.len() {
            Some(&mut self.columns[pos])
        } else {
            None
        }
    }

    fn make_columns(schema: &Schema) -> Vec<OwnedColumn> {
        schema.iter()
            .map(|attr| OwnedColumn::new(attr))
            .collect()
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
}

impl<'a> Drop for Block<'a> {
    fn drop(&mut self) {

    }
}

struct Table<'a> {
    block: Option<Block<'a>>,
}

impl<'a> View for Table<'a> {
    fn schema(&self) -> &Schema {
        self.block.as_ref().map(|b| b.schema()).unwrap()
    }

    fn column(&self, pos: usize) -> Option<&Column> {
        self.block.as_ref().and_then(|b| b.column(pos))
    }

    fn rows(&self) -> RowOffset {
        self.block.as_ref().map(|b| b.rows()).unwrap()
    }
}

impl<'a> Table<'a> {
    pub fn new(schema: &Schema, capacity: Option<RowOffset>) -> Table {
        Table {
            block: Some(Block::new(schema))
        }
    }

    pub fn add_row(&mut self) -> Result<RowOffset, DBError> {
        let block = match self.block {
            Some(ref mut b) => b,
            None => panic!("Attempting to add a row to non-existing block")
        };

        block.expand().ok_or(DBError::Unknown)
    }

    pub fn block(&self) -> Option<&Block> {
        self.block.as_ref()
    }

    pub fn mut_block(&mut self) -> Option<&mut Block> {
        self.block.as_mut()
    }

    pub fn take(&mut self) -> Option<Block> {
        self.block.take()
    }
}

/// TableAppender is a convenient way to pragmatically build a Table/Block.
///
/// TableAppender assumes that the Table owns the Block. If the Table does not own the block (eg.
/// it was been taken) then the use of TableAppender will result in a panic!
struct TableAppender<'a> {
    table: &'a mut Table<'a>,
    // Start row (when we started appending to)
    start: RowOffset,
    // Current row offset
    row: RowOffset,
    // Current column offset
    col: usize,
    error: Option<DBError>,
}

impl<'a> TableAppender<'a> {
    pub fn new(table: &'a mut Table) -> TableAppender<'a> {
        return TableAppender {
            start: table.rows(),
            row: table.rows(),
            table: table,
            col: 0,
            error: None,
        };
    }

    pub fn add_row(&mut self) -> &mut Self {
        if self.error.is_some() {
            return self;
        }

        self.col = 0;
        // Panics if this failed
        self.row = self.table.add_row().unwrap();

        self
    }

    pub fn set_null(&mut self, value: bool) -> &mut Self {
        let mut error: Option<DBError> = None;

        {
            let row = self.row;
            let col = self.next_column().unwrap();

            let attr = &col.attr;

            if attr.nullable {
                col.nulls[row] = value as u8
            } else {
                error = Some(DBError::makeColumnNotNullable(attr.name.clone()))
            }
        }

        self.error = self.error.take().or(error);
        self
    }

    pub fn set_u32(&mut self, value: u32) -> &mut Self {
        self
    }

    /// Result of append operation
    pub fn status(&self) -> Option<&DBError> {
        self.error.as_ref()
    }

    /// Short hand
    fn next_column(&mut self) -> Option<&mut OwnedColumn> {
        if self.error.is_some() {
            return None;
        }

        // Will panic! if this is null
        let block = self.table.mut_block().unwrap();

        let col = block.mut_column(self.col);

        if col.is_none() {
            self.error = Some(DBError::AttributeMissing(format!("(pos: {})", self.col)));
            None
        } else {
            self.col += 1;
            col
        }
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



