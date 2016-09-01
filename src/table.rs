use super::allocator::{Allocator};
use super::block::*;
use super::error::DBError;
use super::schema::Schema;
use super::types::{TypeInfo};
use super::row::RowOffset;

/// Abstraction on top of a Block for easy construction and modification of contained data.
///
/// The container assumes that all operations on the block are safe and schema type conforming. In
/// case of errors it simply panics.
pub struct Table<'alloc> {
    block: Option<Block<'alloc>>,
}

impl<'alloc> View<'alloc> for Table<'alloc> {
    fn schema(&'alloc self) -> &'alloc Schema {
        self.block
            .as_ref()
            .unwrap()
            .schema()
    }

    fn column(&'alloc self, pos: usize) -> Option<&RefColumn> {
        self.block
            .as_ref()
            .unwrap()
            .column(pos)
    }

    fn rows(&self) -> RowOffset {
        self.block
            .as_ref()
            .unwrap()
            .rows()
    }
}

impl<'alloc> Table<'alloc> {
    pub fn new(alloc: &'alloc Allocator, schema: &Schema, capacity: Option<RowOffset>) -> Table<'alloc> {
        let b = Some(Block::new(alloc, schema));

        if let (Some(c), Some(mut b)) = (capacity, b) {
            b.set_capacity(c);
        }

        Table {
            block: Some(Block::new(alloc, schema))
        }
    }

    pub fn add_row(&mut self) -> Result<RowOffset, DBError> {
        self.block
            .as_mut()
            .unwrap()
            .add_row()
    }

    pub fn block_ref(&self) -> &'alloc Block {
        self.block
            .as_ref()
            .unwrap()
    }

    pub fn block_ref_mut(&mut self) -> &'alloc mut Block {
        self.block
            .as_mut()
            .unwrap()
    }

    pub fn take(&mut self) -> Option<Block<'alloc>> {
        self.block.take()
    }

    /// panics on out of bounds column
    pub fn column_mut(&mut self, pos: usize) -> Option<&mut Column<'alloc>> {
        self.block
            .as_mut()
            .unwrap()
            .column_mut(pos)
    }
}

/// TableAppender is a convenient way to programmatically build a Table/Block.
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

impl<'alloc, 't> TableAppender<'alloc, 't> {
    pub fn new(table: &'t mut Table<'alloc>) -> TableAppender<'alloc, 't> {
         TableAppender {
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
        match self.table.add_row() {
            Ok(row) => self.row = row,
            Err(e) => self.error = Some(e),
        }

        self
    }

    pub fn set_null(mut self, value: bool) -> TableAppender<'alloc, 't> {
        if self.error.is_some() {
            return self
        }

        let col = self.col;
        let row = self.row;

        self.error = self.table
            .column_mut(col)
            .ok_or(DBError::make_column_unknown_pos(col))
            .and_then(|c| c.mut_nulls())
            .and_then(|nulls| { ;nulls[row] = value as u8; Ok(()) })
            .err();

        self.col += 1;
        self
    }

    // This is a pretty ugly workaround
    pub fn set<T: TypeInfo>(mut self, value: T::Store) -> TableAppender<'alloc, 't>
    {
        if self.error.is_some() {
            return self
        }

        let col = self.col;
        let row = self.row;

        self.error = self.table
            .column_mut(col)
            .ok_or(DBError::make_column_unknown_pos(col))
            .and_then(|c| c.rows_mut::<T>())
            .and_then(|rows| { rows[row] = value; Ok(())})
            .err();

        self.col += 1;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator;
    use block::*;
    use error::DBError;
    use row::*;
    use schema::*;
    use types::*;

    // Append one row to table via TableAppender, verify that underlying block has one row.
    #[test]
    fn appender_rows()
    {
        let schema = Schema::make_one_attr("test_column", true, Type::UINT32);
        let mut table = Table::new(&allocator::GLOBAL, &schema, None);

        {
            let status = TableAppender::new(&mut table)
                .add_row().set_null(true)
                .add_row().set::<UInt32>(15)
                .done();

            assert!(status.is_none(), "Error appending rows {}", status.unwrap());
        }

        // Block exists
        assert!(table.block.is_some(), "No block inside table");

        // Schema looks correct
        assert_eq!(table.block_ref().schema().count(), 1);

        // Expected number of rows
        assert_eq!(table.block_ref().rows(), 2 as RowOffset);

        // Verify data
        let column = table.block_ref().column(0).unwrap();
        let data = column_rows::<UInt32>(column).unwrap();
        let nulls = column_nulls(column).unwrap();

        assert!(nulls[0] == 1 && nulls[1] == 0, "Null vector incorrect");
        assert_eq!(data[1], 15);
    }

    #[test]
    fn appender_end_of_row()
    {
        let schema = Schema::make_one_attr("test_column", true, Type::UINT32);
        let mut table = Table::new(&allocator::GLOBAL, &schema, None);

        let status = TableAppender::new(&mut table)
            .add_row().set_null(true).set::<UInt32>(15)
            .done();

        match status {
            Some(DBError::AttributeMissing(_)) => (), // nop
            Some(e) => assert!(false, "Unexpected error {}", e),
            None => assert!(false, "Expected error"),
        }
    }
}
