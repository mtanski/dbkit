use super::allocator::{Allocator};
use super::block::*;
use super::error::DBError;
use super::schema::Schema;
use super::row::RowOffset;
use super::util::copy_value::ValueSetter;

/// Abstraction on top of a `Block` for easy construction and modification of contained data.
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

    /// Add a single row.
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

    /// Take ownership of the contained `Block`.
    ///
    /// This is done when the `Table` is complete and is going to be used elsewhere.
    pub fn take(&mut self) -> Option<Block<'alloc>> {
        self.block.take()
    }

    /// Get a mutable reference to the `Table`/`Block` column.
    ///
    /// panics on out of bounds column
    pub fn column_mut(&mut self, pos: usize) -> Option<&mut Column<'alloc>> {
        self.block
            .as_mut()
            .unwrap()
            .column_mut(pos)
    }

    /// Set nul value for (col, row) in the currently allocated table space.
    pub fn set_null(&mut self, col: usize, row: RowOffset, value: bool) -> Result<(), DBError> {
        if row >= self.rows() {
            return Err(DBError::RowOutOfBounds)
        }

        self.column_mut(col)
            .ok_or(DBError::make_column_unknown_pos(col))
            .and_then(|c| c.nulls_mut())
            .and_then(|nulls| { nulls[row] = value as u8; Ok(()) })
    }

    /// Set value for (col, row) in the currently allocated table space.
    pub fn set<T: ValueSetter>(&mut self, col: usize, row: RowOffset, value: T)
        -> Result<(), DBError>
    {
        if row >= self.rows() {
            return Err(DBError::RowOutOfBounds)
        }

        // TODO: Clear null value

        self.column_mut(col)
            .ok_or(DBError::make_column_unknown_pos(col))
            .and_then(|c| value.set_row(c, row))
    }
}

/// `TableAppender` is a convenient way to programmatically build a `Table`/`Block`.
///
/// `TableAppender` works on a row -> column basis. You first add a new row, then you fill up each
/// of the columns in the row until you're ready for the next row (or done).
///
/// `TableAppender` assumes that the Table owns the Block. If the Table does not own the block (eg.
/// it was been taken) then the use of `TableAppender` will result in a panic!
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

    /// Result (error) of append operation
    pub fn status(&self) -> Option<&DBError> {
        self.error.as_ref()
    }

    /// Takes the result (error) of the append operation
    pub fn done(&mut self) -> Option<DBError> {
        self.error.take()
    }

    /// Append new row
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

    /// Set column value to NUL and move onto the column to the right
    pub fn set_null(mut self, value: bool) -> TableAppender<'alloc, 't> {
        if self.error.is_some() {
            return self
        }

        self.error = self.table.set_null(self.col, self.row, value).err();
        self.col += 1;

        self
    }

    /// Set column value and move onto the column to the right
    pub fn set<T: ValueSetter>(mut self, value: T) -> TableAppender<'alloc, 't> {
        if self.error.is_some() {
            return self
        }

        self.error = self.table.set(self.col, self.row, value).err();
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
    fn appender_rows() {
        let schema = Schema::make_one_attr("test_column", true, Type::UINT32);
        let mut table = Table::new(&allocator::GLOBAL, &schema, None);

        {
            let status = TableAppender::new(&mut table)
                .add_row().set_null(true)
                .add_row().set(15 as u32)
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
        let rows = column_row_data::<UInt32>(column).unwrap();

        assert!(rows.nulls[0] == 1 && rows.nulls[1] == 0, "Null vector incorrect");
        assert_eq!(rows.values[1], 15);
    }

    #[test]
    fn appender_end_of_row() {
        let schema = Schema::make_one_attr("test_column", true, Type::UINT32);
        let mut table = Table::new(&allocator::GLOBAL, &schema, None);

        let status = TableAppender::new(&mut table)
            .add_row().set_null(true).set(15 as u32)
            .done();

        match status {
            Some(DBError::AttributeMissing(_)) => (), // nop
            Some(e) => assert!(false, "Unexpected error {}", e),
            None => assert!(false, "Expected error"),
        }
    }

    #[test]
    fn varlen_columns() {
        let bytes: [u8; 5] = [0, 1, 2, 3, 4];

        let table = {
            let attrs = vec![
                Attribute{name: "one".to_string(), nullable: false, dtype: Type::BLOB},
                Attribute{name: "two".to_string(), nullable: false, dtype: Type::TEXT},
            ];

            let schema = Schema::from_vec(attrs).unwrap();
            let mut table = Table::new(&allocator::GLOBAL, &schema, None);

            {

                let status = TableAppender::new(&mut table)
                    .add_row()
                        .set(bytes.as_ref())
                        .set("one")
                    .add_row()
                        .set(bytes.as_ref())
                        .set("two".to_string())
                    .done();

                assert!(status.is_none(), "Error appending rows {}", status.unwrap());
            }

            table
        };

        {
            let col0 = table.block_ref().column(0).unwrap();
            let rows = column_row_data::<Blob>(col0).unwrap();
            assert_eq!(rows.values[0].as_ref(), bytes);
            assert_eq!(rows.values[1].as_ref(), bytes);
        }

        {
            let col1 = table.block_ref().column(1).unwrap();
            let rows = column_row_data::<Text>(col1).unwrap();
            assert_eq!(rows.values[0].as_ref() as &str, "one");
            assert_eq!(rows.values[1].to_string(), String::from("two"));
        }
    }
}
