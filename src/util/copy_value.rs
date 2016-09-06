use ::block::Column;
use ::error::DBError;
use ::row::RowOffset;
use ::types;

pub trait ValueSetter {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError>;
}

impl ValueSetter for u32 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<types::UInt32>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for u64 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<types::UInt64>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for i32 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<types::Int32>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for i64 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<types::Int64>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl<'b> ValueSetter for &'b str {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let data = self.as_bytes();
        let ptr = {
            let arena = col.arena();
            arena.append(data)?.1
        };

        let rows = col.rows_mut::<types::Text>()?;
        rows[row] = types::RawData{data: ptr, size: data.len()};
        Ok(())
    }
}

impl ValueSetter for String {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let data = self.as_bytes();
        let ptr = {
            let arena = col.arena();
            arena.append(data)?.1
        };

        let rows = col.rows_mut::<types::Text>()?;
        rows[row] = types::RawData{data: ptr, size: data.len()};
        Ok(())
    }
}

impl<'b> ValueSetter for &'b[u8] {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let ptr = {
            let arena = col.arena();
            arena.append(self)?.1
        };

        let rows = col.rows_mut::<types::Blob>()?;
        rows[row] = types::RawData{data: ptr, size: self.len()};
        Ok(())
    }
}
