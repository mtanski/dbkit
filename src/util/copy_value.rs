
use ::block::Column;
use ::error::DBError;
use ::types;
use ::allocator::ChainedArena;
use ::row::RowOffset;

pub trait ValueSetter {
    fn set_row(self, col: &mut Column, row: RowOffset, deep: bool) -> Result<(), DBError>;
}

impl ValueSetter for u32 {
    fn set_row(self, col: &mut Column, row: RowOffset, deep: bool) -> Result<(), DBError> {
        let rows = col.rows_mut::<types::UInt32>()?;
        rows[row] = self;
        Ok(())
    }
}


