use ::block::Column;
use ::error::DBError;
use ::row::RowOffset;
use types::*;

/// Trait for setting column row values from rust native types.
/// Deals correctly with types that need to store data in the column's arena.
pub trait ValueSetter {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError>;
}

impl ValueSetter for NullType {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.nulls_mut()?;
        rows[row] = true as u8;
        Ok(())
    }
}

impl ValueSetter for u32 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<UInt32>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for u64 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<UInt64>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for i32 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<Int32>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for i64 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<Int64>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for f32 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<Float32>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for f64 {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<Float64>()?;
        rows[row] = *self;
        Ok(())
    }
}

impl ValueSetter for bool {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let rows = col.rows_mut::<Boolean>()?;
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

        let rows = col.rows_mut::<Text>()?;
        rows[row] = RawData{data: ptr, size: data.len()};
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

        let rows = col.rows_mut::<Text>()?;
        rows[row] = RawData{data: ptr, size: data.len()};
        Ok(())
    }
}

impl<'b> ValueSetter for &'b[u8] {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        let ptr = {
            let arena = col.arena();
            arena.append(self)?.1
        };

        let rows = col.rows_mut::<Blob>()?;
        rows[row] = RawData{data: ptr, size: self.len()};
        Ok(())
    }
}

/// Optional container. If none, then it'll be NULL
impl<T> ValueSetter for Option<T>
    where T: ValueSetter
{
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        match self {
            None    => NULL_VALUE.set_row(col, row),
            Some(v) => v.set_row(col, row),
        }
    }
}

/// Value setter for arbitrary type stored in types::Value
impl<'b> ValueSetter for Value<'b> {
    fn set_row<'a>(&self, col: &mut Column<'a>, row: RowOffset) -> Result<(), DBError> {
        match self {
            Value::NULL         => NULL_VALUE.set_row(col, row),
            Value::UINT32(v)    => v.set_row(col, row),
            Value::UINT64(v)    => v.set_row(col, row),
            Value::INT32(v)     => v.set_row(col, row),
            Value::INT64(v)     => v.set_row(col, row),
            Value::FLOAT32(v)   => v.set_row(col, row),
            Value::FLOAT64(v)   => v.set_row(col, row),
            Value::BOOLEAN(v)   => v.set_row(col, row),
            Value::TEXT(&v)     => v.set_row(col, row),
            Value::BLOB(&v)     => v.set_row(col, row),
        }
    }
}

// TODO: Make a value alias... we can set a value but without copying the data in the arena.
// Clearly unsafe, but useful for things like join with Tiny... where it's always alive.
