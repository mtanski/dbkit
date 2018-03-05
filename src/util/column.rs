use std::marker::PhantomData;
use std::default::Default;

use ::block::{Column, RefColumn, column_row_data};
use ::error::DBError;
use ::row::{RowOffset, RowRange};
use ::types::*;
use ::util::copy_value::ValueSetter;

pub trait OneColMapper<IT: TypeInfo, OT: TypeInfo> {
    /// Map non-null column to another value
    fn map_value(&self, in_val: &IT::Store) -> OT::Store;

    /// Map null, or not null column to another value
    ///
    /// Default implementation passed along nulls, and calls map_value in non-null case
    fn map(&self, in_val: Option<&IT::Store>) -> Option<OT::Store> {
        in_val.map(|v| self.map_value(v))
    }
}

pub struct BoundOneColMapper<IT: TypeInfo, IN: Nullability, OT: TypeInfo, ON: Nullability> {
    _it: PhantomData<IT>,
    _in: PhantomData<IN>,
    _ot: PhantomData<OT>,
    _on: PhantomData<ON>,
}

impl<IT: TypeInfo, IN: Nullability, OT: TypeInfo, ON: Nullability>
    BoundOneColMapper<IT, IN, OT, ON>
    where <OT as TypeInfo>::Store: Default
{
    pub fn new() -> BoundOneColMapper<IT, IN, OT, ON> {
        BoundOneColMapper {
            _it: PhantomData,
            _in: PhantomData,
            _ot: PhantomData,
            _on: PhantomData
        }
    }

    pub fn map(&self, in_col: &RefColumn, out_col: &mut Column, mapper: &OneColMapper<IT, OT>)
        -> Result<RowOffset, DBError>
    {
        let src_rows = column_row_data::<IT>(in_col)?;
        let out_rows = out_col.row_data_mut::<OT>()?;

        if IN::NULLABLE {
            for idx in 0..src_rows.values.len() {
                let in_val= match src_rows.nulls[idx] {
                    0 => Some(&src_rows.values[idx]),
                    _ => None,
                };

                let out_val = mapper.map(in_val);
                let null = out_val.is_some() as u8;

                if ON::NULLABLE {
                    out_rows.nulls[idx] = null;
                }

                out_rows.values[idx] = out_val.unwrap_or(Default::default());
            }
        } else {
            for idx in 0..src_rows.values.len() {
                let in_val = Some(&src_rows.values[idx]);
                let out_val = mapper.map(in_val);
                let null = out_val.is_some() as u8;

                if ON::NULLABLE {
                    out_rows.nulls[idx] = null;
                }

                out_rows.values[idx] = out_val.unwrap_or(Default::default());
            }
        }

        Ok(src_rows.values.len())
    }
}
