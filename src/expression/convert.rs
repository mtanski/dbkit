use std::marker::PhantomData;
use std::string::ToString;

use ::allocator::Allocator;
use ::block::{Block, View, column_row_data};
use ::error::DBError;
use ::expression::*;
use ::row::RowOffset;
use ::schema::Schema;
use ::types::*;
use ::util::copy_value::ValueSetter;

pub struct CastExpr<'b> {
    pub to: Type,
    pub input: Box<Expr<'b> + 'b>,
}

pub struct ToStr<'b> {
    pub input: Box<Expr<'b> + 'b>,
}

struct ToStrBound<'alloc, T> {
    alloc: &'alloc Allocator,
    schema: Schema, // TODO: Can this just be a static?
    pt: PhantomData<T>,
}

impl<'b> Expr<'b> for CastExpr<'b> {
    fn bind<'a: 'b>(&self, alloc: &'a Allocator, input_schema: &Schema)
        -> Result<Box<BoundExpr<'a> + 'b>, DBError>
    {
        unimplemented!()
    }
}

impl<'a> CastExpr<'a> {
    pub fn new<T: Expr<'a> + 'a>(to: Type, input: T) -> CastExpr<'a> {
        CastExpr {
            to: to,
            input: box input,
        }
    }
}

impl<'a> ToStr<'a> {
    pub fn new<T: Expr<'a> + 'a>(to: Type, input: T) -> ToStr<'a> {
        ToStr { input: box input }
    }
}

impl<'b> Expr<'b> for ToStr<'b> {
    fn bind<'a: 'b>(&self, alloc: &'a Allocator, input_schema: &Schema) ->
        Result<Box<BoundExpr<'a> + 'a>, DBError>
    {
        if input_schema.count() != 1 {
            return Err(DBError::ExpressionInputCount(format!("{} != 1", input_schema.count())))
        }

        let out_attr = input_schema.get(0)?.cast(Type::TEXT);
        let out_schema = Schema::from_attr(out_attr);

        let out: Box<BoundExpr<'a> + 'a> = match input_schema.get(0)?.dtype {
            Type::UINT32 =>
                box ToStrBound::<UInt32>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::UINT64 =>
                box ToStrBound::<UInt64>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::INT32 =>
                box ToStrBound::<Int32>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::INT64 =>
                box ToStrBound::<Int64>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::FLOAT32 =>
                box ToStrBound::<Float32>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::FLOAT64 =>
                box ToStrBound::<Float64>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::BOOLEAN =>
                box ToStrBound::<Float32>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::TEXT =>
                // TODO: Just copy
                unimplemented!(),
            Type::BLOB =>
                box ToStrBound::<Blob>{alloc: alloc, schema: out_schema, pt: PhantomData},
        };

        Ok(out)
    }
}

impl<'alloc> BoundExpr<'alloc> for ToStrBound<'alloc, Blob>
{
    fn evaluate<'a>(&self, view: &'a View<'a>, rows: RowOffset) -> Result<Block<'alloc>, DBError> {
        unimplemented!()
    }
}

impl<'alloc, T: ValueInfo, V: ToString> BoundExpr<'alloc> for ToStrBound<'alloc, T>
    where T: ValueInfo<Store=V>
{
    default fn schema(&self) -> &Schema {
        &self.schema
    }

    default fn evaluate<'a>(&self, view: &'a View<'a>, rows: RowOffset) -> Result<Block<'alloc>, DBError> {
        let mut out = Block::new(self.alloc, &self.schema);
        out.add_rows(rows)?;

        let src_col = view.column(0).unwrap();
        let src_rows = column_row_data::<T>(src_col)?;

        {
            let col = out.column_mut(0).unwrap();

            let nullable = self.schema[0].nullable;
            if !nullable {
                for idx in 0 .. rows {
                    // TODO: don't allocate
                    src_rows.values[idx].to_string()
                        .set_row(col, idx);
                }
            } else {
                // TODO: Copy null vector 1st, copy values second

                // TODO: Make sure we're not bounds checking
                for idx in 0 .. rows {
                    if src_rows.nulls[idx] != 0 {
                        NULL_VALUE.set_row(col, idx);
                    } else {
                        src_rows.values[idx].to_string()
                            .set_row(col, idx);
                    }
                }
            }
        }

        Ok(out)
    }
}

