use std::marker::PhantomData;
use std::fmt::Display;

use ::allocator::Allocator;
use ::block::{Block, View, RefColumn, column_row_data};
use ::error::DBError;
use ::expression::*;
use ::row::RowOffset;
use ::schema::{Attribute, Schema};
use ::types::*;
use ::util::copy_value::ValueSetter;

pub struct CastExpression<'b> {
    pub to: Type,
    pub input: Box<Expression<'b> + 'b>,
}

pub struct ToString<'b> {
    pub input: Box<Expression<'b> + 'b>,
}

struct ToStringBound<'alloc, T: ValueInfo> {
    alloc: &'alloc Allocator,
    schema: Schema, // TODO: Can this just be a static?
    pt: PhantomData<T>,
}

impl<'b> Expression<'b> for CastExpression<'b> {
    fn bind<'a: 'b>(&self, alloc: &'a Allocator, input_schema: &Schema)
        -> Result<Box<BoundExpression<'a> + 'b>, DBError>
    {
        unimplemented!()
    }
}

impl<'a> CastExpression<'a> {
    pub fn new<T: Expression<'a> + 'a>(to: Type, input: T) -> CastExpression<'a> {
        CastExpression {
            to: to,
            input: box input,
        }
    }
}

impl<'a> ToString<'a> {
    pub fn new<T: Expression<'a> + 'a>(to: Type, input: T) -> ToString<'a> {
        ToString { input: box input }
    }
}

impl<'b> Expression<'b> for ToString<'b> {
    fn bind<'a: 'b>(&self, alloc: &'a Allocator, input_schema: &Schema) ->
        Result<Box<BoundExpression<'a> + 'a>, DBError>
    {
        if input_schema.count() != 1 {
            return Err(DBError::ExpressionInputCount(format!("{} != 1", input_schema.count())))
        }

        let out_attr = input_schema.get(0)?.cast(Type::TEXT);
        let out_schema = Schema::from_attr(out_attr);

        let out: Box<BoundExpression<'a> + 'a> = match input_schema.get(0)?.dtype {
            Type::UINT32 =>
                box ToStringBound::<UInt32>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::UINT64 =>
                box ToStringBound::<UInt64>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::INT32 =>
                box ToStringBound::<Int32>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::INT64 =>
                box ToStringBound::<Int64>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::FLOAT32 =>
                box ToStringBound::<Float32>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::FLOAT64 =>
                box ToStringBound::<Float64>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::BOOLEAN =>
                box ToStringBound::<Float32>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::TEXT =>
                // TODO: Just copy
                unimplemented!(),
                // box ToStringBound::<Text>{alloc: alloc, schema: out_schema, pt: PhantomData},
            Type::BLOB =>
                unimplemented!(),
                // box ToStringBound::<Blob>{alloc: alloc, schema: out_schema, pt: PhantomData},
        };

        Ok(out)
    }
}

impl<'alloc, T> BoundExpression<'alloc> for ToStringBound<'alloc, T>
    where T: ValueInfo, T::Store: Display
{
    default fn evaluate<'a>(&self, view: &'a View<'a>, rows: RowOffset) -> Result<Block<'alloc>, DBError> {
        let mut out = Block::new(self.alloc, &self.schema);
        out.add_rows(rows)?;

        let src_col = view.column(0).unwrap();
        let src_rows = column_row_data::<T>(src_col)?;

        {
            let col = out.column_mut(0).unwrap();

            let nullable = self.schema[0].nullable;
            if !nullable {
                for idx in 0 ... rows {
                    src_rows.values[idx].to_string()
                        .set_row(col, idx);
                }
            } else {
                // TODO
            }
        }

        Ok(out)
    }

    default fn schema(&self) -> &Schema {
        &self.schema
    }
}

/* TODO: Specialization not working?

impl<'alloc> BoundExpression<'alloc> for ToStringBound<'alloc, Blob>
{
    fn evaluate(&self, view: &View, rows: RowOffset) -> Result<Block<'alloc>, DBError> {
        unimplemented!()
    }
}

*/