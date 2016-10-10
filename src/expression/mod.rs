use std::convert::Into;

use ::allocator::Allocator;
use ::block::{Block, View};
use ::error::DBError;
use ::schema::Schema;
use ::types::{Type, Value};
use ::row::RowOffset;

/// Materialized expression. Input and output schema of the operation are know
///
pub trait BoundExpression<'alloc> {
    /// Output schema
    fn schema(&self) -> &Schema;

    fn evaluate<'a>(&self, view: &'a View<'a>, rows: RowOffset) -> Result<Block<'alloc>, DBError>;

    /// Parent expression can can hoist out the constant value and use it directly in the
    /// expression without generating the column. For example hoisting out a constant in a EQUALS
    /// expression.
    fn is_constant(&self) -> bool {
        false
    }
}

/// Expression    checked that hasn't been 
/// 
pub trait Expression<'b> {
    fn bind<'a: 'b>(&self, alloc: &'a Allocator, input_schema: &Schema)
        -> Result<Box<BoundExpression<'a> + 'b>, DBError>;
}

pub struct GenericConstValue<'a> {
    pub value: Value<'a>
}

pub struct BoundConstValue<'a> {
    pub dtype: Type,
    pub value: Value<'a>
}

impl<'a> GenericConstValue<'a>
{
    pub fn new<T>(from: T) -> GenericConstValue<'a>
        where T: Into<Value<'a>>
    {
        GenericConstValue{ value: from.into() }
    }
}

pub mod convert;
