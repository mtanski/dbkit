use ::allocator::Allocator;
use ::block::{Block, View};
use ::error::DBError;
use ::schema::Schema;
use ::types::Value;
use ::row::RowOffset;

/// Single expression in a expression AST.
/// This expression has been been type checked nor materialized.
pub trait Expr<'b> {
    fn bind<'a: 'b>(&self, alloc: &'a Allocator, input_schema: &Schema)
                    -> Result<Box<BoundExpr<'a> + 'b>, DBError>;

    /// Expression can be evaluated without row data and the expression produces the same value on
    /// each invocation.
    fn is_constant(&self) -> bool {
        false
    }
}

/// Materialized expression. Input and output schema of the operation are know
///
pub trait BoundExpr<'alloc> {
    /// Output schema
    fn schema(&self) -> &Schema;

    fn evaluate<'a>(&self, view: &'a View<'a>, rows: RowOffset) -> Result<Block<'alloc>, DBError>;

    /// Parent expression can can hoist out the constant value and use it directly in the
    /// expression without generating the column. For example hoisting out a constant in a EQUALS
    /// expression.
    fn is_constant(&self) -> bool {
        false
    }

    fn evaluate_constant(&self) -> Result<Value<'alloc>, DBError> {
        Err(DBError::ExpressionNotCost)
    }
}

pub mod convert;
pub mod comparison;
// pub mod internal;
