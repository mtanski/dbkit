use std::cmp::Eq;
use std::marker::PhantomData;

use ::expression::*;
use ::error::DBError;
use ::types::ValueInfo;

pub struct EqaulsExpr<'a> {
    pub lhs: Box<Expr<'a> + 'a>,
    pub rhs: Box<Expr<'a> + 'a>,
}

struct EqualsBound<'a, T: 'a + ValueInfo> {
    alloc: &'a Allocator,
    schema: Schema, // TODO: Can this just be a static?
    phantom: PhantomData<&'a T>,
}

impl<'a> EqaulsExpr<'a> {
    pub fn new<T: Expr<'a> + 'a>(lhs: T, rhs: T) -> EqaulsExpr<'a> {
        EqaulsExpr { lhs: box lhs, rhs: box rhs }
    }
}

impl<'b> Expr<'b> for EqaulsExpr<'b> {
    fn bind <'a: 'b> (&self, alloc: &'a Allocator, input_schema: &Schema) ->
        Result <Box<BoundExpr<'a> + 'a>, DBError>
    {
        Err(DBError::Unknown)
    }
}

impl<'alloc, T: ValueInfo, V: Eq> BoundExpr<'alloc> for EqualsBound<'alloc, T>
    where T: ValueInfo<Store=V>
{
    default fn schema(&self) -> &Schema {
        &self.schema
    }

    fn evaluate<'a>(&self, view: &'a View<'a>, rows: RowOffset) -> Result<Block<'alloc>, DBError> {
        let mut out = Block::new(self.alloc, &self.schema);

        Ok(out)
    }
}

