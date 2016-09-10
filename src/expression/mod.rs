use std::convert::Into;

use ::allocator::Allocator;
use ::error::DBError;
use ::types::{Type, Value};

pub trait BoundExpression {

}

pub trait Expression<'a> {
    fn bind<'b: 'a>(&self, alloc: &'a Allocator) -> Result<Box<BoundExpression + 'a>, DBError>;
}

pub struct GenericConstValue<'a> {
    pub value: Value<'a>
}

pub struct CastExpression<'a> {
    pub to: Type,
    pub input: Box<Expression<'a> + 'a>,
}

impl<'a> GenericConstValue<'a>
{
    pub fn new<T>(from: T) -> GenericConstValue<'a>
        where T: Into<Value<'a>>
    {
        GenericConstValue{ value: from.into() }
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

