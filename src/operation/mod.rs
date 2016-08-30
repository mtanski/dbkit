
use super::error::DBError;
use super::allocator::Allocator;
use super::cursor::Cursor;

use super::block::View;

pub trait Operation {
    // Convert operation AST a bound Cursor
    fn bind(&self, &mut Allocator) -> Result<&Cursor, DBError>;
}

