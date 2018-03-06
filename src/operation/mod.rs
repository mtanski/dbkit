use super::error::DBError;
use super::allocator::Allocator;

use super::block::RefView;
use super::row::RowOffset;
use super::schema::Schema;

#[allow(dead_code)]
const DEFAULT_CURSOR_FETCH : RowOffset = 1024;

/// Next series of `Cursor` data
pub enum CursorChunk<'a> {
    /// Next chunk
    Next(RefView<'a>),
    /// End of stream
    End,

    // TODO: Next for off memory data (GPU)
}

/// Materialized operation cursor stream results from previous operations.
///
/// A cursor know it output and (optionally) input schema.
pub trait Cursor<'a> {
    fn schema(&self) -> &Schema;

    // Can't quite be an iterator, we can want different batch sizes in subsequent calls
    fn next(&'a mut self, rows: RowOffset) -> Result<CursorChunk<'a>, DBError>;
}

/// `Operation` is the basic building model of a query.
///
/// Operations are built together into a tree of Operation that represent the flow of rows from
/// one relational Operation into another.
pub trait Operation<'a> {

    /// Convert operation AST a bound Cursor
    // TODO: Tell bind if we want to shuffle GPU data or memory data
    fn bind<'b: 'a>(&self, &'b Allocator) -> Result<Box<Cursor<'a> + 'a>, DBError>;
}

pub mod project;
pub mod scan_view;

pub use self::scan_view::ScanView;
pub use self::project::Project;

