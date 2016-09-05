use std::cmp::min;

use ::allocator::Allocator;
use ::block::{RefView, View, window_alias};
use ::error::DBError;
use ::row::{RowRange, RowOffset};
use ::schema::Schema;

use super::{Operation, Cursor, CursorChunk};

/// Operation that takes an "external" view and uses it as a source
pub struct ScanView<'a> {
    pub src: &'a View<'a>,
    pub range: Option<RowRange>,
}

impl<'a> ScanView<'a> {
    pub fn new(src: &'a View<'a>, range: Option<RowRange>) -> ScanView<'a> {
        ScanView { src: src, range: range }
    }
}

impl<'a> Operation<'a> for ScanView<'a> {
    fn bind<'b: 'a>(&self, _: &'b Allocator) -> Result<Box<Cursor<'a> + 'a>, DBError> {
        let sub = window_alias(self.src, self.range)?;
        let out = Box::new(ScanViewCursor { src: sub, offset: 0 });
        Ok(out)
    }
}

struct ScanViewCursor<'a> {
    /// This view is already sub
    src: RefView<'a>,
    offset: RowOffset,
}

impl<'a> Cursor<'a> for ScanViewCursor<'a> {
    fn schema(&self) -> &Schema {
        self.src.schema()
    }

    fn next(&'a mut self, rows: RowOffset) -> Result<CursorChunk<'a>, DBError> {
        let left = self.src.rows() - self.offset;

        if left == 0 {
            return Ok(CursorChunk::End)
        }

        let range = RowRange { offset: self.offset, rows: min(left, rows) };
        let sub = window_alias(&self.src, Some(range))?;

        self.offset += range.rows;
        Ok(CursorChunk::Next(sub))
    }
}
