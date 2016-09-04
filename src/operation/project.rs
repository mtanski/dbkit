use std::mem::replace;

use ::allocator::Allocator;
use ::block::RefView;
use ::error::DBError;
use ::row::RowOffset;
use ::schema::Schema;

use ::projector::*;

use super::{Operation, Cursor, CursorChunk};

pub struct Project<'a> {
    pub src: Box<Operation<'a> + 'a>,
    pub proj: SingleSourceProjector,
}

struct ProjectCursor<'a> {
    input: Box<Cursor<'a> + 'a>,
    proj: BoundProjector,
    _next: RefView<'a>,
}

impl<'a> Project<'a> {
    pub fn new<T: Operation<'a> + 'a>(proj: SingleSourceProjector, src: T) -> Project<'a> {
        Project { src: box src, proj: proj }
    }
}

impl<'a> Operation<'a> for Project<'a> {
    fn bind<'b: 'a>(&'a self, alloc: &'b Allocator) -> Result<Box<Cursor<'a> + 'a>, DBError> {
        let boxed = self.src.bind(alloc)?;

        let proj = {
            let cursor = &*boxed;
            let schema = cursor.schema();
            self.proj.bind(schema)?
        };

        let out = Box::new(ProjectCursor {input: boxed, proj: proj, _next: Default::default()});
        Ok(out)
    }
}

impl<'a> Cursor<'a> for ProjectCursor<'a> {
    fn schema(&self) -> &Schema {
        &self.proj.schema
    }

    fn next(&'a mut self, rows: RowOffset) -> Result<CursorChunk<'a>, DBError> {
        let next_chunk = self.input.as_mut()
            .next(rows)?;

        if let CursorChunk::Next(src) = next_chunk {
            replace(&mut self._next, src);
            self.proj.project_view(&self._next)
                .map(|v| CursorChunk::Next(v))
        } else {
            Ok(next_chunk)
        }
    }
}

