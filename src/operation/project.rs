use std::mem::replace;

use ::allocator::Allocator;
use ::block::RefView;
use ::error::DBError;
use ::row::RowOffset;
use ::schema::Schema;

use ::projector::*;

use super::{Operation, Cursor, CursorChunk};

/// Relational Project Operation
pub struct Project<'a> {
    pub src: Box<Operation<'a> + 'a>,
    pub proj: SingleSourceProjector,
}

/// Implementation of the `Project` operation
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
    fn bind<'b: 'a>(&self, alloc: &'b Allocator) -> Result<Box<Cursor<'a> + 'a>, DBError> {
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


#[cfg(test)]
mod tests {
    use super::*;
    use ::allocator;
    use ::schema::{Attribute, Schema};
    use ::operation::{Operation, ScanView};
    use ::projector::*;
    use ::table::{Table, TableAppender};
    use ::types::*;

    #[test]
    fn reorder_columns() {
        let block = {
            let attrs = vec![
                Attribute{name: "one".to_string(), nullable: false, dtype: Type::UINT32},
                Attribute{name: "two".to_string(), nullable: false, dtype: Type::UINT32},
                Attribute{name: "three".to_string(), nullable: false, dtype: Type::UINT32},
            ];

            let schema = Schema::from_vec(attrs).unwrap();
            let mut table = Table::new(&allocator::GLOBAL, &schema, None);

            {
                let status = TableAppender::new(&mut table)
                    .add_row()
                        .set(0 as u32)
                        .set(1 as u32)
                        .set(13 as u32)
                    .done();

                assert!(status.is_none(), "Error appending rows {}", status.unwrap());
            }

            table.take()
        };

        let proj = BuildSingleSourceProjector::new()
            .add_as(project_by_position(2), "new_one")
            .add(project_by_name("two"))
            .done();

        {
            let scan_op = ScanView::new(block.as_ref().unwrap(), None);
            let proj_op = Project::new(proj, scan_op);

            let cursor = proj_op.bind(&allocator::GLOBAL).unwrap();
            let cursor_ref = &*cursor;

            // Columns correctly re-arranged
            let cursor_schema = cursor_ref.schema();
            assert_eq!(cursor_schema.get(0).unwrap().name, "new_one", "Bad cursor schema");
            assert_eq!(cursor_schema.get(1).unwrap().name, "two", "Bad cursor schema");
        }
    }
}
