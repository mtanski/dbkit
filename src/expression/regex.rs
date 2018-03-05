extern crate regex;

use std::marker::PhantomData;

use ::block::{Block, View};
use ::expression::*;
use ::types::*;
use ::util::column;

pub struct RegexMatch<'a> {
    pub input: Box<Expr<'a> + 'a>,
    re: regex::Regex,
}

pub struct BoundRegexMatch<'a, NULL: Nullability> {
    re: &'a regex::Regex,
    alloc: &'a Allocator,
    schema: Schema,
    nullability: PhantomData<NULL>,
}

pub struct RegexMatchMapper<'a> {
    re: &'a regex::Regex,
}

impl<'a> column::OneColMapper<Text, Boolean> for RegexMatchMapper<'a> {
    fn map_value(&self, in_val: &<Text as TypeInfo>::Store) -> <Boolean as TypeInfo>::Store {
        self.re.is_match(in_val.as_ref())
    }
}


impl<'alloc, NULL: Nullability> BoundExpr<'alloc> for BoundRegexMatch<'alloc, NULL>
{
    fn schema(&self) -> &Schema {
        return &self.schema
    }

    fn evaluate<'a>(&self, view: &'a View<'a>, rows: RowOffset) -> Result<Block<'alloc>, DBError> {
        let mut out = Block::new(self.alloc, &self.schema);
        out.add_rows(rows)?;

        // TODO: in offsets

        let src_col = view.column(0).unwrap();
        let out_col = out.column_mut(0).unwrap();

        let mut mapper = RegexMatchMapper{re: &self.re};

        let conv = column::BoundOneColMapper::<Text, NULL, Boolean, NULL>::new();
        conv.map(src_col, out_col, &mut mapper)?;

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::allocator;
    use ::schema::{Attribute, Schema};
    use ::table::{Table, TableAppender};
    use ::types::*;

    #[test]
    fn expr() {
        let attrs = vec![
            Attribute{name: "one".to_string(), nullable: false, dtype: Type::TEXT},
            Attribute{name: "two".to_string(), nullable: true, dtype: Type::TEXT},
        ];

        let block = {
            let schema = Schema::from_slice(attrs.as_ref()).unwrap();
            let mut table = Table::new(&allocator::GLOBAL, &schema, None);

            let status = TableAppender::new(&mut table)
                .add_row()
                .set("text")
                .set("1234")
                .add_row()
                .set("ext")
                .set_null(true)
                .add_row()
                .set("none")
                .set("5678")
                .done();

            assert!(status.is_none(), "Error appending rows {}", status.unwrap());

            table.take().unwrap()
        };

        let col0 = block.slice_view(&[0], None).unwrap();

        let nn_re = regex::Regex::new("ext").unwrap();
        let nn_be = BoundRegexMatch::<NotNullable> {
            re: &nn_re,
            alloc: &allocator::GLOBAL,
            schema: Schema::from_attr(attrs[0].cast(Type::BOOLEAN)),
            nullability: PhantomData,
        };

        let result = nn_be.evaluate(&col0, block.rows()).unwrap();

        // TODO:
        // 3. compare

        let col1 = block.slice_view(&[1], None).unwrap();
    }
}
