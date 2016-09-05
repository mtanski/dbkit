use itertools::Itertools;

use super::error::DBError;
use super::schema::{Attribute, Schema};
use super::block::{self, RefView, View};

/// Typed checked and evaluated projector
pub struct BoundProjector {
    /// Output Schema
    pub schema: Schema,
    bound_attrs: Vec<BoundAttribute>,
}

/// Projection for a single input operation
pub struct SingleSourceProjector(Vec<Projector>);
pub struct BuildSingleSourceProjector(Vec<Projector>);

/// Projection for multi-input operation
pub struct MultiSourceProjector(Vec<MultiProjector>);

enum Source {
    /// From source by position
    POS(usize),
    /// From source by name
    NAME(String),
    /// All source attributes
    ALL,
}

/// Project attribute as
enum As {
    /// Original name
    ORIG,
    // Project and prefix original name
    PREFIX(String),
    /// New name
    NEW(String),
}

struct Projector(Source, As);
struct MultiProjector(Projector, usize);

/// Bound attribute
/// input index, input column index & output attribute.
struct BoundAttribute(usize, usize, Attribute);

/// Project all attributes without renaming them
pub fn project_all_attributes() -> SingleSourceProjector {
    SingleSourceProjector(vec![Projector(Source::ALL, As::ORIG)])
}

/// Project single argument from source by column position
pub fn project_by_position(pos: usize) ->  SingleSourceProjector {
    SingleSourceProjector(vec![Projector(Source::POS(pos), As::ORIG)])
}

/// Project single argument from source by column name
pub fn project_by_name<S: ToString>(name: S) ->  SingleSourceProjector {
    SingleSourceProjector(vec![Projector(Source::NAME(name.to_string()), As::ORIG)])
}

fn mk_bound_attr(input: &Schema, pos: usize, out: &As) -> Result<BoundAttribute, DBError> {
    input.get(pos)
        .map(|attr| match *out {
            As::ORIG                => attr.clone(),
            As::PREFIX(ref prefix)  => attr.rename(format!("{}{}", prefix, attr.name)),
            As::NEW(ref name)       => attr.rename(name.clone()),
        })
        .map(|attr| BoundAttribute(0, pos, attr))
}

impl SingleSourceProjector {
    pub fn bind(&self, input: &Schema) -> Result<BoundProjector, DBError> {
        let mut bound = Vec::new();

        for proj in &self.0 {
            match proj.0 {
                Source::POS(pos) =>
                    bound.push(mk_bound_attr(input, pos, &proj.1)?),
                Source::NAME(ref name) =>
                    bound.push(mk_bound_attr(input, input.exists_ok(name.as_str())?, &proj.1)?),
                Source::ALL =>
                    for pos in 0..input.count() {
                        bound.push(mk_bound_attr(input, pos, &proj.1)?)
                    }
            }
        }

        let attrs = bound.iter().map(|e| e.2.clone()).collect();
        Ok(BoundProjector { schema: Schema::from_vec(attrs)?, bound_attrs: bound })
    }
}

impl BuildSingleSourceProjector {

    pub fn new() -> BuildSingleSourceProjector {
        BuildSingleSourceProjector(Vec::new())
    }

    pub fn add(mut self, mut proj: SingleSourceProjector) -> BuildSingleSourceProjector {
        self.0.append(&mut proj.0);
        self
    }

    pub fn add_as<S: ToString>(mut self, proj: SingleSourceProjector, name: S)
        -> BuildSingleSourceProjector
    {
        proj.0.into_iter()
            .map(|mut p| { p.1 = As::NEW(name.to_string()); p})
            .foreach(|p| self.0.push(p));
        self
    }

    pub fn add_prefixed<S: ToString>(mut self, proj: SingleSourceProjector, prefix: S)
        -> BuildSingleSourceProjector
    {
        proj.0.into_iter()
            .map(|mut p| { p.1 = As::PREFIX(prefix.to_string()); p})
            .foreach(|p| self.0.push(p));
        self
    }

    pub fn done(self) -> SingleSourceProjector {
        SingleSourceProjector(self.0)
    }
}

impl MultiSourceProjector {
    pub fn bind(&self, src: &[&Schema]) -> Result<BoundProjector, DBError> {
        Err(DBError::Unknown)
    }
}

impl BoundProjector {
    pub fn project_view<'a>(&self, src: &'a View<'a>) -> Result<RefView<'a>, DBError> {
        let mut columns = Vec::new();
        let schema = src.schema().clone();
        let rows = src.rows();

        for bound_attr in &self.bound_attrs {
            let c = src.column(bound_attr.1).unwrap();
            let nc = block::alias_column(c, None)?;

            columns.push(nc);
        }

        let out = RefView::new(schema, columns, rows);
        Ok(out)
    }
}

