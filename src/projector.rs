
use super::error::DBError;
use super::schema::{Attribute, Schema};

pub struct BoundProjector {
    /// Output Schema
    schema: Schema,
    bound_attrs: Vec<BoundAttribute>,
}

/// Projection for a single input operation
pub struct SingleSourceProjector(Vec<Projector>);

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

pub fn project_by_position(pos: usize) ->  SingleSourceProjector {
    SingleSourceProjector(vec![Projector(Source::POS(pos), As::ORIG)])
}

pub fn project_by_name(name: String) ->  SingleSourceProjector {
    SingleSourceProjector(vec![Projector(Source::NAME(name), As::ORIG)])
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
    fn bind(&self, input: &Schema) -> Result<BoundProjector, DBError> {
        let mut bound = Vec::new();

        for proj in self.0.iter() {
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

        let attrs = bound.iter().map(|ref e| e.2.clone()).collect();
        Ok(BoundProjector { schema: Schema::from_vec(attrs)?, bound_attrs: bound })
    }
}

impl MultiSourceProjector {
    fn bind(&self, src: &[&Schema]) -> Result<BoundProjector, DBError> {
        Err(DBError::Unknown)
    }
}
