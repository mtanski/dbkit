// vim: set ts=4 sw=4 et :

// libstd
use std::iter::Iterator;
use std::collections::HashSet;

// DBKit
use super::error::DBError;
use super::types::Type;

/// Attribute represents high level column metadata such as name, nullability and type
#[derive(Clone)]
pub struct Attribute {
    pub name: String,
    pub nullable: bool,
    pub dtype: Type,
}

/// Describes the attributes and organization of data
#[derive(Clone, Default)]
pub struct Schema {
    attrs: Vec<Attribute>,
}

pub struct AttributeIter<'a> {
    schema: &'a Schema,
    cur: usize
}

impl Attribute {
    pub fn rename<S: Into<String>>(&self, name: S) -> Attribute {
        Attribute { name: name.into(), nullable: self.nullable, dtype: self.dtype }
    }
}

impl Schema {
    pub fn from_slice(attrs: &[Attribute]) -> Result<Schema, DBError> {
        let mut names = HashSet::with_capacity(attrs.len());

        for a in attrs {
            if names.replace(a.name.clone()).is_some() {
                return Err(DBError::AttributeDuplicate(a.name.clone()))
            }
        }

        Ok(Schema { attrs: Vec::from(attrs) })
    }

    pub fn from_vec(attrs: Vec<Attribute>) -> Result<Schema, DBError> {
        Self::from_slice(attrs.as_slice())
    }

    /// Create a single Attribute schema from an external attribute
    pub fn from_attr(attr: Attribute) -> Schema {
        Schema { attrs: vec!(attr) }
    }

    /// Create a single Attribute schema
    pub fn make_one_attr<S: Into<String>>(name: S, nullable: bool, dtype: Type) -> Schema {
        Schema::from_attr(Attribute{name: name.into(), nullable: nullable, dtype: dtype})
    }

    pub fn count(&self) -> usize {
        self.attrs.len()
    }

    pub fn exists(&self, name: &str) -> Option<usize> {
        for pos in 0..self.attrs.len() {
            if &self.attrs[pos].name == name {
                return Some(pos)
            }
        }

        None
    }

    pub fn exists_ok(&self, name: &str) -> Result<usize, DBError> {
        self.exists(name)
            .ok_or(DBError::AttributeMissing(format!("(name: {})", name)))
    }

    pub fn get(&self, pos: usize) -> Result<&Attribute, DBError> {
        if pos >= self.attrs.len() {
            Err(DBError::AttributeMissing(format!("(pos: {})", pos)))
        } else {
            Ok(&self.attrs[pos])
        }
    }

    pub fn find(&self, name: &str) -> Result<&Attribute, DBError> {
        for attr in &self.attrs {
            if &attr.name == name {
                return Ok(attr)
            }
        }

        Err(DBError::AttributeMissing(format!("(name: {})", name)))
    }

    pub fn iter(&self) -> AttributeIter {
        AttributeIter { schema: self, cur: 0 }
    }
}

/*

impl Display for Schema {

}

*/

impl<'a> Iterator for AttributeIter<'a> {
    type Item = &'a Attribute;

    fn next(&mut self) -> Option<&'a Attribute> {
        let r = if self.cur >= self.schema.attrs.len() {
            return None
        } else {
            &self.schema.attrs[self.cur]
        };

        self.cur += 1;
        Some(r)
    }
}

