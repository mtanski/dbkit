// vim: set ts=4 sw=4 et :

// libstd
use std::iter::Iterator;

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

impl Attribute {
    fn new(name: &str, nullable: bool, dtype: Type) -> Attribute {
        Attribute { name: String::from(name), nullable: nullable, dtype: dtype }
    }
}

#[derive(Clone)]
pub struct Schema {
    attrs: Vec<Attribute>,
}

pub struct AttributeIter<'a> {
    schema: &'a Schema,
    cur: usize
}

impl Schema {
    pub fn from_slice(attrs: &[Attribute]) -> Schema {
        let mut av = Vec::new();
        av.extend_from_slice(attrs);

        Schema { attrs: av }
    }

    pub fn from_vec(attrs: Vec<Attribute>) -> Schema {
        Schema { attrs: attrs }
    }

    /// Create a single Attribute schema
    pub fn from_attr(attr: Attribute) -> Schema {
        Schema { attrs: vec!(attr) }
    }

    pub fn make_one_attr(name: &str, nullable: bool, dtype: Type) -> Schema {
        Schema::from_attr(Attribute::new(name, nullable, dtype))
    }

    pub fn count(&self) -> usize {
        return self.attrs.len()
    }

    pub fn exists(&self, name: &String) -> Option<usize> {
        return None
    }

    pub fn get(&self, pos: usize) -> Result<&Attribute, DBError> {
        return if pos >= self.attrs.len() {
            Err(DBError::AttributeMissing(format!("(pos: {})", pos)))
        } else {
            Ok(&self.attrs[pos])
        }
    }

    pub fn find(&self, name: &String) -> Result<&Attribute, DBError> {
        for attr in self.attrs.iter() {
            if attr.name == *name {
                return Ok(attr)
            }
        }

        Err(DBError::AttributeMissing(format!("(name: {})", name)))
    }

    pub fn iter(&self) -> AttributeIter {
        AttributeIter { schema: self, cur: 0 }
    }
}

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

