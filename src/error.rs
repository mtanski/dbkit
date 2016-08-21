// vim: set ts=4 sw=4 et :

use std::fmt;
use std::io::{Error as IOError};

pub enum DBError {
    Unknown,
    IO(IOError),
    AttributeMissing(String),
    AttributeNullability(String),
    AttributeType(String),
    /// Unknown memory allocation error
    Memory,
    /// Memory allocation limit reached (via policy)
    MemoryLimit(String),
}

impl DBError {
    pub fn makeColumnNotNullable(name: String) -> DBError {
        DBError::AttributeNullability(name)
    }

    pub fn makeColumnUnknownPos(pos: usize) -> DBError {
        DBError::AttributeMissing(format!("(pos: {})", pos))
    }
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DBError::Unknown =>
                write!(f, "Unknown Error"),
            DBError::IO(ref e) =>
                write!(f, "IO Error {}", e),
            DBError::AttributeMissing(ref attr) =>
                write!(f, "Unknown Attribute {}", attr),
            DBError::AttributeNullability(ref attr) =>
                write!(f, "Attribute Not Nullable {}", attr),
            DBError::AttributeType(ref attr) =>
                write!(f, "Attribute Type Mismatch {}", attr),
        }
    }
}

impl fmt::Debug for DBError {
    // Dummy implementation for Option / Result unwrap()
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}