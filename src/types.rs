
use std::convert::AsRef;
use std::mem;
use std::slice;
use std::str;

use super::error::DBError;

#[derive(Clone, Copy)]
pub struct RawData {
    pub data: *mut u8,
    pub size: usize,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Type {
    UINT32,
    UINT64,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    BOOLEAN,
    TEXT,
    BLOB,
}

/// Trait providing higher level metadata about types
pub trait ValueInfo {
    type Store;

    const ENUM: Type;
    const DEEP_COPY: bool = false;

    // RUST is frustrating
    // cannot use mem::size_of::<Self::Store>()
    // because apparently size_of is not constant.
    fn size_of(&self) -> usize {
        mem::size_of::<Self::Store>()
    }
}

pub struct UInt32;
pub struct UInt64;
pub struct Int32;
pub struct Int64;
pub struct Float32;
pub struct Float64;
pub struct Boolean;
pub struct Text;
pub struct Blob;

impl ValueInfo for UInt32 {
    type Store = u32;
    const ENUM: Type = Type::UINT32;
}

impl ValueInfo for UInt64 {
    type Store = u64;
    const ENUM: Type = Type::UINT64;
}

impl ValueInfo for Int32 {
    type Store = i32;
    const ENUM: Type = Type::INT32;
}

impl ValueInfo for Int64 {
    type Store = i64;
    const ENUM: Type = Type::INT64;
}

impl ValueInfo for Float32 {
    type Store = f32;
    const ENUM: Type = Type::FLOAT32;
}

impl ValueInfo for Float64 {
    type Store = f64;
    const ENUM: Type = Type::FLOAT64;
}

impl ValueInfo for Boolean {
    type Store = bool;
    const ENUM: Type = Type::BOOLEAN;
}

impl ValueInfo for Text {
    type Store = RawData;
    const ENUM: Type = Type::TEXT;
    const DEEP_COPY: bool = true;
}

impl ValueInfo for Blob {
    type Store = RawData;
    const ENUM: Type = Type::BLOB;
    const DEEP_COPY: bool = true;
}

static UINT32: UInt32 = UInt32{};
static UINT64: UInt64 = UInt64{};
static INT32: Int32 = Int32{};
static INT64: Int64 = Int64{};
static FLOAT32: Float32 = Float32{};
static FLOAT64: Float64 = Float64{};
static BOOLEAN: Boolean = Boolean{};
static TEXT: Text = Text{};
static BLOB: Blob = Blob{};

impl Type {
    pub fn name(self) -> &'static str {
        match self {
            Type::UINT32  => "UINT32",
            Type::UINT64  => "UINT64",
            Type::INT32   => "INT32",
            Type::INT64   => "INT64",
            Type::FLOAT32 => "FLOAT32",
            Type::FLOAT64 => "FLOAT64",
            Type::BOOLEAN => "BOOLEAN",
            Type::TEXT    => "TEXT",
            Type::BLOB    => "BLOB",
        }
    }

    // RUST is frustrating
    // There's no implementation specialization,
    // and can't use a associated trait type (defaulted or not) in an expression.
    // So we have to keep repeating ourselves
    pub fn size_of(self) -> usize {
        match self {
            Type::UINT32    => UINT32.size_of(),
            Type::UINT64    => UINT64.size_of(),
            Type::INT32     => INT32.size_of(),
            Type::INT64     => INT64.size_of(),
            Type::FLOAT32   => FLOAT32.size_of(),
            Type::FLOAT64   => FLOAT64.size_of(),
            Type::BOOLEAN   => BOOLEAN.size_of(),
            Type::TEXT      => TEXT.size_of(),
            Type::BLOB      => BLOB.size_of(),
        }
    }
}

impl str::FromStr for Type {
    type Err = DBError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "UINT32"  => Ok(Type::UINT32),
            "UINT64"  => Ok(Type::UINT64),
            "INT32"   => Ok(Type::INT32),
            "INT64"   => Ok(Type::INT64),
            "FLOAT32" => Ok(Type::FLOAT32),
            "FLOAT64" => Ok(Type::FLOAT64),
            "BOOLEAN" => Ok(Type::BOOLEAN),
            "TEXT"    => Ok(Type::TEXT),
            "BLOB"    => Ok(Type::BLOB),
            _         => Err(DBError::UnknownType(String::from(s)))
        }
    }
}

impl AsRef<[u8]> for RawData {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.size) }
    }
}

impl AsRef<str> for RawData {
    fn as_ref(&self) -> &str {
        unsafe {
            let slice = slice::from_raw_parts(self.data, self.size);
            str::from_utf8_unchecked(slice)
        }
    }
}

impl ToString for RawData {
    fn to_string(&self) -> String {
        let str: &str = self.as_ref();
        String::from(str)
    }
}
