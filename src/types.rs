
use std::convert::Into;
use std::mem;
use std::str;
use std::string;

use super::error::DBError;

pub struct RawData {
    data: *mut u8,
    size: usize,
}

#[derive(Clone, PartialEq)]
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

pub trait TypeInfo {
    type Store;
    const ENUM: Type;
    const DEEP_COPY: bool = false;

    // RUST SUCKS: cannot use mem::size_of::<Self::Store>()
    // because apparently size_of is not constant.
    fn size_of() -> usize {
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

impl TypeInfo for UInt32 {
    type Store = u32;
    const ENUM: Type = Type::UINT32;
}

impl TypeInfo for UInt64 {
    type Store = u64;
    const ENUM: Type = Type::UINT64;
}

impl TypeInfo for Int32 {
    type Store = i32;
    const ENUM: Type = Type::INT32;
}

impl TypeInfo for Int64 {
    type Store = i64;
    const ENUM: Type = Type::INT64;
}

impl TypeInfo for Float32 {
    type Store = f32;
    const ENUM: Type = Type::FLOAT32;
}

impl TypeInfo for Float64 {
    type Store = f64;
    const ENUM: Type = Type::FLOAT64;
}

impl TypeInfo for Boolean {
    type Store = bool;
    const ENUM: Type = Type::BOOLEAN;
}

impl TypeInfo for Text {
    type Store = RawData;
    const ENUM: Type = Type::TEXT;
    const DEEP_COPY: bool = true;
}

impl TypeInfo for Blob {
    type Store = RawData;
    const ENUM: Type = Type::BLOB;
    const DEEP_COPY: bool = true;
}

static uint32 : UInt32 = UInt32{};
static uint64: UInt64 = UInt64{};
static int32: Int32 = Int32{};
static int64: Int64 = Int64{};
static float32: Float32 = Float32{};
static float64: Float64 = Float64{};
static boolean: Boolean = Boolean{};
static text: Text = Text{};
static blob: Blob = Blob{};

impl Type {
    fn name(&self) -> &'static str {
        match *self {
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

