#![feature(associated_consts)]

use std::mem;

#[derive(Clone)]
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
    const SIZE_OF: usize;
    const ENUM: Type;
    const DEEP_COPY: bool = false;
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
    const SIZE_OF: usize = mem::size_of::<UInt32::Store>();
    const ENUM: Type = Type::UINT32;
}

impl TypeInfo for UInt64 {
    type Store = u64;
    const SIZE_OF: usize = mem::size_of::<UInt64::Store>();
    const ENUM: Type = Type::UINT64;
}

impl TypeInfo for Int32 {
    type Store = i32;
    const SIZE_OF: usize = mem::size_of::<Int32::Store>();
    const ENUM: Type = Type::INT32;
}

impl TypeInfo for Int64 {
    type Store = i64;
    const SIZE_OF: usize = mem::size_of::<Int64::Store>();
    const ENUM: Type = Type::INT64;
}

impl TypeInfo for Float32 {
    type Store = f32;
    const SIZE_OF: usize = mem::size_of::<Float32::Store>();
    const ENUM: Type = Type::FLOAT32;
}

impl TypeInfo for Float64 {
    type Store = f64;
    const SIZE_OF: usize = mem::size_of::<Float64::Store>();
    const ENUM: Type = Type::FLOAT64;
}

impl TypeInfo for Boolean {
    type Store = bool;
    const SIZE_OF: usize = mem::size_of::<Boolean::Store>();
    const ENUM: Type = Type::BOOLEAN;
}

impl TypeInfo for Text {
    type Store = str;
    const SIZE_OF: usize = mem::size_of::<Text::Store>();
    const ENUM: Type = Type::TEXT;
    const DEEP_COPY: bool = true;
}

impl TypeInfo for Blob {
    type Store = [u8];
    const SIZE_OF: usize = mem::size_of::<Blob::Store>();
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

    fn from_name(name: &str) -> Option<Type> {
        Some(match name {
            "UINT32"  => Type::UINT32,
            "UINT64"  => Type::UINT64,
            "INT32"   => Type::INT32,
            "INT64"   => Type::INT64,
            "FLOAT32" => Type::FLOAT32,
            "FLOAT64" => Type::FLOAT64,
            "BOOLEAN" => Type::BOOLEAN,
            "TEXT"    => Type::TEXT,
            "BLOB"    => Type::BLOB,
            _         => return None,
        })
    }

    fn info(&self) -> TypeInfo {
        match *self {
            Type::UINT32  => &uint32,
            Type::UINT64  => &uint64,
            Type::INT32   => &int32,
            Type::INT64   => &int64,
            Type::FLOAT32 => &float32,
            Type::FLOAT64 => &float64,
            Type::BOOLEAN => &boolean,
            Type::TEXT    => &text,
            Type::BLOB    => &blob,
        }
    }
}

