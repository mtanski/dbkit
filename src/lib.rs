#![feature(alloc)]
#![feature(associated_consts)]
#![feature(associated_type_defaults)]
#![feature(box_patterns)]
#![feature(box_syntax)]
#![feature(heap_api)]
#![feature(inclusive_range_syntax)]
#![feature(question_mark)]

//! DBKit Engine -- Columnar query processing engine
//!
//! Part of the DBKit set of Rust libraries. DBKit isn't a standalone database, rather its a
//! group of libraries that provided building blocks to build a database or database like data
//! processing applications.

extern crate alloc;

#[macro_use]
extern crate log;

#[macro_use]
extern crate itertools;

extern crate num;

/// Database error type and error utilities
pub mod error;

/// Allocator facilities for column data and in flight operations & expressions.
pub mod allocator;
/// Database Type system
pub mod types;
/// Database schema
pub mod schema;
pub mod row;
pub mod util;

/// Containers for columnar data.
pub mod block;
/// Tools for creating, writing & accessing columnar by row or element.
pub mod table;

/// Database operations
pub mod operation;
/// Database expressions
pub mod expression;

/// Data structures for representing schema projections.
pub mod projector;

#[cfg(feature = "jit")]
pub mod jit;
