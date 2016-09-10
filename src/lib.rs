/// Columnar query processing engine written in Rust.
///
/// Part of the dbkit suite of Rust libraries. dbkit isn't a standalone database, rather its a
/// group of libaries that provided building blocks to build a database or database like data
/// processing applications.


#![feature(alloc)]
#![feature(associated_consts)]
#![feature(associated_type_defaults)]
#![feature(box_patterns)]
#![feature(box_syntax)]
#![feature(heap_api)]
#![feature(question_mark)]

extern crate alloc;

#[macro_use]
extern crate itertools;

pub mod error;

pub mod allocator;
pub mod types;
pub mod schema;
pub mod row;
pub mod util;

pub mod block;
pub mod table;

pub mod operation;
pub mod expression;

pub mod projector;
