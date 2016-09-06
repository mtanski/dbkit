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
