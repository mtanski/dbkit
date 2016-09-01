#![feature(associated_consts)]
#![feature(associated_type_defaults)]
#![feature(alloc)]
#![feature(heap_api)]
#![feature(question_mark)]

extern crate alloc;

#[macro_use]
extern crate itertools;

pub mod error;

pub mod types;
pub mod schema;
pub mod row;

pub mod allocator;
pub mod block;
pub mod table;

pub mod operation;
pub mod expression;

pub mod projector;
