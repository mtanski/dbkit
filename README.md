# dbkit

[![Join the chat at https://gitter.im/rust-dbkit/Lobby](https://badges.gitter.im/rust-dbkit/Lobby.svg)](https://gitter.im/rust-dbkit/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/mtanski/dbkit.svg?branch=master)](https://travis-ci.org/mtanski/dbkit)
[![Crates.io](https://img.shields.io/crates/v/dbkit-engine.svg)](https://crates.io/crates/dbkit-engine)

Columnar query processing engine written in Rust.
Part of the dbkit suite of Rust libraries. dbkit isn't a standalone database,
rather its a group of libaries that provided building blocks to build a
database or database like data processing applications.

dbkit can be used to build:
* It can be used to implement parts of an OLAP database
* Building (big) data workflows pipelines that using relational algerbra / calculus operators
* Building tools that operate structured data

Since the project early in its life and under currently *under heavy development* and no backwards
compatability is provided even in minor versions.

## Personal Goals

dbkit is my personal project to improve my Rust skills and be means of learning a number of new
techniques (such a code generation) through the lense of databases. 

This project has been influenced by many database papers, open source databases,
CMU's database group's [Youtube channel](https://www.youtube.com/@CMUDatabaseGroup)
and of course my past career of building weird little databases.

## Project Goals

- [X] Building blocks representing schema & data
- [ ] Implementation of relational operations and expressions *In progress*
- [ ] Query AST tree representation
- [ ] SIMD implementation of operations
- [ ] GPU (OpenCL) implementation of operations
- [ ] Bindings for other languages
    - [ ] Python
    - [ ] ?

## Documentation

Automatically generated documentation is available from [docs.rs](https://docs.rs/dbkit-engine)

## Requirements

The project requires the Rust language compiler and cargo to build.
Currently the project only builds using nightly channel of rust to due to aggressive use of features only
present in nightly Rust.

## Rust unsafe

The project makes extensive use unsafe Rust particulary in lower level primatives. I would love PRs that
reduce the use of unsafe where its possible without a performance penalty.
