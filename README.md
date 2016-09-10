# dbkit

[![Join the chat at https://gitter.im/rust-dbkit/Lobby](https://badges.gitter.im/rust-dbkit/Lobby.svg)](https://gitter.im/rust-dbkit/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/mtanski/dbkit.svg?branch=master)](https://travis-ci.org/mtanski/dbkit)
[![Crates.io](https://img.shields.io/crates/v/dbkit-engine.svg)](https://crates.io/crates/dbkit-engine)

Columnar query processing engine written in Rust.
dbkit is not a standalone database, rather it a library that be leveraged in other poejcts.

dbkit can be used to build:
* It can be used to implement parts of an OLAP database
* Building (big) data workflows pipelines that using relational algerbra / calculus operators
* Building tools that operate structured data

Since the project early in its life and under currently *under heavy development* and no backwards
compatability is provided even in minor versions.

## Supersonic inspiration

dbkit is inspired by the [Supersonic](https://github.com/google/supersonic) columar query engine by Google.
While dbkit draws a lot of inspiration from Supersonic it is not meant to be a strait port from C++ to Rust.

## Goals

- [ ] Building blocks representing schema & data *In progress*
- [ ] Implementation of relational operations and expressions
- [ ] Query AST tree representation
- [ ] SIMD implementation of operations
- [ ] GPU (OpenCL) implementation of operations 

## Documentation

Automatically generated documentation is available from [docs.rs](https://docs.rs/dbkit-engine)

## Requirements

The project requires the Rust language compiler and cargo to build.
Currently the project only builds using nightly channel of rust to due to aggressive use of features only
present in nightly Rust.

## Rust unsafe

The project makes extensive use unsafe Rust particulary in lower level primatives. I would love PRs that
reduce the use of unsafe where its possible without a performance penalty.
