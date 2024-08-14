# osmx-rs

This is a Rust port of [OSMExpress](https://github.com/protomaps/OSMExpress/), a fast storage format for OpenStreetMap data created by Brandon Liu for [Protomaps](https://protomaps.com/). The format supports random access (looking up nodes, ways and relations by ID), spatial indexing of node locations, and in-place updates.

## Usage

See the [docs](https://docs.rs/osmx/latest) for API documentation and the [examples](./examples) directory for commented example programs.

The [README](https://github.com/protomaps/OSMExpress/blob/main/README.md), [Manual](https://github.com/protomaps/OSMExpress/blob/main/docs/MANUAL.md), and [Programming Guide](https://github.com/protomaps/OSMExpress/blob/main/docs/PROGRAMMING_GUIDE.md) of the OSMExpress C++ reference implementation may also be helpful.

## Features

This crate supports reading from `.osmx` database files, including:
- fetching nodes, ways and relations by ID
- reading an element's tags
- getting a node's location, a way's nodes, or a relation's members
- finding nodes in a region using the spatial index
- getting reverse relationships (finding all ways that a node is part of, or all relations that an element is a member of)

It does _not_ yet support:
- reading element metadata (e.g. the version number of an element or the changeset and user that most recently modified it)
- writing data to an `.osmx` database, or creating a new `.osmx` database

Pull requests for these missing features are welcome.

## Safety

osmx-rs is not designed for reading untrusted input files. Using this crate to read a malformed `.osmx` file may cause the process to panic, or worse. Pull requests to improve safety or prevent panics are welcome.

Also worth noting that osmx-rs depends on the [lmdb](https://crates.io/crates/lmdb) crate, which wraps the [LMDB](https://www.symas.com/lmdb) C API and therefore requires the use of `unsafe`.

## CLI command

This repository also contains a CLI tool for interacting with `.osmx` files.

Usage: `osmx-rs [COMMAND] [ARGS...]`

Commands:
- `expand`: convert an OSM PBF file to an OSMX database
- `stat`: print statistics about the contents of an OSMX database

The command is intended to be useful tool, but also to be an illustrative example of how to use the `osmx-rs` crate to create and interact with `.osmx` files. The source code can be found in the `bin/` directory.

## License

This code can be used under the terms of either the [MIT license](./LICENSE-MIT) or [Apache-2.0 license](./LICENSE-APACHE), at your option.
