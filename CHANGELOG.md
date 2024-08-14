# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2024-08-13

### Added

- `osmx-rs` CLI command, which includes an `extract` subcommand to turn an `.osm.pbf` file into a `.osmx` database, and a `stat` command which prints statistics about the contents of an `.osmx` database.

## [0.1.0] - 2024-03-06

Initial release.

Supports reading `.osmx` files, including:

- fetching nodes, ways and relations by ID
- reading an element's tags
- getting a node's location, a way's nodes, or a relation's members
- finding nodes in a region using the spatial index
- getting reverse relationships (finding all ways that a node is part of, or all relations that an element is a member of)

Does not yet support:

- reading element metadata (e.g. the version number of an element or the changeset and user that most recently modified it)
- writing data to an .osmx database, or creating a new .osmx database



[unreleased]: https://github.com/jake-low/osmx-rs/compare/v1.1.1...HEAD
[0.2.0]: https://github.com/jake-low/osmx-rs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jake-low/osmx-rs/releases/tag/v0.1.0
