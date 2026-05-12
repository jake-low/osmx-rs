# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-05-12

## Added

- `osmx-rs augmented-diff` subcommand to compute an XML Augmented Diff, given an OSMX database and `.osc` change file.
- support for reading element metadata

## Fixed

- `osmx-rs expand` subcommand flush() will no longer crash if there's nothing to flush
- `osmx-rs expand` coordinates are now converted to OSMX internal representation correctly
- fixed a bug in `builders::WayBuilder` that caused `osmx-rs expand` to discard tags from ways

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



[0.3.0]: https://github.com/jake-low/osmx-rs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jake-low/osmx-rs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jake-low/osmx-rs/releases/tag/v0.1.0
