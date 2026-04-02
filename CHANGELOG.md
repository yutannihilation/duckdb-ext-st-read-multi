# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.5] - 2026-04-02

- gpkg: Fix support on DATE and DATETIME (#49).
- gpkg: Fix handling of tables whose PK is not `fid` (#50).
- gpkg: Fix handling of BLOB columns (#51).

## [0.0.4] - 2026-02-07

- gpkg: Support DATE and DATETIME (#39).

## [0.0.3] - 2025-06-11

### Changed
- Renamed columns `filename` and `layer` to avoid conflicts with existing column names in the source data
- Refactored code to use cursors and offsets for better performance
- Updated rusqlite dependency from 0.32.1 to 0.36.0

### Fixed
- Fixed data chunk handling for both GeoJSON and GeoPackage formats

### Removed
- Removed unused imports

## [0.0.2] - 2025-06-10

### Added
- Support for reading multiple GeoJSON files with glob patterns
- Support for reading GeoPackage files with multiple layers
- Added `filename` and `layer` columns to output to track data source

[Unreleased]: https://github.com/yutannihilation/duckdb-ext-st-read-multi/compare/v0.0.5...HEAD
[0.0.5]: https://github.com/yutannihilation/duckdb-ext-st-read-multi/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/yutannihilation/duckdb-ext-st-read-multi/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/yutannihilation/duckdb-ext-st-read-multi/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/yutannihilation/duckdb-ext-st-read-multi/releases/tag/v0.0.2
