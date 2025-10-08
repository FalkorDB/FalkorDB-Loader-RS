# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2024-10-08

### Fixed
- **MAJOR PERFORMANCE FIX**: Implemented true batch query execution
  - Previously executed one query per record (causing 30x slowdown)
  - Now executes entire batches in single queries for dramatic performance improvement
  - Should now match or exceed Python version performance
  - Includes intelligent fallback to individual queries if batch fails

### Changed
- Optimized query execution strategy for maximum performance
- Improved error handling with batch query fallback mechanism

## [0.1.1] - 2024-10-08

### Added
- **Progress Reporting**: Configurable progress tracking during loading operations
  - `--progress-interval` option to control reporting frequency (default: 1000 records)
  - Real-time progress updates with percentage and record counts
  - File-by-file progress tracking for multiple CSV files
  - Overall progress tracking across all nodes and edges
  - Can be disabled by setting interval to 0
- Enhanced logging with progress indicators using emojis
- Total record count calculation for accurate progress percentages

### Changed
- Improved batch processing with progress reporting integration
- Enhanced user experience with visual progress indicators

## [0.1.0] - 2024-10-08

### Added
- Initial release of FalkorDB CSV Loader in Rust
- Complete port of Python `falkordb_csv_loader.py` functionality
- Async/await support with Tokio runtime
- Batch processing for optimal performance
- Comprehensive CLI argument parsing with clap
- Schema management (indexes and constraints)
- Support for both CREATE and MERGE modes
- Type-safe property handling with automatic type inference
- Label sanitization for invalid characters
- Connection management with authentication support
- Detailed logging and error handling
- Graph statistics and verification features
- Complete documentation and usage examples

### Features
- **Performance**: Significantly faster than Python version due to Rust's zero-cost abstractions
- **Memory Safety**: Rust's ownership system prevents memory leaks and data races
- **Type Safety**: Compile-time guarantees prevent runtime type errors
- **Async Operations**: All database operations are async for better concurrency
- **Resource Efficiency**: Lower memory footprint and CPU usage
- **Robust Error Handling**: Comprehensive error handling with anyhow
- **Flexible Configuration**: Support for all Python version command-line options
- **Cross-platform**: Works on macOS, Linux, and Windows

### Technical Details
- Built with Rust 2021 edition
- Uses redis crate for FalkorDB connectivity
- CSV processing with csv crate
- Structured logging with log and env_logger
- CLI parsing with clap v4
- Date/time handling with chrono
- Error handling with anyhow

### Compatibility
- Compatible with all CSV formats supported by the Python version
- Supports the same command-line interface as the Python version
- Maintains compatibility with existing CSV output from migration tools