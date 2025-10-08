# FalkorDB CSV Loader - Rust

A high-performance Rust implementation of the FalkorDB CSV loader, designed to load nodes and edges from CSV files into FalkorDB with batch processing and comprehensive error handling.

## Features

- **Async/Await**: Built with Tokio for high-performance async operations
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Schema Management**: Automatic creation of indexes and constraints
- **Merge Mode**: Support for upsert operations using MERGE instead of CREATE
- **Type Safety**: Automatic type inference for node and relationship properties
- **Progress Reporting**: Configurable progress tracking during loading operations
- **Error Handling**: Comprehensive error handling with detailed logging
- **Label Sanitization**: Automatic handling of invalid characters in labels
- **Connection Management**: Robust Redis connection handling with authentication support

## Installation

### Prerequisites

- Rust 1.70+ (2021 edition)
- FalkorDB running on Redis

### Build from source

```bash
git clone [<repository-url>](https://github.com/FalkorDB/FalkorDB-Loader-RS)
cd FalkorDBLoader-RS
cargo build --release
```

The binary will be available at `target/release/falkordb-loader`.

## Usage

### Basic usage

```bash
./target/release/falkordb-loader my_graph
```

### Advanced usage

```bash
./target/release/falkordb-loader my_graph \
  --host localhost \
  --port 6379 \
  --username myuser \
  --password mypass \
  --csv-dir ./csv_output \
  --batch-size 1000 \
  --merge-mode \
  --stats \
  --progress-interval 500
```

### Command-line options

- `graph_name`: Target graph name in FalkorDB (required)
- `--host`: FalkorDB host (default: localhost)
- `--port`: FalkorDB port (default: 6379)
- `--username`: FalkorDB username (optional)
- `--password`: FalkorDB password (optional)
- `--csv-dir`: Directory containing CSV files (default: csv_output)
- `--batch-size`: Batch size for loading (default: 5000)
- `--merge-mode`: Use MERGE instead of CREATE for upsert behavior
- `--stats`: Show graph statistics after loading
- `--progress-interval`: Report progress every N records (default: 1000, set to 0 to disable)

### Environment variables for logging

Set the log level using the `RUST_LOG` environment variable:

```bash
RUST_LOG=info ./target/release/falkordb-loader my_graph
RUST_LOG=debug ./target/release/falkordb-loader my_graph  # More verbose
```

### Progress reporting

Control progress reporting frequency:

```bash
# Report progress every 500 records
./target/release/falkordb-loader my_graph --progress-interval 500

# Report progress every 10,000 records (less frequent)
./target/release/falkordb-loader my_graph --progress-interval 10000

# Disable progress reporting entirely
./target/release/falkordb-loader my_graph --progress-interval 0
```

## CSV File Format

The loader expects CSV files in the following format:

### Node files

Files should be named `nodes_<LABEL>.csv` where `<LABEL>` is the node label.

```csv
id,name,age,email
1,"John Doe",30,"john@example.com"
2,"Jane Smith",25,"jane@example.com"
```

Required columns:
- `id`: Unique identifier for the node

### Edge files

Files should be named `edges_<RELATIONSHIP_TYPE>.csv` where `<RELATIONSHIP_TYPE>` is the relationship type.

```csv
source,target,source_label,target_label,weight,since
1,2,"Person","Person",0.8,"2020-01-01"
```

Required columns:
- `source`: ID of the source node
- `target`: ID of the target node

Optional columns:
- `source_label`: Label of the source node (improves performance)
- `target_label`: Label of the target node (improves performance)

### Index files (optional)

File should be named `indexes.csv`:

```csv
labels,properties,uniqueness,type
Person,name,NON_UNIQUE,BTREE
Person,email,NON_UNIQUE,BTREE
```

### Constraint files (optional)

File should be named `constraints.csv`:

```csv
labels,properties,type,entity_type
Person,email,UNIQUE,NODE
```

## Performance Characteristics

- **Optimized Batch Processing**: True batch query execution (multiple records per query)
- **Async Operations**: All database operations are async for better concurrency
- **Configurable Batch Sizes**: Default 5000 records per batch, fully configurable
- **Index Creation**: Indexes are created before loading data for optimal performance
- **Memory Efficient**: Streams data from CSV files without loading everything into memory
- **Connection Pooling**: Uses Redis connection pooling for better performance
- **Intelligent Fallback**: Automatic fallback to individual queries if batch execution fails

## Error Handling

The application provides comprehensive error handling:

- Connection errors with retry logic
- CSV parsing errors with line numbers
- Query execution errors with full query logging
- Schema creation errors with graceful degradation
- File system errors with clear messages

## Comparison with Python Version

This Rust implementation provides several advantages over the Python version:

1. **Performance**: Significantly faster due to Rust's zero-cost abstractions and async runtime
2. **Memory Safety**: Rust's ownership system prevents memory leaks and data races
3. **Type Safety**: Compile-time guarantees prevent runtime type errors
4. **Concurrency**: Better async/await support with Tokio
5. **Resource Usage**: Lower memory footprint and CPU usage

## Architecture

The application is structured as follows:

- `FalkorDBCSVLoader`: Main struct handling all operations
- `Args`: CLI argument parsing with clap
- Async methods for each operation (index creation, constraint creation, data loading)
- Error handling with anyhow for better error propagation
- Logging with env_logger for configurable output

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions:
- Create an issue in the repository
- Check the logs for detailed error information
- Ensure FalkorDB is running and accessible
