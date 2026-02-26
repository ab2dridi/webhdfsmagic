# Changelog

All notable changes to webhdfsmagic will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.6] - 2026-02-26

### Added
- **`%hdfs stat` — File/directory metadata** (single path via `GETFILESTATUS`):
  - Returns a single-row DataFrame with: `name`, `type`, `size`, `owner`, `group`, `permissions`, `block_size`, `modified`, `replication`
  - Graceful 404 handling: returns a friendly "path not found" message instead of raising an exception
- **`%hdfs mv` — Rename/move files and directories** (via `RENAME` operation):
  - Syntax: `%hdfs mv <src> <dst>`
  - Returns a success message or a clear error if the destination already exists
  - Graceful 404 handling: returns a friendly message when the source path does not exist

## [0.0.5] - 2026-02-26

### Added
- **`%hdfs du` — Disk usage command** (real directory sizes via `GETCONTENTSUMMARY`):
  - Default: iterates over immediate children and returns their real recursive size
  - `-s`: summary of the path itself (single `GETCONTENTSUMMARY` call, no children iteration)
  - `-h`: human-readable sizes (KB/MB/GB)
  - `-sh`: combine both options
  - Returns a DataFrame with columns: `name`, `type`, `size`, `space_consumed`, `file_count`, `dir_count`, `error`
  - **Graceful permission handling**: directories returning HTTP 403/401 are included in the DataFrame with `size=NaN` and an `error` message — the command never crashes mid-iteration

## [0.0.4] - 2025-12-21

### Added
- **Parallel transfers (PUT/GET):**
  - Added `--threads`/`-t` option for `put` and `get` commands (multi-threading with ThreadPoolExecutor)
  - Support for simultaneous upload/download of multiple files with robust error handling
  - Syntax: `%hdfs put --threads N <local_files> <hdfs_dir>` and `%hdfs get --threads N <hdfs_files> <local_dir>`

### Fixed
- Fixed newline display for multi-file results in Jupyter output

## [0.0.3] - 2025-12-21

### Added
- **Smart Cat Feature**: Automatic format detection and intelligent display for CSV, TSV, and Parquet files
  - Auto-detection of file format based on extension and content analysis
  - CSV/TSV formatting with pandas-style display and truncation for large datasets
  - Parquet support with **Polars** for ultra-fast and memory-efficient processing (3.7x faster than PyArrow)
  - New options: `--format <type>` (csv, parquet, pandas, polars, raw) and `--raw` for raw content display
  - Format options: pandas (classic), polars (with schema and types), grid (default table)
  - Intelligent line limiting with `-n` option that respects data structure
  - **Memory protection**: 50 MB download limit for partial reads to prevent memory saturation with large files
  - Large Parquet file detection (> 100 MB) with warning and recommendation to download first
- Enhanced help documentation with comprehensive examples

### Fixed
- Argument parsing for `rm -r` command to properly extract path
- Autoload installation for development mode compatibility

## [0.0.2] - 2025-12-08

### Added
- Docker demo environment with HDFS and Knox Gateway for local testing
- GIF animation showcasing complete workflow in README
- Demo recording script for documentation

### Improved
- Consolidated multiple demo notebooks into single comprehensive demo.ipynb

### Fixed
- Version extraction for PyPI build process

## [0.0.1] - 2025-12-03

### Added
- Initial release of webhdfsmagic IPython extension
- Core HDFS commands via WebHDFS/Knox Gateway:
  - `ls`: List files and directories with DataFrame output
  - `mkdir`: Create directories (with automatic parent creation)
  - `put`: Upload files with wildcard support
  - `get`: Download files with wildcard and `~` expansion
  - `cat`: Display file content
  - `rm`: Delete files/directories with recursive option
  - `chmod`: Change file permissions
  - `chown`: Change file ownership
- Configuration management via `~/.webhdfsmagic/config.json`
- SSL certificate verification support (optional)
- Automatic extension loading via `jupyter-webhdfsmagic` command
- Structured logging system with configurable levels

---

**Note**: This project follows semantic versioning. For upgrade instructions and breaking changes, see the [README](README.md).
