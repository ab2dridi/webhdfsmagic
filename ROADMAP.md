# 🗺️ Roadmap - Upcoming Features

## Current Status

**Version 0.0.4** - Multi-threaded get/put


**Features Implemented:**
- ✅ Auto-detect file type from extension and content (CSV, TSV, Parquet)
- ✅ Infer delimiter automatically (comma, tab, pipe, semicolon)
- ✅ Display as formatted table using `tabulate`
- ✅ Support for Parquet files
- ✅ Line limit option to avoid memory issues with large files
- ✅ `--raw` flag to preserve raw text behavior
- ✅ `--format` option (pandas, polars)

**Version 0.0.2** - 8 commands fully implemented and tested:
- ✅ `ls` - List files and directories
- ✅ `mkdir` - Create directories (with parent creation)
- ✅ `put` - Upload files (supports wildcards like `*.csv`)
- ✅ `get` - Download files (supports wildcards and `~` expansion)
- ✅ `cat` - Display file content (with line limit option)
- ✅ `rm` - Delete files/directories (recursive with `-r`, supports wildcards)
- ✅ `chmod` - Change permissions (recursive with `-R`)
- ✅ `chown` - Change owner (recursive with `-R`, requires superuser)

**Advanced Features:**
- ✅ Wildcard support for `put`, `get`, and `rm` commands
- ✅ Recursive operations with `-R` flag for `chmod` and `chown`
- ✅ Home directory expansion (`~`) in file paths
- ✅ Docker hostname resolution fix for containerized environments
- ✅ SSL verification with custom certificates
- ✅ Streaming support for large file downloads



## 🎯 Priority Features

### 1. stat - File Metadata

Get detailed information about a file (size, owner, permissions, modification date, etc.)

```python
%hdfs stat /data/large_file.csv
# Output: Size: 1.2 GB, Owner: hdfs, Permissions: rw-r--r--, Modified: 2025-12-04 10:30:00
```

**Use Case:** Data validation, existence checking, quick inspection before processing

### 2. du - Disk Usage

Calculate disk space used by files or directories

```python
%hdfs du -h /data
# Output: 15.3 GB    /data

%hdfs du /data/logs
# 1048576    /data/logs/app.log
# 2097152    /data/logs/error.log
```

**Options:**
- `-s`: Summary only (no details per file)
- `-h`: Human-readable format (MB, GB, TB)

**Use Case:** Space monitoring, quota management, cleaning old data

### 3. mv - Move/Rename

Move or rename files and directories

```python
%hdfs mv /data/raw/file.csv /data/processed/file.csv

%hdfs mv /tmp/old_name /data/new_name
```

**Use Case:** Data organization, ETL workflows, archiving

## 🚀 Future Features

### 4. cp - Copy Files

Duplicate files on HDFS for backup or replication

```python
%hdfs cp /data/important.csv /backup/important.csv
```

**Use Case:** Backup, data duplication, testing

### 5. tail - Read File End

Display the last lines of a file (useful for logs)

```python
%hdfs tail -n 100 /logs/application.log
```

**Use Case:** Log analysis, debugging, monitoring

### 6. find - Search Files

Search files by name or pattern in the tree

```python
%hdfs find /data -name "*.csv"
%hdfs find /logs -name "error*"
```

**Use Case:** Data discovery, audit, cleanup

### 7. Progress Bars

Display progress for long operations (upload/download of large files)

```python
%hdfs put large_file.parquet /data/
# [████████████████████] 100% | 5.2 GB/5.2 GB | 2m 15s
```

**Use Case:** Better user experience, operation tracking


### 8. Checksum - Integrity Verification

Calculate and verify MD5 checksums of files

```python
%hdfs checksum /data/file.csv
# MD5: a3b2c1d4e5f6...
```

**Use Case:** Integrity validation, corruption detection

### 9. Append - Add to File

Append content to the end of an existing file

```python
%hdfs append local_new_data.csv /data/cumulative.csv
```

**Use Case:** Log aggregation, incremental loading

## 🔧 Configuration Improvements

### Inline Configuration

Modify configuration without editing files

```python
%hdfs config --set knox_url=https://prod.example.com/gateway
%hdfs config --show
```

### Multiple Profiles

Easily switch between environments (dev, staging, prod)

```python
%hdfs config --profile production
%hdfs config --profile development
```

Configuration stored in `~/.webhdfsmagic/profiles/`

### Environment Variables

Support for environment variables for CI/CD

```bash
export WEBHDFS_URL="https://..."
export WEBHDFS_USER="username"
export WEBHDFS_PASSWORD="password"
```

## 🏢 Enterprise Features

### Kerberos Authentication

Kerberos support for production-secured clusters

```python
%hdfs config --auth kerberos --keytab /path/to/keytab
```

### Advanced SSL Validation

Improved SSL/TLS certificate handling

### Quota Management

View and manage disk space quotas

```python
%hdfs quota /user/username
# Space quota: 500 GB, Used: 342 GB (68%)
```

