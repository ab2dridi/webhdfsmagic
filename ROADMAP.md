# 🗺️ Roadmap - Upcoming Features

## Current Status

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

### 8. Parallel Operations

Speed up transfers of multiple files with parallelization

```python
%hdfs put -j 4 *.csv /data/  # 4 parallel threads
```

**Use Case:** Performance, batch processing

### 9. Checksum - Integrity Verification

Calculate and verify MD5 checksums of files

```python
%hdfs checksum /data/file.csv
# MD5: a3b2c1d4e5f6...
```

**Use Case:** Integrity validation, corruption detection

### 10. Append - Add to File

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

## 💡 Contributing

Contributions are welcome! To propose a new feature:

1. Open an issue on GitHub for discussion
2. Fork the project and create a branch
3. Implement the feature with tests
4. Submit a Pull Request

**Current Priorities:** `stat`, `du`, `mv`


### Enterprise (Future)
10. 🏢 Kerberos
11. 🏢 Advanced SSL
12. 🏢 Quota management

## 📊 Community Feedback

We welcome feature requests! Please:
1. Open an issue on GitHub with `[feature-request]` tag
2. Describe your use case
3. Provide example usage

## 🤝 Contributing

Want to implement a feature from the roadmap?

1. Check the [Issues page](https://github.com/ab2dridi/webhdfsmagic/issues) for current work
2. Comment on the issue you want to work on
3. Fork the repo and create a feature branch
4. Implement with tests
5. Submit a PR with:
   - Implementation
   - Unit tests
   - Demo notebook example
   - Documentation update

## 📝 Decision Log

### Why These Features?

- **stat/du/mv**: Most commonly requested operations from user surveys
- **Progress bars**: Frequently mentioned in feedback for large file operations
- **Kerberos**: Required for 80% of enterprise Hadoop deployments
- **Multiple profiles**: Common request from users with dev/prod environments

### What We're NOT Implementing

- **HDFS Federation support**: Too complex, minimal user benefit
- **Snapshot management**: Advanced feature, low demand
- **WebHDFS server implementation**: Out of scope for client library
- **GUI/Web interface**: Notebook-first philosophy

---

**Last Updated**: December 4, 2025  
**Roadmap Owner**: @ab2dridi  
**Status**: 🟢 Actively Maintained
