# Logging Guide for webhdfsmagic

This guide explains how to use the logging system in webhdfsmagic for debugging and auditing HDFS operations.

## Overview

webhdfsmagic automatically logs all operations to `~/.webhdfsmagic/logs/webhdfsmagic.log`, inspired by sparkmagic's logging approach. This provides complete traceability for:

- 📝 Command execution history
- 🔍 HTTP request/response details
- ❌ Error tracking with stack traces
- 🔐 Security (passwords are automatically masked)
- 📊 Operation performance metrics

## Log Location

```bash
~/.webhdfsmagic/logs/webhdfsmagic.log
```

**Log Rotation:**
- Maximum size: 10 MB per file
- Backup files: 5 (webhdfsmagic.log.1, .2, .3, .4, .5)
- Automatic rotation when size limit reached

## Log Levels

### File Logging (DEBUG level)
All details are logged to the file:
- DEBUG: HTTP requests/responses, parameters, headers
- INFO: Operation start/end, configuration changes
- WARNING: Non-critical issues
- ERROR: Failures with full stack traces

### Console Output (WARNING level)
Only warnings and errors are shown in the notebook/terminal to avoid clutter.

## Log Format

```
YYYY-MM-DD HH:MM:SS - webhdfsmagic - LEVEL - [file.py:line] - message
```

**Example:**
```
2025-12-08 10:30:15 - webhdfsmagic - INFO - [magics.py:145] - >>> Starting operation: hdfs ls
2025-12-08 10:30:15 - webhdfsmagic - DEBUG - [magics.py:148] -     command: ls /user/hdfs
2025-12-08 10:30:15 - webhdfsmagic - DEBUG - [magics.py:148] -     args: ['/user/hdfs']
2025-12-08 10:30:15 - webhdfsmagic - DEBUG - [client.py:85] - HTTP Request: GET http://knox:8080/gateway/default/webhdfs/v1//user/hdfs
2025-12-08 10:30:15 - webhdfsmagic - DEBUG - [client.py:88] -     operation: LISTSTATUS
2025-12-08 10:30:15 - webhdfsmagic - DEBUG - [client.py:88] -     path: /user/hdfs
2025-12-08 10:30:16 - webhdfsmagic - DEBUG - [client.py:105] - HTTP Response: 200 from http://knox:8080/...
2025-12-08 10:30:16 - webhdfsmagic - INFO - [magics.py:180] - <<< Operation completed: hdfs ls - SUCCESS
2025-12-08 10:30:16 - webhdfsmagic - DEBUG - [magics.py:183] -     path: /user/hdfs
2025-12-08 10:30:16 - webhdfsmagic - DEBUG - [magics.py:183] -     file_count: 15
```

## Viewing Logs

### View Recent Activity

```bash
# Last 50 lines
tail -50 ~/.webhdfsmagic/logs/webhdfsmagic.log

# Follow logs in real-time
tail -f ~/.webhdfsmagic/logs/webhdfsmagic.log
```

### Search for Specific Operations

```bash
# Find all 'ls' commands
grep "hdfs ls" ~/.webhdfsmagic/logs/webhdfsmagic.log

# Find all 'put' operations with context
grep "hdfs put" ~/.webhdfsmagic/logs/webhdfsmagic.log -A 5 -B 2

# Find errors
grep "ERROR" ~/.webhdfsmagic/logs/webhdfsmagic.log

# Find specific file operations
grep "customers.csv" ~/.webhdfsmagic/logs/webhdfsmagic.log
```

### Filter by Date/Time

```bash
# Today's operations
grep "$(date +%Y-%m-%d)" ~/.webhdfsmagic/logs/webhdfsmagic.log

# Specific hour
grep "2025-12-08 10:" ~/.webhdfsmagic/logs/webhdfsmagic.log
```

### View HTTP Details

```bash
# All HTTP requests
grep "HTTP Request" ~/.webhdfsmagic/logs/webhdfsmagic.log

# All HTTP responses
grep "HTTP Response" ~/.webhdfsmagic/logs/webhdfsmagic.log

# Failed HTTP requests
grep "HTTP Response: [4-5][0-9][0-9]" ~/.webhdfsmagic/logs/webhdfsmagic.log
```

## Example Log Sessions

### Successful File Upload

```
2025-12-08 10:35:22 - webhdfsmagic - INFO - [magics.py:145] - >>> Starting operation: hdfs put
2025-12-08 10:35:22 - webhdfsmagic - DEBUG - [magics.py:148] -     command: put data.csv /user/hdfs/
2025-12-08 10:35:22 - webhdfsmagic - DEBUG - [magics.py:148] -     args: ['data.csv', '/user/hdfs/']
2025-12-08 10:35:22 - webhdfsmagic - DEBUG - [file_ops.py:125] - Uploading: data.csv -> /user/hdfs/data.csv
2025-12-08 10:35:22 - webhdfsmagic - DEBUG - [client.py:85] - HTTP Request: PUT http://knox:8080/...
2025-12-08 10:35:22 - webhdfsmagic - DEBUG - [client.py:105] - HTTP Response: 307 from http://knox:8080/...
2025-12-08 10:35:23 - webhdfsmagic - DEBUG - [client.py:105] - HTTP Response: 201 from http://datanode:9864/...
2025-12-08 10:35:23 - webhdfsmagic - INFO - [magics.py:180] - <<< Operation completed: hdfs put - SUCCESS
2025-12-08 10:35:23 - webhdfsmagic - DEBUG - [magics.py:183] -     local_pattern: data.csv
2025-12-08 10:35:23 - webhdfsmagic - DEBUG - [magics.py:183] -     hdfs_dest: /user/hdfs/
```

### Failed Operation with Error

```
2025-12-08 10:40:15 - webhdfsmagic - INFO - [magics.py:145] - >>> Starting operation: hdfs get
2025-12-08 10:40:15 - webhdfsmagic - DEBUG - [magics.py:148] -     command: get /missing.csv ./
2025-12-08 10:40:15 - webhdfsmagic - DEBUG - [client.py:85] - HTTP Request: GET http://knox:8080/...
2025-12-08 10:40:15 - webhdfsmagic - DEBUG - [client.py:105] - HTTP Response: 404 from http://knox:8080/...
2025-12-08 10:40:15 - webhdfsmagic - ERROR - [client.py:122] - ERROR in GET OPEN: HTTPError: 404 Client Error
2025-12-08 10:40:15 - webhdfsmagic - ERROR - [client.py:127] -     url: http://knox:8080/gateway/default/webhdfs/v1//missing.csv
2025-12-08 10:40:15 - webhdfsmagic - ERROR - [client.py:127] -     status_code: 404
2025-12-08 10:40:15 - webhdfsmagic - ERROR - [client.py:127] -     response_text: {"RemoteException":{"exception":"FileNotFoundException",...}}
2025-12-08 10:40:15 - webhdfsmagic - ERROR - [logger.py:135] - Full traceback:
Traceback (most recent call last):
  File "/workspaces/webhdfsmagic/webhdfsmagic/client.py", line 98, in execute
    response.raise_for_status()
  ...
```

## Troubleshooting with Logs

### Connection Issues

```bash
# Check if connection is established
grep "WebHDFSClient initialized" ~/.webhdfsmagic/logs/webhdfsmagic.log

# View connection errors
grep -i "connection" ~/.webhdfsmagic/logs/webhdfsmagic.log
```

### Authentication Problems

```bash
# Check authentication attempts
grep "HTTP Response: 401" ~/.webhdfsmagic/logs/webhdfsmagic.log

# Note: Passwords are automatically masked as "***MASKED***"
grep "password:" ~/.webhdfsmagic/logs/webhdfsmagic.log
```

### Performance Analysis

```bash
# Find slow operations (time between >>> and <<<)
grep -E "(Starting operation|Operation completed)" ~/.webhdfsmagic/logs/webhdfsmagic.log
```

### Wildcard Operations

```bash
# Track which files were matched by wildcards
grep "Uploading:" ~/.webhdfsmagic/logs/webhdfsmagic.log
grep "Downloading:" ~/.webhdfsmagic/logs/webhdfsmagic.log
```

## Security Features

### Password Masking

Passwords are **automatically masked** in logs:

```python
# In code
%hdfs setconfig {"username": "user", "password": "secret123"}

# In logs
2025-12-08 10:45:00 - webhdfsmagic - DEBUG - [magics.py:148] -     username: user
2025-12-08 10:45:00 - webhdfsmagic - DEBUG - [magics.py:148] -     password: ***MASKED***
```

### Log File Permissions

Log files are created with restricted permissions. Verify:

```bash
ls -la ~/.webhdfsmagic/logs/
```

Recommended: `chmod 600 ~/.webhdfsmagic/logs/*.log` to restrict access.

## Log Management

### Clean Old Logs

```bash
# Remove logs older than 30 days
find ~/.webhdfsmagic/logs/ -name "*.log*" -mtime +30 -delete

# Keep only current log
rm ~/.webhdfsmagic/logs/webhdfsmagic.log.[1-5]
```

### Archive Logs

```bash
# Archive to compressed file
cd ~/.webhdfsmagic/logs
tar -czf archive-$(date +%Y%m%d).tar.gz *.log*
mv archive-*.tar.gz ~/archives/
```

### Rotate Manually

Logs rotate automatically, but you can force rotation:

```bash
mv ~/.webhdfsmagic/logs/webhdfsmagic.log ~/.webhdfsmagic/logs/webhdfsmagic.log.manual
# Next operation will create a new log file
```

## Integration with Monitoring Tools

### Parse Logs Programmatically

```python
import re
from pathlib import Path

log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"

# Extract all operations
with open(log_file) as f:
    for line in f:
        if "Starting operation:" in line:
            print(line.strip())

# Count errors
error_count = 0
with open(log_file) as f:
    for line in f:
        if " - ERROR - " in line:
            error_count += 1
print(f"Total errors: {error_count}")
```

### Send Logs to External System

```bash
# Example: Send to syslog
tail -F ~/.webhdfsmagic/logs/webhdfsmagic.log | logger -t webhdfsmagic

# Example: Send to log aggregation service
tail -F ~/.webhdfsmagic/logs/webhdfsmagic.log | your-log-shipper
```

## Best Practices

1. **Check logs regularly** for errors and warnings
2. **Monitor log size** - automatic rotation keeps it manageable
3. **Archive old logs** periodically for compliance
4. **Use grep patterns** to quickly find relevant information
5. **Correlate timestamps** with notebook cell execution for debugging
6. **Keep logs secure** - they may contain sensitive paths/URLs
7. **Use logs for auditing** - track who accessed what and when