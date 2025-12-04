# Testing Guide Without HDFS Server

This guide explains how to test `webhdfsmagic` without requiring an actual HDFS cluster.

## ⚠️ Important Notes

### mkdir Command - No `-p` Option Needed

The `%hdfs mkdir` command **automatically creates parent directories**, similar to `mkdir -p` in Unix. You do **NOT** need to use the `-p` flag.

#### ✅ Correct Usage

```python
# Creates /demo/data/2024/12 and all parent directories automatically
%hdfs mkdir /demo/data/2024/12

# Simple directory
%hdfs mkdir /test
```

#### ❌ Incorrect Usage

```python
# The -p option does NOT exist and will cause an error
%hdfs mkdir -p /demo/data  # ❌ DO NOT USE
```

**Technical Explanation**: WebHDFS uses the `MKDIRS` operation which inherently creates all parent directories, so no additional flag is necessary.

---

## 🎯 Testing Options

### Option 1: Mock-based Testing (Recommended) ✅

Mocks simulate HDFS responses without needing a real server.

#### Python Standalone Script

```bash
python examples/test_mock.py
```

#### In Jupyter Notebook

Open `examples/examples.ipynb` and execute the cells with mocks.

### Option 2: Local Docker HDFS

If you want a real HDFS cluster for integration testing:

```bash
# Start a mini HDFS cluster
docker run -d \
  -p 9870:9870 \
  -p 9000:9000 \
  -p 50070:50070 \
  --name hdfs-test \
  apache/hadoop:3.3.6

# Verify HDFS is ready
docker logs -f hdfs-test
```

Then in your notebook:

```python
%hdfs setconfig {
    "knox_url": "http://localhost:9870",
    "webhdfs_api": "/webhdfs/v1",  
    "username": "root",
    "password": "",
    "verify_ssl": false
}

%hdfs ls /
```

### Option 3: Unit Tests (CI/CD)

Unit tests already use mocks:

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=webhdfsmagic --cov-report=html

# Run specific test
pytest tests/test_magics.py::test_ls -v
```

## 📝 Creating Your Own Mocks

Example in a notebook:

```python
from unittest.mock import MagicMock, patch

# Dummy configuration
%hdfs setconfig {
    "knox_url": "http://fake:8443/gateway/default",
    "webhdfs_api": "/webhdfs/v1",
    "username": "test",
    "password": "test",  # pragma: allowlist secret
    "verify_ssl": false
}

# Mock for ls command
def mock_ls(*args, **kwargs):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "FileStatuses": {
            "FileStatus": [{
                "pathSuffix": "my_file.txt",
                "type": "FILE",
                "length": 1024,
                "owner": "you",
                "group": "hadoop",
                "permission": "644",
                "modificationTime": 1700000000000,
                "blockSize": 134217728,
                "replication": 3
            }]
        }
    }
    response.content = b'{"FileStatuses": ...}'
    return response

# Use the mock
with patch('requests.request', side_effect=mock_ls):
    result = %hdfs ls /user/test
    display(result)
```

### Complete Mock Examples

For all 8 commands (ls, cat, mkdir, rm, chmod, chown, put, get), see:
- `examples/examples.ipynb` - Full notebook with comprehensive mocks
- `examples/test_mock.py` - Standalone Python script

#### Mock for PUT (Upload)

```python
def mock_put(*args, **kwargs):
    """Mock two-step PUT: 307 redirect then 201 created"""
    if kwargs.get('allow_redirects') is False:
        # Step 1: Return redirect
        response = MagicMock()
        response.status_code = 307
        response.headers = {'Location': 'http://datanode:50075/webhdfs/v1/file?...'}
        return response
    else:
        # Step 2: Return success
        response = MagicMock()
        response.status_code = 201
        return response
```

#### Mock for GET (Download)

```python
def mock_get(*args, **kwargs):
    """Mock streaming download"""
    response = MagicMock()
    response.status_code = 200
    response.iter_content = lambda chunk_size: [b'file content chunk 1', b'chunk 2']
    return response
```

## 🚀 Testing Auto-loading

Verify the extension loads automatically:

1. Launch Jupyter: `jupyter notebook`
2. Create a new notebook
3. In the first cell, type directly: `%hdfs help`
4. If it works without `%load_ext webhdfsmagic`, auto-loading is working! ✅

## 🛠️ Troubleshooting

### Extension Doesn't Auto-load

Check the startup script:

```bash
cat ~/.ipython/profile_default/startup/00-webhdfsmagic.py
```

Should contain:
```python
try:
    from IPython import get_ipython
    ipython = get_ipython()
    if ipython is not None:
        ipython.magic('load_ext webhdfsmagic')
except Exception as e:
    print(f"Failed to auto-load webhdfsmagic: {e}")
```

### Reinstall Auto-loading

```bash
jupyter-webhdfsmagic
```

### Manual Testing

In IPython/Jupyter:

```python
import sys
print(sys.path)  # Verify webhdfsmagic is in the path

%load_ext webhdfsmagic  # Load manually
%hdfs help
```

### Debug IPython Configuration

```python
# In a notebook cell
from IPython import get_ipython
import sys

# Check IPython paths
print("IPython paths:")
for path in get_ipython().profile_dir.startup_dir, sys.prefix:
    print(f"  {path}")

# Check if webhdfsmagic is installed
import webhdfsmagic
print(f"\nwebhdfsmagic location: {webhdfsmagic.__file__}")
```

## 🧪 Advanced Testing Scenarios

### Testing Error Handling

```python
def mock_error(*args, **kwargs):
    response = MagicMock()
    response.status_code = 404
    response.text = "File not found"
    response.json.return_value = {"RemoteException": {"message": "File not found"}}
    return response

with patch('requests.request', side_effect=mock_error):
    %hdfs ls /nonexistent
```

### Testing with Wildcards

```python
# Mock response for wildcard patterns
def mock_ls_wildcard(*args, **kwargs):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "FileStatuses": {
            "FileStatus": [
                {"pathSuffix": "file1.csv", "type": "FILE", "length": 100},
                {"pathSuffix": "file2.csv", "type": "FILE", "length": 200},
            ]
        }
    }
    return response

with patch('requests.request', side_effect=mock_ls_wildcard):
    %hdfs ls /data/*.csv
```

### Performance Testing

```python
import time

def mock_large_file(*args, **kwargs):
    """Simulate large file download"""
    response = MagicMock()
    response.status_code = 200
    # Simulate 100MB file in 1MB chunks
    response.iter_content = lambda chunk_size: (b'x' * chunk_size for _ in range(100))
    return response

start = time.time()
with patch('requests.get', side_effect=mock_large_file):
    %hdfs get /large_file.dat /tmp/output.dat
elapsed = time.time() - start
print(f"Download completed in {elapsed:.2f}s")
```

## 📊 Continuous Integration

The project uses GitHub Actions for CI/CD. The workflow includes:

1. **Linting**: `ruff check .`
2. **Testing**: `pytest tests/ -v`
3. **Coverage**: `pytest --cov=webhdfsmagic`

To run the same checks locally before pushing:

```bash
# Full CI simulation
ruff check .
pytest tests/ -v --cov=webhdfsmagic

# Or use pre-commit hooks
pre-commit run --all-files
```
