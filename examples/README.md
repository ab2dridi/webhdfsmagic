# WebHDFS Magic - Examples

This directory contains examples and test notebooks for **webhdfsmagic**.

## Files

### Notebooks

1. **`examples.ipynb`** ⭐ **Recommended for users**
   - Clean, comprehensive notebook in English
   - Demonstrates all 8 HDFS commands (ls, cat, mkdir, rm, chmod, chown, put, get)
   - Shows mock testing without real HDFS cluster
   - Can be run with "Run All" - cells are properly ordered
   - Best for learning and demos

2. **`debug_autoload.ipynb`** 🔧 **For debugging**
   - Detailed diagnostic notebook
   - Verifies IPython configuration paths
   - Checks startup script installation
   - Troubleshooting auto-load issues

### Python Scripts

3. **`test_mock.py`** 🐍 **Standalone test script**
   - Run without Jupyter: `python examples/test_mock.py`
   - Tests extension loading and basic commands
   - No HDFS cluster required

## Getting Started

### 1. Installation

```bash
# Install the package
pip install -e .

# Run installation script to enable auto-loading
jupyter-webhdfsmagic

# Restart any running Jupyter kernels
```

### 2. Test with Mock HDFS (No Real Cluster)

#### Option A: Jupyter Notebook
```bash
jupyter notebook examples/examples.ipynb
```

Then:
1. Restart the kernel (Kernel → Restart Kernel)
2. Run all cells
3. The `%hdfs` commands will work with mocked data

#### Option B: Python Script
```bash
python examples/test_mock.py
```

### 3. Use with Real HDFS

Configure your connection in `~/.webhdfsmagic/config.json`:

```json
{
  "knox_url": "https://your-knox-gateway:8443/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",  # pragma: allowlist secret
  "verify_ssl": true
}
```

Or use `%hdfs setconfig` in a notebook (see examples).

## Mock Testing Details

The notebooks use Python's `unittest.mock` to simulate HDFS responses without needing a real cluster.

### Two Mock Functions

```python
# For ls, mkdir, rm, chmod, etc. (uses requests.request)
def mock_request(method, url, **kwargs):
    params = kwargs.get('params', {})
    operation = params.get('op', '')
    # ... return mocked response based on operation

# For cat, get (uses requests.get for streaming)
def mock_get(url, **kwargs):
    if "op=OPEN" in url:
        # ... return mocked file content
```

### Why Two Functions?

- **`ls`, `mkdir`, `rm`, etc.** → Use `requests.request()` with operation in `params`
- **`cat`, `get`** → Use `requests.get()` for efficient streaming

Both must be patched correctly:
```python
# For ls
with patch('webhdfsmagic.magics.requests.request', side_effect=mock_request):
    %hdfs ls /path

# For cat
with patch('webhdfsmagic.magics.requests.get', side_effect=mock_get):
    %hdfs cat /path/file.txt
```

## Troubleshooting

### Auto-loading Not Working?

1. Check startup script exists:
   ```bash
   ls ~/.ipython/profile_default/startup/00-webhdfsmagic.py
   ```

2. Verify content:
   ```bash
   cat ~/.ipython/profile_default/startup/00-webhdfsmagic.py
   ```

3. **Restart Jupyter kernel** after installation

4. Run `debug_autoload.ipynb` for detailed diagnostics

### Mock Tests Failing?

- Make sure you use the correct `patch` target:
  - `webhdfsmagic.magics.requests.request` for most commands
  - `webhdfsmagic.magics.requests.get` for cat/get

- Ensure mock functions match the signature:
  - `mock_request(method, url, **kwargs)`
  - `mock_get(url, **kwargs)`

## Further Reading

- [Main README](../README.md) - Installation and basic usage
- [TESTING.md](../TESTING.md) - Complete testing guide
- [WebHDFS REST API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) - Official API documentation

## Contributing

When adding new examples:
1. Keep them simple and focused
2. Add clear documentation
3. Use mocks for tests (no real HDFS required)
4. Test on a fresh Jupyter kernel
