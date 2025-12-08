![Version](https://img.shields.io/badge/version-0.0.2-blue.svg)
![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

# webhdfsmagic

**webhdfsmagic** is a Python package that provides IPython magic commands to interact with HDFS via WebHDFS/Knox Gateway directly from your Jupyter notebooks.

## 🚀 Why webhdfsmagic?

Simplify your HDFS interactions in Jupyter:

**Before** (with PyWebHdfsClient):
```python
from pywebhdfs.webhdfs import PyWebHdfsClient
hdfs = PyWebHdfsClient(host='...', port='...', user_name='...', ...)
data = hdfs.read_file('/data/file.csv')
df = pd.read_csv(BytesIO(data))
```

**Now** (with webhdfsmagic):
```python
%hdfs get /data/file.csv .
df = pd.read_csv('file.csv')
```

**93% less code!** ✨

## 🎬 See it in Action

![webhdfsmagic demo](docs/demo.gif)

*Complete workflow demo: mkdir → put → ls → cat → get → chmod → rm*

## ✨ Features

| Command | Description |
|----------|-------------|
| `%hdfs ls [path]` | List files and directories (returns pandas DataFrame) |
| `%hdfs mkdir <path>` | Create directory (parents created automatically) |
| `%hdfs put <local> <hdfs>` | Upload one or more files (supports wildcards `*.csv`) |
| `%hdfs get <hdfs> <local>` | Download files (supports wildcards and `~` for home directory) |
| `%hdfs cat <file> [-n lines]` | Display file content (default: first 100 lines) |
| `%hdfs rm [-r] <path>` | Delete files/directories (`-r` for recursive, supports wildcards) |
| `%hdfs chmod [-R] <mode> <path>` | Change permissions (`-R` for recursive) |
| `%hdfs chown [-R] <user:group> <path>` | Change owner (`-R` for recursive, requires superuser) |

## 📦 Installation

```bash
pip install webhdfsmagic
```

**Install from source:**
```bash
git clone https://github.com/ab2dridi/webhdfsmagic.git
cd webhdfsmagic
pip install -e .

# Enable autoload (creates startup script)
jupyter-webhdfsmagic
```

## 🔧 Configuration

### Automatic Loading

After installation, **enable autoload** to have webhdfsmagic load automatically in all Jupyter sessions:

```bash
jupyter-webhdfsmagic
```

This creates `~/.ipython/profile_default/startup/00-webhdfsmagic.py` so the extension loads automatically.

**Alternative:** Load manually in each notebook:
```python
%load_ext webhdfsmagic
```

### Configuration File

Create `~/.webhdfsmagic/config.json`:

```json
{
  "knox_url": "https://hostname:port/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",
  "verify_ssl": false
}
```

**SSL Options:**
- `"verify_ssl": false` → Disable SSL verification (development only)
- `"verify_ssl": true` → Use system certificates
- `"verify_ssl": "/path/to/cert.pem"` → Use custom certificate (supports `~`)

**Configuration Examples:**  
See [examples/config/](examples/config/) for complete configurations (with/without SSL, custom certificate, etc.)

**Sparkmagic Fallback:**  
If `~/.webhdfsmagic/config.json` doesn't exist, the package tries `~/.sparkmagic/config.json` and extracts configuration from `kernel_python_credentials.url`.

### Logging & Debugging

All operations are automatically logged to `~/.webhdfsmagic/logs/webhdfsmagic.log` for debugging and auditing purposes.

**Log Features:**
- ✅ Automatic rotation (10MB per file, keeps 5 backups)
- ✅ Detailed HTTP request/response logging
- ✅ Operation tracing with timestamps
- ✅ Error tracking with full stack traces
- ✅ Password masking for security
- ✅ File-level DEBUG logging
- ✅ Console-level WARNING/ERROR logging

**View Recent Logs:**
```bash
# View last 50 lines
tail -50 ~/.webhdfsmagic/logs/webhdfsmagic.log

# Follow logs in real-time
tail -f ~/.webhdfsmagic/logs/webhdfsmagic.log

# Search for errors
grep "ERROR" ~/.webhdfsmagic/logs/webhdfsmagic.log

# View specific operation
grep "hdfs put" ~/.webhdfsmagic/logs/webhdfsmagic.log
```

**Log Format:**
```
2025-12-08 10:30:15 - webhdfsmagic - INFO - [magics.py:145] - >>> Starting operation: hdfs ls
2025-12-08 10:30:15 - webhdfsmagic - DEBUG - [client.py:85] - HTTP Request: GET http://...
2025-12-08 10:30:15 - webhdfsmagic - DEBUG - [client.py:105] - HTTP Response: 200 from http://...
2025-12-08 10:30:15 - webhdfsmagic - INFO - [magics.py:180] - <<< Operation completed: hdfs ls - SUCCESS
```

## 💡 Usage

```python
# The extension is already loaded automatically!
%hdfs help

# List files
%hdfs ls /data

# Create a directory
%hdfs mkdir /user/hdfs/output

# Upload multiple CSV files using wildcards
%hdfs put ~/data/*.csv /user/hdfs/input/

# Download a file to home directory
%hdfs get /user/hdfs/results/output.csv ~/downloads/

# Download multiple files with wildcards
%hdfs get /user/hdfs/results/*.csv ./local_results/

# Display first 50 lines
%hdfs cat /user/hdfs/data/file.csv -n 50

# Delete files with wildcards
%hdfs rm /user/hdfs/temp/*.log

# Delete a directory recursively
%hdfs rm -r /user/hdfs/temp

# Change permissions recursively
%hdfs chmod -R 755 /user/hdfs/data

# Change owner recursively (requires superuser privileges)
%hdfs chown -R hdfs:hadoop /user/hdfs/data
```

**Integration with pandas:**
```python
# Download and read directly
%hdfs get /data/sales.csv .
df = pd.read_csv('sales.csv')
df.head()
```

## 🎯 Advanced Features

### Wildcard Operations

Upload, download, and delete multiple files using shell-style wildcards:

```python
# Upload all CSV files
%hdfs put data/*.csv /hdfs/input/

# Download specific pattern
%hdfs get /hdfs/output/result_*.csv ./downloads/

# Delete log files
%hdfs rm /hdfs/temp/*.log
```

### Recursive Permissions

Apply permission changes to entire directory trees:

```python
# Recursive chmod
%hdfs chmod -R 755 /hdfs/project/

# Recursive chown (requires superuser)
%hdfs chown -R hdfs:hadoop /hdfs/project/
```

### Home Directory Expansion

Use `~` as a shortcut for your home directory:

```python
# Download to home directory
%hdfs get /hdfs/file.csv ~/downloads/

# Works in subdirectories too
%hdfs get /hdfs/data/*.csv ~/projects/analysis/
```

## 📚 Documentation and Examples

- **[examples/demo.ipynb](examples/demo.ipynb)** - Full demo with real HDFS cluster (Docker)
- **[examples/examples.ipynb](examples/examples.ipynb)** - Examples with mocked tests (no cluster needed)
- **[examples/config/](examples/config/)** - Configuration file examples
- **[ROADMAP.md](ROADMAP.md)** - Upcoming features

## 🧪 Testing

**Unit tests** (no HDFS cluster required):
```bash
pytest tests/ -v
```

**Test with Docker HDFS cluster:**
```bash
# Start the demo environment
cd demo
docker-compose up -d

# Wait 30 seconds for initialization
sleep 30

# Configure webhdfsmagic (if not already done)
mkdir -p ~/.webhdfsmagic
cat > ~/.webhdfsmagic/config.json << 'EOF'
{
  "knox_url": "http://localhost:8080/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "testuser",
  "password": "testpass",
  "verify_ssl": false
}
EOF

# Test with demo notebook
cd ..
jupyter notebook examples/demo.ipynb
```

See [demo/README.md](demo/README.md) for complete Docker environment documentation.

## 🐛 Troubleshooting

### Check Logs

All operations are logged to `~/.webhdfsmagic/logs/webhdfsmagic.log`:

```bash
# View recent activity
tail -50 ~/.webhdfsmagic/logs/webhdfsmagic.log

# Check for errors
grep -i "error" ~/.webhdfsmagic/logs/webhdfsmagic.log

# View specific command execution
grep "hdfs put" ~/.webhdfsmagic/logs/webhdfsmagic.log -A 5
```

### Common Issues

**Connection Errors:**
- Check Knox gateway URL in `~/.webhdfsmagic/config.json`
- Verify SSL settings (`verify_ssl: false` for testing)
- Check logs for HTTP error details

**Authentication Errors:**
- Verify username/password in config
- Check if credentials have expired
- Review authentication errors in logs

**File Transfer Issues:**
- Check local file paths exist
- Verify HDFS paths are absolute (start with `/`)
- Review detailed HTTP request/response in logs
- Check disk space on both local and HDFS

**Permission Errors:**
- Verify HDFS user permissions
- Check file/directory ownership in HDFS
- Review operation logs for specific error messages

## 🤝 Contributing

Contributions are welcome! To contribute:

1. Fork the project
2. Create a branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'feat: add new feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

**Testing and code quality:**
```bash
# Run tests
pytest tests/ -v

# Check code style
ruff check .
ruff format .
```

## 📝 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## 🔗 Links

- **PyPI:** https://pypi.org/project/webhdfsmagic/
- **GitHub:** https://github.com/ab2dridi/webhdfsmagic
- **Issues:** https://github.com/ab2dridi/webhdfsmagic/issues

## 📬 Contact

For questions or suggestions, open an issue on GitHub.

