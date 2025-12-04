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

## ✨ Features

| Command | Description |
|----------|-------------|
| `%hdfs ls [path]` | List files and directories (returns pandas DataFrame) |
| `%hdfs mkdir <path>` | Create directory (parents created automatically) |
| `%hdfs put <local> <hdfs>` | Upload one or more files (supports wildcards `*.csv`) |
| `%hdfs get <hdfs> <local>` | Download files (streaming for large files) |
| `%hdfs cat <file> [-n lines]` | Display file content (default: first 100 lines) |
| `%hdfs rm [-r] <path>` | Delete files/directories (`-r` for recursive) |
| `%hdfs chmod <mode> <path>` | Change permissions (e.g., `chmod 755 /data`) |
| `%hdfs chown <user:group> <path>` | Change owner (requires superuser privileges) |

## 📦 Installation

```bash
pip install webhdfsmagic
```

**Install from source:**
```bash
git clone https://github.com/ab2dridi/webhdfsmagic.git
cd webhdfsmagic
pip install -e .
```

## 🔧 Configuration

### Automatic Loading

After installation, configure automatic loading:

```bash
jupyter-webhdfsmagic
```

The extension will load automatically every time you start Jupyter. No need for `%load_ext webhdfsmagic`!

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

## 💡 Usage

```python
# The extension is already loaded automatically!
%hdfs help

# List files
%hdfs ls /data

# Create a directory
%hdfs mkdir /user/hdfs/output

# Upload multiple CSV files
%hdfs put ~/data/*.csv /user/hdfs/input/

# Download a file
%hdfs get /user/hdfs/results/output.csv .

# Display first 50 lines
%hdfs cat /user/hdfs/data/file.csv -n 50

# Delete a directory recursively
%hdfs rm -r /user/hdfs/temp

# Change permissions
%hdfs chmod 755 /user/hdfs/data

# Change owner (requires superuser privileges)
%hdfs chown hdfs:hadoop /user/hdfs/data
```

**Integration with pandas:**
```python
# Download and read directly
%hdfs get /data/sales.csv .
df = pd.read_csv('sales.csv')
df.head()
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

