# 🚀 WebHDFSMagic Demo Branch

This branch provides a **complete, working demonstration environment** for testing webhdfsmagic with a real HDFS cluster running in Docker.

## 🎯 What's in This Demo?

This demo showcases how **webhdfsmagic** dramatically simplifies HDFS operations in Jupyter notebooks compared to traditional Python clients.

### Traditional Approach (PyWebHdfsClient)

```python
from pywebhdfs.webhdfs import PyWebHdfsClient
import pandas as pd
from io import BytesIO

# Lots of configuration...
knox_host = 'gateway.example.com'
knox_port = '8443'
user = 'hdfs_user'
password = 'hdfs_password'
knox_path = '/gateway/default'

# Initialize client
hdfs = PyWebHdfsClient(
    host=knox_host,
    port=knox_port,
    user_name=user,
    password=password,
    base_path=knox_path + '/webhdfs/v1',
    use_https=True,
    verify=False
)

# Read CSV from HDFS
bytes_data = hdfs.read_file('/user/hdfs/data/sample.csv')
df = pd.read_csv(BytesIO(bytes_data))
print(df.head())
```

### With webhdfsmagic ✨

```python
# 1. Load extension (once)
%load_ext webhdfsmagic

# 2. Get and read file directly
%hdfs get /user/hdfs/data/sample.csv .
import pandas as pd
df = pd.read_csv('sample.csv')
print(df.head())
```

**Result: Simpler, cleaner, more intuitive!**

## 📋 Prerequisites

- Docker & docker-compose installed
- Python 3.8+ with Jupyter
- `webhdfsmagic` package installed

## 🏃 Quick Start

### 1. Install webhdfsmagic

```bash
pip install webhdfsmagic
```

### 2. Start HDFS Environment

```bash
# Clone the repository and checkout demo branch
git clone https://github.com/ab2dridi/webhdfsmagic.git
cd webhdfsmagic
git checkout demo

# Start the Docker environment
docker-compose up -d

# Wait ~30 seconds for services to initialize
sleep 30

# Check that services are running
docker-compose ps
```

You should see:
- ✅ `namenode` - Running on port 9870
- ✅ `datanode` - Running on port 9864  
- ✅ `webhdfs-gateway` - Running on port 8080 (Knox Gateway simulator)

### 3. Create Configuration

```bash
mkdir -p ~/.webhdfsmagic
cat > ~/.webhdfsmagic/config.json << 'EOF'
{
  "knox_url": "http://localhost:8080/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "hdfs",
  "password": "password",
  "verify_ssl": false
}
EOF
```

### 4. Run the Demo Notebook

```bash
# Launch Jupyter
jupyter notebook examples/demo_quick_start.ipynb
```

Or use the complete test notebook:
```bash
jupyter notebook examples/test_local_hdfs.ipynb
```

## 📚 Demo Notebooks

### `demo_quick_start.ipynb`
A clean, professional demonstration showcasing:
- Configuration and connection
- Directory operations (ls, mkdir)
- File upload/download (put, get)
- File reading (cat)
- Batch operations with wildcards
- Complete data workflow example
- Comparison with PyWebHdfsClient

### `test_local_hdfs.ipynb`
Technical testing notebook for validation:
- All HDFS operations
- Error handling
- Edge cases
- Performance testing

## 🌐 Access Points

Once the Docker environment is running:

- **HDFS NameNode UI**: http://localhost:9870
  - Browse HDFS file system
  - View cluster health
  - Monitor DataNodes

- **WebHDFS Gateway**: http://localhost:8080/gateway/default/webhdfs/v1/
  - Knox Gateway simulator
  - REST API endpoint for HDFS operations

## 🧪 What Gets Tested?

The demo validates all currently implemented features:

| Feature | Command | Status |
|---------|---------|--------|
| List files | `%hdfs ls /path` | ✅ Working |
| Create directory | `%hdfs mkdir /path` | ✅ Working |
| Upload file | `%hdfs put local.csv /hdfs/path` | ✅ Working |
| Download file | `%hdfs get /hdfs/path local.csv` | ✅ Working |
| Read file | `%hdfs cat /hdfs/file` | ✅ Working |
| Read with limit | `%hdfs cat -n 10 /hdfs/file` | ✅ Working |
| Delete file | `%hdfs rm /hdfs/file` | ✅ Working |
| Delete directory | `%hdfs rm -r /hdfs/dir` | ✅ Working |
| Wildcard upload | `%hdfs put *.csv /hdfs/dir/` | ✅ Working |
| Wildcard download | `%hdfs get /hdfs/dir/*.csv .` | ✅ Working |

## 🏗️ Architecture

```
┌─────────────────┐
│  Jupyter        │
│  + webhdfsmagic │
└────────┬────────┘
         │
         │ HTTP Requests
         │
┌────────▼────────┐
│  nginx          │  Port 8080
│  (Knox Gateway  │  Simulates Apache Knox
│   Simulator)    │
└────────┬────────┘
         │
         │ Forward to NameNode
         │
┌────────▼────────┐
│  HDFS NameNode  │  Port 9870 (WebHDFS)
│                 │  Port 19000 (RPC)
└────────┬────────┘
         │
         │ Redirect to DataNode for file ops
         │
┌────────▼────────┐
│  HDFS DataNode  │  Port 9864
│                 │
└─────────────────┘
```

### Key Features:

1. **Knox Gateway Simulation**: nginx proxies requests to simulate Knox Gateway behavior
2. **Docker Hostname Handling**: webhdfsmagic automatically handles Docker internal hostnames in redirects
3. **Streaming Support**: Efficient upload/download of large files
4. **User Authentication**: Proper user.name parameter injection for WebHDFS API

## 🛠️ Troubleshooting

### Services won't start
```bash
# Check Docker logs
docker-compose logs

# Restart services
docker-compose restart

# Full restart with clean state
docker-compose down -v
docker-compose up -d
```

### Connection errors
```bash
# Verify services are listening
curl http://localhost:9870/  # NameNode UI
curl http://localhost:8080/  # Gateway

# Check configuration
cat ~/.webhdfsmagic/config.json
```

### Port conflicts
If ports 9870, 9864, or 8080 are already in use, edit `docker-compose.yml`:
```yaml
ports:
  - "19870:9870"  # Change host port (left side)
```

## 🧹 Cleanup

### Stop services (keep data)
```bash
docker-compose stop
```

### Stop and remove containers
```bash
docker-compose down
```

### Remove everything including data volumes
```bash
docker-compose down -v
```

## 📊 Performance Comparison

| Operation | PyWebHdfsClient | webhdfsmagic | Improvement |
|-----------|-----------------|--------------|-------------|
| Lines of code (upload) | ~15 lines | ~1 line | 🔥 93% less |
| Configuration setup | Manual | JSON file | ✅ Reusable |
| Batch operations | Loop required | Wildcards | ✅ Native |
| Notebook integration | Poor | Excellent | ✅ Magic commands |
| Knox Gateway support | Manual setup | Built-in | ✅ Ready |

## 🎓 Learning Resources

- **WebHDFS REST API**: [Apache Hadoop Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
- **Apache Knox**: [Knox Gateway Documentation](https://knox.apache.org/)
- **IPython Magic Commands**: [IPython Documentation](https://ipython.readthedocs.io/en/stable/interactive/magics.html)

## 🤝 Contributing

Found an issue or have an improvement? 

1. Test with this demo environment first
2. Open an issue with reproduction steps
3. Submit a PR with tests

## 📝 License

MIT License - see [LICENSE](../LICENSE) file

## 🙏 Acknowledgments

- **Apache Hadoop** - HDFS implementation
- **Apache Knox** - Gateway security
- **Big Data Europe** - Docker images for Hadoop
- **IPython** - Magic command framework

---

**Ready to try it? Start with:**
```bash
docker-compose up -d && jupyter notebook examples/demo_quick_start.ipynb
```

**Questions?** Open an issue on GitHub!
