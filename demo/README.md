# WebHDFS Magic - Demo Environment

This directory contains a complete Docker-based HDFS environment for testing and demonstrating **webhdfsmagic**.

## 🐳 Components

- **namenode** - Hadoop NameNode with WebHDFS enabled (port 9870)
- **datanode** - Hadoop DataNode (port 9864)
- **webhdfs-gateway** - Nginx simulating Knox Gateway (port 8080)

## 🚀 Quick Start

### 1. Start the Environment

```bash
cd demo
docker-compose up -d
```

Wait ~30 seconds for HDFS to initialize.

### 2. Verify Containers

```bash
docker ps
```

You should see: `namenode`, `datanode`, `webhdfs-gateway`

### 3. Configure webhdfsmagic

```bash
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
```

### 4. Test the Setup

**With curl:**
```bash
# List HDFS root
curl "http://localhost:8080/gateway/default/webhdfs/v1/?op=LISTSTATUS&user.name=testuser"

# Create a directory
curl -X PUT "http://localhost:8080/gateway/default/webhdfs/v1/test?op=MKDIRS&user.name=testuser"
```

**With webhdfsmagic:**
```bash
cd ..
jupyter notebook examples/demo.ipynb
```

## 🔧 Management

### View Logs

```bash
docker-compose logs -f
docker-compose logs namenode
docker-compose logs webhdfs-gateway
```

### Stop Environment

```bash
docker-compose down
```

### Clean Everything (⚠️ deletes data)

```bash
docker-compose down -v
```

### Restart Services

```bash
docker-compose restart
```

## 🌐 Access URLs

- **NameNode UI:** http://localhost:9870
- **DataNode UI:** http://localhost:9864
- **WebHDFS Gateway:** http://localhost:8080/gateway/default/webhdfs/v1

## 📁 Files

- `docker-compose.yml` - Docker Compose configuration
- `Dockerfile.knox` - Custom Knox Gateway Docker image
- `nginx.conf` - Nginx configuration (Knox simulator)
- `hadoop.env` - Hadoop environment variables
- `knox-init.sh` - Knox initialization script
- `knox-config/` - Knox Gateway configuration files
- `test_hdfs_connection.py` - Python script to test HDFS connection

## 🔍 Troubleshooting

### Containers won't start

```bash
# Check logs
docker-compose logs

# Remove old volumes
docker-compose down -v
docker-compose up -d
```

### Connection refused errors

Wait 30-60 seconds after `docker-compose up -d` for HDFS to fully initialize.

### Permission denied errors

Make sure `user.name=testuser` is included in WebHDFS requests.

## 🎯 Smart Cat Demo

The smart cat feature allows automatic formatting of CSV and Parquet files. To test it:

### Setup Test Data

```bash
# Create test files
cat > /tmp/sales.csv << 'EOF'
Product,Region,Sales,Quantity
Laptop,North,15000,25
Phone,South,8000,40
Tablet,East,12000,30
Monitor,West,5000,15
Keyboard,North,2000,50
EOF

# Upload to HDFS
docker cp /tmp/sales.csv namenode:/tmp/sales.csv
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put -f /tmp/sales.csv /data/sales.csv
```

### Test Smart Cat

Open `../examples/smart_cat_demo.ipynb` in Jupyter and run the cells to see:
- Automatic CSV/Parquet detection
- Beautiful table formatting
- Delimiter auto-detection (comma, tab, semicolon)
- Format options (`--raw`, `--format pandas`)

See [SMART_CAT_TEST_RESULTS.md](../examples/SMART_CAT_TEST_RESULTS.md) for complete test results.

### Cleanup

```bash
./cleanup.sh
```

## 📝 Notes

This is a **development/demo environment only**. Do not use in production.

The setup simulates Knox Gateway using nginx for simplicity. In production, use actual Apache Knox.
