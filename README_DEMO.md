# 🚀 Demo Branch - Quick Start

This is the **demo** branch of webhdfsmagic, providing a complete working demonstration environment.

## 📖 Documentation

- **[DEMO_README.md](DEMO_README.md)** - Complete setup and usage guide
- **[DEMO_BRANCH_SUMMARY.md](DEMO_BRANCH_SUMMARY.md)** - Technical details and changes

## 🎯 Quick Start

```bash
# 1. Start Docker environment
docker-compose up -d

# 2. Create configuration
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

# 3. Run demo notebook
jupyter notebook examples/demo_quick_start.ipynb
```

## ✨ What's Included

- ✅ Docker HDFS cluster (NameNode + DataNode)
- ✅ Knox Gateway simulator (nginx)
- ✅ Complete demo notebooks in English
- ✅ PyWebHdfsClient comparison
- ✅ All unit tests passing
- ✅ Production-ready examples

## 🎓 Demo Notebooks

1. **`examples/demo_quick_start.ipynb`** - Professional showcase (start here!)
2. **`examples/test_local_hdfs.ipynb`** - Technical testing

## 🔗 Useful Links

- **Main README**: [README.md](README.md)
- **Package on PyPI**: https://pypi.org/project/webhdfsmagic/
- **GitHub Repository**: https://github.com/ab2dridi/webhdfsmagic

## ❓ Need Help?

See [DEMO_README.md](DEMO_README.md) for:
- Detailed setup instructions
- Troubleshooting guide
- Architecture explanation
- Performance comparison

---

**Ready?** Start with: `docker-compose up -d && jupyter notebook examples/demo_quick_start.ipynb`
