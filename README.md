![Version](https://img.shields.io/badge/version-0.0.1-blue.svg)

# webhdfsmagic

**webhdfsmagic** is a Python package that provides IPython magic commands to interact with HDFS via WebHDFS/Knox.  
It supports common HDFS operations such as listing, uploading, downloading, and managing file permissions and ownership—all directly from your Jupyter notebooks.

## Features

- **List Files:**  
  `%hdfs ls [path]` lists files in the specified HDFS directory.

- **Create Directory:**  
  `%hdfs mkdir <path>` creates a new directory on HDFS.

- **Delete Files/Directories:**  
  `%hdfs rm <path or pattern> [-r]` deletes files or directories on HDFS. Wildcards are supported (e.g. `%hdfs rm /user/files*`).

- **Upload Files:**  
  `%hdfs put <local_file_or_pattern> <hdfs_destination>` uploads one or more local files to HDFS.  
  For large files, the upload is done using streaming to avoid high memory consumption.

- **Download Files:**  
  `%hdfs get <hdfs_file_or_pattern> <local_destination>` downloads files from HDFS to your local machine.  
  Streaming is used for downloads to properly handle large files.

- **Display File Content:**  
  `%hdfs cat <file> [-n <number_of_lines>]` displays the content of a HDFS file.  
  By default, the first 100 lines are shown. Use `-n -1` to display the full file.

- **Modify Permissions/Ownership:**  
  `%hdfs chmod [-R] <permission> <path>` and `%hdfs chown [-R] <user:group> <path>` allow you to change file permissions and owner/group recursively.

- **Dynamic Configuration:** Use `%hdfs setconfig { ... }` to update configuration directly in the notebook.

## Installation

Install the package using pip:

```bash
pip install webhdfsmagic
```

Or for development:

```bash
git clone https://github.com/ab2dridi/webhdfsmagic.git
cd webhdfsmagic
pip install -e .
```

### Automatic Loading

After installation, run the configuration script to enable automatic loading:

```bash
jupyter-webhdfsmagic
```

This will configure IPython/Jupyter to automatically load webhdfsmagic when you start a notebook or IPython session.

**No need to use `%load_ext webhdfsmagic` anymore!** The extension loads automatically.

## Usage

Simply open a Jupyter notebook and start using the commands:

```python
# The extension is already loaded automatically!
%hdfs help
```

Or if you prefer manual loading:

```python
%load_ext webhdfsmagic
```

Then, you can use the available commands. For example:

```bash
# List files on HDFS root
%hdfs ls /

# Upload multiple CSV files from the local directory to HDFS
%hdfs put ~/data/*.csv /user/hdfs/data/

# Download a file from HDFS to the current directory
%hdfs get /user/hdfs/data/sample.csv .

# Display the first 50 lines of a HDFS file
%hdfs cat /user/hdfs/data/sample.csv -n 50
```

## Developer Guide

### Setting Up Development Environment

#### 1. Clone the Repository

```bash
git clone https://github.com/ab2dridi/webhdfsmagic.git
cd webhdfsmagic
```

#### 2. Create Virtual Environment

Using **conda** (recommended):

```bash
conda create -n webhdfsmagic_env python=3.9
conda activate webhdfsmagic_env
```

Or using **venv**:

```bash
python3.9 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

#### 3. Install Development Dependencies

Install the package in editable mode with development dependencies:

```bash
pip install -e ".[dev]"
```

This installs:
- Core dependencies (requests, pandas, traitlets, ipython, jupyter_core)
- Development tools (pytest, ruff)

#### 4. Install Pre-commit Hooks (Optional)

The project uses pre-commit hooks for code quality:

```bash
pip install pre-commit
pre-commit install
```

This will automatically run checks (ruff, mypy, etc.) before each commit.

### Running Tests

The package uses **pytest** for unit testing:

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_magics.py -v

# Run specific test
pytest tests/test_magics.py::test_ls -v
```

**Note:** Tests use mocks and don't require a real HDFS cluster. See [TESTING.md](TESTING.md) for advanced testing scenarios.

#### Code Coverage

To measure test coverage:

```bash
# Install coverage tool (if not already installed)
pip install pytest-cov

# Run tests with coverage report
pytest tests/ --cov=webhdfsmagic --cov-report=term-missing

# Generate HTML coverage report
pytest tests/ --cov=webhdfsmagic --cov-report=html

# Open coverage report in browser
open htmlcov/index.html  # macOS
# or
xdg-open htmlcov/index.html  # Linux
```

**Current coverage:** ~31% (focused on critical functions: configuration, SSL handling, ls, cat)

### Code Quality with Ruff

The project uses **ruff** as the linter and formatter:

```bash
# Check code quality
ruff check .

# Auto-fix issues
ruff check --fix .

# Format code
ruff format .
```

**Ruff configuration** (in `pyproject.toml`):
- Line length: 100 characters
- Target: Python 3.9+
- Enabled rules: pycodestyle, pyflakes, isort, flake8-bugbear, pyupgrade

### Project Structure

```
webhdfsmagic/
├── webhdfsmagic/           # Main package
│   ├── __init__.py         # Package initialization
│   ├── magics.py           # IPython magic commands implementation
│   └── install.py          # Auto-loading configuration script
├── tests/                  # Unit tests
│   └── test_magics.py      # Tests for magic commands
├── examples/               # Example notebooks
│   ├── examples.ipynb      # Comprehensive examples with all commands
│   ├── debug_autoload.ipynb # Debug auto-loading mechanism
│   └── test_mock.py        # Standalone test script
├── pyproject.toml          # Project configuration and dependencies
├── setup.py                # Build configuration
├── README.md               # This file
├── TESTING.md              # Testing guide
└── LICENSE                 # MIT License
```

### Making Changes

1. **Create a new branch** for your feature/fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the code style:
   - Use type hints where appropriate
   - Add docstrings for public functions
   - Keep line length ≤ 100 characters
   - Follow PEP 8 conventions

3. **Run tests and linting**:
   ```bash
   pytest tests/ -v
   ruff check --fix .
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add new feature"
   ```

5. **Push and create a Pull Request**:
   ```bash
   git push origin feature/your-feature-name
   ```

### Building and Distribution

To build the package for distribution:

```bash
# Install build tools
pip install build twine

# Build the package
python -m build

# Check the distribution
twine check dist/*

# Upload to PyPI (maintainers only)
twine upload dist/*
```

### Testing Auto-loading

After making changes to the auto-loading mechanism:

```bash
# Reinstall the startup script
jupyter-webhdfsmagic

# Verify the script was created
cat ~/.ipython/profile_default/startup/00-webhdfsmagic.py

# Test in a new notebook
jupyter notebook
# In a cell, type: %hdfs help
```

## Configuration File
### Overview
The package relies on configuration files to set connection parameters (Knox URL, WebHDFS API endpoint, authentication credentials, and SSL verification).
It supports two configuration files in a prioritized order:

`~/.webhdfsmagic/config.json` (Highest Priority):
If present, this file is used to load the configuration directly.

`~/.sparkmagic/config.json` (Fallback):
If the above file is absent, the package will attempt to load configuration from Sparkmagic's file.
It then extracts the URL found in `"kernel_python_credentials": { "url": ... }` and splits it by removing the last segment.
For example, if the URL is https://hostname:port/gateway/default/livy_for_spark3 or https://hostname:port/gateway/default/my_livy, the package will keep only the base URL:
https://hostname:port/gateway/default and then append /webhdfs/v1.

### SSL Verification (verify_ssl)
The parameter `verify_ssl` controls SSL certificate verification:

- **Boolean (`true`/`false`):** Enable or disable SSL verification
- **String (path):** Path to a custom certificate file (supports `~` expansion)

**Examples:**
```json
// Disable SSL verification (development only)
"verify_ssl": false

// Enable SSL with system CA bundle (recommended for production)
"verify_ssl": true

// Use custom certificate file
"verify_ssl": "/etc/ssl/certs/ca-bundle.crt"

// Use certificate in user home directory (~ supported)
"verify_ssl": "~/certs/knox-ca.pem"
```

For Sparkmagic-based configuration, `verify_ssl` is set to `false` by default.
If you wish to enable SSL verification with a custom certificate, simply set verify_ssl to the path of your certificate file in your configuration file.

**See [examples/config/](examples/config/) for complete configuration examples.**

### Example Configuration Files

**Basic configuration** (`~/.webhdfsmagic/config.json`):

```json
{
  "knox_url": "https://hostname:port/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",
  "verify_ssl": false
}
```

**With SSL verification:**

```json
{
  "knox_url": "https://hostname:port/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",
  "verify_ssl": true
}
```

**With custom certificate:**

```json
{
  "knox_url": "https://hostname:port/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",
  "verify_ssl": "/path/to/your/cacert.pem"
}
```

**More examples:** See [examples/config/](examples/config/) directory for:
- `config_no_ssl.json` - Disable SSL verification
- `config_with_ssl.json` - Enable SSL with system CA
- `config_with_cert.json` - Custom certificate path
- `config_minimal.json` - Local development setup

**Sparkmagic fallback configuration** (`~/.sparkmagic/config.json`):

```json
{
  "kernel_python_credentials": {
    "username": "user",
    "password": "password",
    "url": "https://hostname:port/gateway/default/livy_for_spark3",
    "auth": "Basic_Access"
  }
}
```

In this case, the package will extract the base URL (`https://hostname:port/gateway/default`) from the Sparkmagic configuration, then set the Knox URL for WebHDFS to `https://hostname:port/gateway/default/webhdfs/v1`, and use the `username` and `password` provided. The SSL verification will default to `false` unless overridden.
