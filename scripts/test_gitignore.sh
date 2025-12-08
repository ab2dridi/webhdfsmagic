#!/bin/bash
# Test script to verify .gitignore patterns

echo "Testing .gitignore patterns..."
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

test_pattern() {
    pattern="$1"
    if git check-ignore -q "$pattern" 2>/dev/null; then
        echo -e "${GREEN}✓${NC} $pattern is ignored"
        return 0
    else
        echo -e "${RED}✗${NC} $pattern is NOT ignored"
        return 1
    fi
}

# Python cache files
echo "Python cache files:"
test_pattern "__pycache__"
test_pattern "tests/__pycache__/test.pyc"
test_pattern "file.pyc"
test_pattern "file.pyo"
echo ""

# IDE files
echo "IDE files:"
test_pattern ".vscode"
test_pattern ".idea"
test_pattern "file.swp"
test_pattern ".vscode/settings.json"
echo ""

# OS files
echo "macOS files:"
test_pattern ".DS_Store"
test_pattern "._file"
test_pattern ".Spotlight-V100"
echo ""

echo "Windows files:"
test_pattern "Thumbs.db"
test_pattern "Desktop.ini"
echo ""

# Jupyter
echo "Jupyter files:"
test_pattern ".ipynb_checkpoints"
test_pattern "examples/.ipynb_checkpoints"
test_pattern ".ipynb_checkpoints/test-checkpoint.ipynb"
echo ""

# Environment files
echo "Environment files:"
test_pattern ".env"
test_pattern ".env.local"
test_pattern "venv"
test_pattern "env"
echo ""

# Ruff and mypy
echo "Linter cache:"
test_pattern ".ruff_cache"
test_pattern ".mypy_cache"
test_pattern ".pytest_cache"
echo ""

# Backup files
echo "Backup files:"
test_pattern "file.bak"
test_pattern "file.backup"
test_pattern "file.old"
test_pattern "file~"
echo ""

# Config files with credentials
echo "Config files (should be ignored except examples):"
test_pattern "config.json"
test_pattern "my_config.json"
test_pattern "config_local.json"
echo ""

echo "Config examples (should NOT be ignored):"
if ! git check-ignore -q "examples/config/config_no_ssl.json" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} examples/config/config_no_ssl.json is tracked"
else
    echo -e "${RED}✗${NC} examples/config/config_no_ssl.json is ignored (WRONG)"
fi

echo ""
echo "All critical patterns tested!"
