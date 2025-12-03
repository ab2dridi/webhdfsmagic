"""Installation script for webhdfsmagic to enable automatic loading in Jupyter."""

import os
import sys
from pathlib import Path


def get_ipython_startup_dir():
    """Get the IPython startup directory."""
    # IPython startup scripts are in ~/.ipython/profile_default/startup/
    ipython_dir = os.path.join(os.path.expanduser("~"), ".ipython")
    startup_dir = os.path.join(ipython_dir, "profile_default", "startup")
    return startup_dir


def install_config():
    """Install IPython startup script to auto-load webhdfsmagic."""
    # Get destination directory
    startup_dir = Path(get_ipython_startup_dir())
    startup_dir.mkdir(parents=True, exist_ok=True)

    # Destination startup script
    dest_script = startup_dir / "00-webhdfsmagic.py"

    # Startup script content
    script_content = """# Auto-load webhdfsmagic extension
try:
    get_ipython().extension_manager.load_extension("webhdfsmagic")
except Exception as e:
    print(f"Warning: Could not auto-load webhdfsmagic: {e}")
"""

    # Check if script already exists
    if dest_script.exists():
        with open(dest_script) as f:
            content = f.read()

        if "webhdfsmagic" in content:
            print("✓ webhdfsmagic already configured in IPython startup")
            return True

    # Create startup script
    with open(dest_script, "w") as f:
        f.write(script_content)
    print(f"✓ Created IPython startup script: {dest_script}")

    return True


def main():
    """Main installation function."""
    print("Installing webhdfsmagic auto-load configuration...")

    if install_config():
        print("\n✓ Installation complete!")
        print("  webhdfsmagic will now load automatically in Jupyter notebooks")
        print("  and IPython sessions.")
        print("\nTo test, run:")
        print("  jupyter notebook")
        print("  # Then in a cell: %hdfs help")
        return 0
    else:
        print("\n✗ Installation failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
