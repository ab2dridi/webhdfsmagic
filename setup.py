"""Setup script for webhdfsmagic package."""

from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install


class PostDevelopCommand(develop):
    """Post-installation for development mode."""
    def run(self):
        develop.run(self)
        # Run autoload installation
        try:
            from webhdfsmagic.install import install_autoload
            if install_autoload():
                print("\n✓ webhdfsmagic autoload configured successfully!")
                print("  The extension will load automatically in Jupyter notebooks.\n")
            else:
                print("\n⚠ Could not configure autoload automatically.")
                print("  Run 'jupyter-webhdfsmagic' manually to configure.\n")
        except Exception as e:
            print(f"\n⚠ Autoload configuration skipped: {e}")
            print("  Run 'jupyter-webhdfsmagic' manually to configure.\n")


class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        # Run autoload installation
        try:
            from webhdfsmagic.install import install_autoload
            if install_autoload():
                print("\n✓ webhdfsmagic autoload configured successfully!")
                print("  The extension will load automatically in Jupyter notebooks.\n")
            else:
                print("\n⚠ Could not configure autoload automatically.")
                print("  Run 'jupyter-webhdfsmagic' manually to configure.\n")
        except Exception as e:
            print(f"\n⚠ Autoload configuration skipped: {e}")
            print("  Run 'jupyter-webhdfsmagic' manually to configure.\n")


# Use pyproject.toml for configuration
setup(
    cmdclass={
        'develop': PostDevelopCommand,
        'install': PostInstallCommand,
    },
)
