#!/usr/bin/env python3
"""
Script to verify that the help command is up-to-date with all features.
"""

import sys

sys.path.insert(0, '/workspaces/webhdfsmagic')

from webhdfsmagic.magics import WebHDFSMagics


class MockShell:
    """Mock IPython shell for testing."""
    pass

def main():
    """Check help documentation completeness."""
    print("🔍 Checking webhdfsmagic help documentation...\n")

    # Create magics instance
    magics = WebHDFSMagics(shell=MockShell())
    help_html = magics._help()
    content = help_html.data

    # List of features that must be documented
    required_features = {
        "--format": "cat --format option",
        "--raw": "cat --raw option",
        "-n": "cat -n option for limiting rows",
        "Smart file preview": "Smart preview feature description",
        "Auto-detects": "Auto-detection capabilities",
        "pandas": "Pandas format support",
        "csv": "CSV format support",
        "parquet": "Parquet format support",
        "-R": "Recursive option for chmod/chown",
        "Examples": "Examples section",
        "wildcards": "Wildcard support for put command",
    }

    print("✅ Required features in documentation:\n")
    missing = []
    for keyword, description in required_features.items():
        if keyword in content:
            print(f"   ✓ {description}")
        else:
            print(f"   ✗ {description} - MISSING!")
            missing.append(description)

    # Summary
    print(f"\n{'='*60}")
    if not missing:
        print("🎉 SUCCESS: All features are properly documented!")
        print(f"📊 Help content: {len(content)} characters")
        return 0
    else:
        print(f"⚠️  WARNING: {len(missing)} features are missing from docs:")
        for item in missing:
            print(f"   - {item}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
